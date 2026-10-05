import errno
import hashlib
import io
import logging
import os
import shutil
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, BinaryIO

import pytest

from flashpoint.acquire import removable
from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.pulls import PullKey, PullLedger, PullStatus
from flashpoint.acquire.removable import (
    MAX_SCAN_DEPTH,
    RemovableMedia,
    VolumeFile,
    scan_volume,
    volume_key,
    volume_source,
)
from flashpoint.acquire.transfer import CHUNK_BYTES, PART_SUFFIX, TransferStatus
from flashpoint.acquire.volumes import Volume

LOGGER = "flashpoint.acquire.removable"
S = 10**9
MB = 1024 * 1024
WPILOG = "logs/FRC_20260314_102233_GACMP_Q7.wpilog"
HOOT = "logs/2026-03-14_10-22-33/rio_2026-03-14_10-22-33.hoot"


def _payload(size: int, seed: int = 1) -> bytes:
    return (hashlib.sha256(str(seed).encode()).digest() * (size // 32 + 1))[:size]


def _put(mount: Path, relpath: str, data: bytes, mtime_s: int = 1000) -> Path:
    target = mount / relpath
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    os.utime(target, ns=(mtime_s * S, mtime_s * S))
    return target


def _no_sleep(_: float) -> None:
    pass


def _snapshot(mount: Path) -> list[tuple[str, int, int, str]]:
    """Every entry on the volume: path, size, mtime and content hash (dirs hash as '')."""
    out = []
    for p in sorted(mount.rglob("*")):
        st = p.stat()
        digest = hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else ""
        out.append((p.relative_to(mount).as_posix(), st.st_size, st.st_mtime_ns, digest))
    return out


def _inbox_files(inbox: Path) -> list[Path]:
    return sorted(p for p in inbox.rglob("*") if p.is_file())


@pytest.fixture
def inbox(tmp_path: Path) -> Path:
    return tmp_path / "lake" / "inbox"


@pytest.fixture
def stick(tmp_path: Path) -> Volume:
    """A robot stick with the documented layout, mounted under tmp_path."""
    mount = tmp_path / "Volumes" / "ROBOTLOGS"
    _put(mount, WPILOG, _payload(2 * MB + 77))
    _put(mount, HOOT, _payload(5000, seed=2))
    _put(mount, "README.txt", b"not a log")
    return Volume(id="1A2B3C4D-0000-4000-8000-00000000A001", label="ROBOTLOGS", mount=mount)


class Detector:
    """A stand-in for `volumes.detect` that counts calls and returns what's plugged in."""

    def __init__(self, *plugged: Volume) -> None:
        self.plugged = list(plugged)
        self.calls = 0

    def __call__(self) -> list[Volume]:
        self.calls += 1
        return list(self.plugged)


def _media(
    ledger: PullLedger,
    inbox: Path,
    detect: Callable[[], list[Volume]],
    *,
    enabled: bool = True,
    sleep: Callable[[float], None] = _no_sleep,
) -> RemovableMedia:
    return RemovableMedia(ledger, inbox, enabled=enabled, settle_s=5, sleep=sleep, detect=detect)


# --- scan ----------------------------------------------------------------------------------


def test_scan_finds_robot_stick_layout(stick: Volume) -> None:
    files = scan_volume(stick.mount)
    assert sorted(f.relpath for f in files) == sorted([WPILOG, HOOT])
    by_path = {f.relpath: f for f in files}
    st = (stick.mount / WPILOG).stat()
    assert by_path[WPILOG] == VolumeFile(WPILOG, st.st_size, st.st_mtime_ns)


def test_scan_depth_limit(tmp_path: Path) -> None:
    mount = tmp_path / "vol"
    _put(mount, "top.wpilog", b"0")
    _put(mount, "a/b/c/d/four.wpilog", b"4")
    _put(mount, "a/b/c/d/e/five.wpilog", b"5")
    _put(mount, "a/b/c/d/e/f/six.wpilog", b"6")
    assert MAX_SCAN_DEPTH == 4
    assert sorted(f.relpath for f in scan_volume(mount)) == [
        "a/b/c/d/four.wpilog",
        "top.wpilog",
    ]


@pytest.mark.parametrize(
    "skipped_dir",
    [
        ".Spotlight-V100",
        "System Volume Information",
        "$RECYCLE.BIN",
        ".Trashes",
        ".fseventsd",
        ".hidden",
    ],
)
def test_scan_skips_hidden_and_os_metadata_directories(tmp_path: Path, skipped_dir: str) -> None:
    mount = tmp_path / "vol"
    _put(mount, "logs/real.wpilog", b"x")
    _put(mount, f"{skipped_dir}/store.wpilog", b"x")
    _put(mount, f"{skipped_dir}/deeper/log.hoot", b"x")
    assert [f.relpath for f in scan_volume(mount)] == ["logs/real.wpilog"]


def test_scan_skips_appledouble_files_and_matches_suffix_case_insensitively(
    tmp_path: Path,
) -> None:
    mount = tmp_path / "vol"
    _put(mount, "logs/._FRC_1.wpilog", b"resource fork")
    _put(mount, "logs/FRC_1.WPILOG", b"x")
    _put(mount, "logs/notes.wpilog.txt", b"x")
    assert [f.relpath for f in scan_volume(mount)] == ["logs/FRC_1.WPILOG"]


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="no symlinks")
def test_scan_does_not_follow_symlinks_off_the_volume(tmp_path: Path) -> None:
    mount = tmp_path / "vol"
    outside = tmp_path / "elsewhere"
    _put(outside, "secret.wpilog", b"x")
    _put(mount, "logs/real.wpilog", b"x")
    try:
        (mount / "link").symlink_to(outside, target_is_directory=True)
        (mount / "logs" / "linked.wpilog").symlink_to(outside / "secret.wpilog")
    except OSError:
        pytest.skip("symlinks unavailable")
    assert [f.relpath for f in scan_volume(mount)] == ["logs/real.wpilog"]


# --- naming --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("ROBOTLOGS", "usb-ROBOTLOGS"),
        ("robot_logs-2", "usb-robot_logs-2"),
        ("My Stick!", "usb-My_Stick_"),
        ("../evil", "usb-___evil"),
        ("C:\\x", "usb-C__x"),
        ("", "usb-1A2B3C4D"),
    ],
)
def test_volume_source_sanitises_label(label: str, expected: str) -> None:
    vol = Volume(id="1A2B3C4D-0000-4000-8000-00000000A001", label=label, mount=Path("/v"))
    assert volume_source(vol) == expected


def test_volume_key_uses_volume_kind_and_id() -> None:
    vol = Volume(id="UUID-1", label="ROBOTLOGS", mount=Path("/Volumes/ROBOTLOGS"))
    f = VolumeFile("logs/a.wpilog", 10, 20)
    assert volume_key(vol, f) == PullKey("volume", "UUID-1", "logs/a.wpilog", 10, 20)


# --- verified copy -------------------------------------------------------------------------


def test_verified_copy_into_usb_label_folder(
    stick: Volume, pull_ledger: PullLedger, inbox: Path
) -> None:
    results = _media(pull_ledger, inbox, Detector(stick)).cycle(stop=threading.Event())
    assert {r.status for r in results} == {TransferStatus.VERIFIED}
    dest = inbox / "usb-ROBOTLOGS" / WPILOG
    assert dest.read_bytes() == (stick.mount / WPILOG).read_bytes()
    assert (inbox / "usb-ROBOTLOGS" / HOOT).is_file()
    assert not list(inbox.rglob(f"*{PART_SUFFIX}"))
    st = (stick.mount / WPILOG).stat()
    row = pull_ledger.get(PullKey("volume", stick.id, WPILOG, st.st_size, st.st_mtime_ns))
    assert row is not None
    assert row["status"] == PullStatus.VERIFIED
    assert row["source_kind"] == "volume"
    assert row["sha256"] == hashlib.sha256(dest.read_bytes()).hexdigest()


class _Opens:
    """Replaces the read-only opener; `hook(path, n)` may substitute the n-th open of a path."""

    def __init__(self, hook: Callable[[Path, int], BinaryIO | None]) -> None:
        self.hook = hook
        self.counts: dict[Path, int] = {}
        self.modes: list[str] = []

    def __call__(self, path: Path) -> BinaryIO:
        n = self.counts[path] = self.counts.get(path, 0) + 1
        replaced = self.hook(path, n)
        if replaced is not None:
            return replaced
        f = path.open("rb")
        self.modes.append(f.mode)
        return f


def test_source_is_rehashed_after_the_copy(
    stick: Volume, pull_ledger: PullLedger, inbox: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    opens = _Opens(lambda _p, _n: None)
    monkeypatch.setattr(removable, "_open_readonly", opens)
    _media(pull_ledger, inbox, Detector(stick)).cycle(stop=threading.Event())
    # one open to copy, one to re-hash, per file; all read-only
    assert sorted(opens.counts.values()) == [2, 2]
    assert set(opens.modes) == {"rb"}


def test_rehash_mismatch_retries_then_fails(
    stick: Volume, pull_ledger: PullLedger, inbox: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = stick.mount / WPILOG

    def corrupt_rehash(path: Path, n: int) -> BinaryIO | None:
        if path == target and n % 2 == 0:  # every re-hash of the wpilog sees other bytes
            return io.BytesIO(b"\0" * target.stat().st_size)
        return None

    monkeypatch.setattr(removable, "_open_readonly", _Opens(corrupt_rehash))
    media = _media(pull_ledger, inbox, Detector(stick))
    statuses = []
    for _ in range(3):
        by_path = {r.source_path: r for r in media.cycle(stop=threading.Event())}
        statuses.append((by_path[WPILOG].status, by_path[WPILOG].reason))
    assert statuses == [
        (TransferStatus.RETRY, "hash-mismatch"),
        (TransferStatus.RETRY, "hash-mismatch"),
        (TransferStatus.FAILED, "hash-mismatch"),
    ]
    assert not (inbox / "usb-ROBOTLOGS" / WPILOG).exists()
    assert not list(inbox.rglob(f"*{PART_SUFFIX}"))
    fourth = media.cycle(stop=threading.Event())
    assert sum(r.bytes_copied for r in fourth) == 0


class _YankedStick(io.RawIOBase):
    """Serves one chunk, then the stick is pulled: the mount goes away and reads fail."""

    def __init__(self, path: Path, mount: Path) -> None:
        self._f = path.open("rb")
        self._mount = mount
        self._served = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1, /) -> bytes:
        if self._served:
            shutil.rmtree(self._mount)
            raise OSError(errno.EIO, "Input/output error")
        self._served += 1
        return self._f.read(size)

    def close(self) -> None:
        self._f.close()
        super().close()


def test_stick_removed_mid_copy(
    tmp_path: Path,
    stick: Volume,
    pull_ledger: PullLedger,
    inbox: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    other_mount = tmp_path / "Volumes" / "OTHER"
    _put(other_mount, "FRC_9.wpilog", _payload(1000, seed=9))
    other = Volume(id="OTHER-ID", label="OTHER", mount=other_mount)
    big = stick.mount / WPILOG
    assert big.stat().st_size > CHUNK_BYTES
    _put(stick.mount, "logs/later.wpilog", b"scanned after the big one")

    def yank(path: Path, n: int) -> BinaryIO | None:
        if path == big:
            return _YankedStick(path, stick.mount)  # type: ignore[return-value]
        return None

    monkeypatch.setattr(removable, "_open_readonly", _Opens(yank))
    results = _media(pull_ledger, inbox, Detector(stick, other)).cycle(stop=threading.Event())

    lost = [r for r in results if r.source_path == WPILOG]
    assert len(lost) == 1
    assert lost[0].status == TransferStatus.RETRY
    assert lost[0].reason == "volume-removed"
    assert lost[0].source_lost
    # the rest of the stick is not attempted; the other volume still copies
    assert "logs/later.wpilog" not in {r.source_path for r in results}
    assert [r.status for r in results if r.source_path == "FRC_9.wpilog"] == [
        TransferStatus.VERIFIED
    ]
    assert not (inbox / "usb-ROBOTLOGS" / WPILOG).exists()
    assert not list(inbox.rglob(f"*{PART_SUFFIX}"))
    # not a failed attempt of the file: no pulls row for it (the hoot was copied first)
    rows = pull_ledger.query("SELECT * FROM pulls WHERE source_id = ?", (stick.id,))
    assert [r["remote_path"] for r in rows] == [HOOT]


def test_reinserted_stick_copies_zero_bytes(
    tmp_path: Path, stick: Volume, pull_ledger: PullLedger, inbox: Path
) -> None:
    detector = Detector(stick)
    first = _media(pull_ledger, inbox, detector).cycle(stop=threading.Event())
    assert sum(r.bytes_copied for r in first) > 0
    shutil.rmtree(inbox)  # ingest took the files

    # ejected, then reinserted at another mount point (e.g. "ROBOTLOGS 1"), new process
    remount = tmp_path / "Volumes" / "ROBOTLOGS 1"
    shutil.copytree(stick.mount, remount)  # copy2 keeps the mtimes
    shutil.rmtree(stick.mount)
    again = Volume(id=stick.id, label=stick.label, mount=remount)
    second = _media(pull_ledger, inbox, Detector(again)).cycle(stop=threading.Event())
    assert sum(r.bytes_copied for r in second) == 0
    assert _inbox_files(inbox) == []


def test_volume_is_never_modified(
    stick: Volume, pull_ledger: PullLedger, inbox: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = _snapshot(stick.mount)
    media = _media(pull_ledger, inbox, Detector(stick))
    media.cycle(stop=threading.Event())  # success
    media.cycle(stop=threading.Event())  # nothing to do
    assert _snapshot(stick.mount) == before

    target = stick.mount / HOOT

    def corrupt(path: Path, n: int) -> BinaryIO | None:
        return io.BytesIO(b"\0" * target.stat().st_size) if n % 2 == 0 else None

    # a new file plus a failing re-hash cycle
    _put(stick.mount, "logs/FRC_2.wpilog", b"new", 3000)
    before = _snapshot(stick.mount)
    monkeypatch.setattr(removable, "_open_readonly", _Opens(corrupt))
    for _ in range(3):
        media.cycle(stop=threading.Event())
    assert _snapshot(stick.mount) == before


def test_settle_waits_once_per_volume_and_skips_changing_files(
    tmp_path: Path, stick: Volume, pull_ledger: PullLedger, inbox: Path
) -> None:
    second_mount = tmp_path / "Volumes" / "B"
    _put(second_mount, "b.wpilog", b"b")
    second = Volume(id="B", label="B", mount=second_mount)
    waits: list[float] = []

    def sleep(s: float) -> None:
        waits.append(s)
        if len(waits) == 1:  # the hoot grows during the first volume's settle wait
            _put(stick.mount, HOOT, _payload(6000, seed=2), 1001)

    media = _media(pull_ledger, inbox, Detector(stick, second), sleep=sleep)
    selections = media.select()
    assert waits == [5, 5]
    by_label = {s.volume.label: s for s in selections}
    assert [f.relpath for f in by_label["ROBOTLOGS"].pull] == [WPILOG]
    assert [f.relpath for f in by_label["ROBOTLOGS"].unstable] == [HOOT]

    media.pull(selections, stop=threading.Event())
    waits.clear()
    media.cycle(stop=threading.Event())  # only the (now stable) hoot is left on ROBOTLOGS
    assert waits == [5]


def test_select_is_a_dry_run(stick: Volume, pull_ledger: PullLedger, inbox: Path) -> None:
    selections = _media(pull_ledger, inbox, Detector(stick)).select()
    assert [(s.volume, sorted(f.relpath for f in s.pull)) for s in selections] == [
        (stick, sorted([WPILOG, HOOT]))
    ]
    assert sum(f.size for f in selections[0].pull) == 2 * MB + 77 + 5000
    assert not inbox.exists()
    assert pull_ledger.query("SELECT * FROM pulls") == []


def test_stop_flag_copies_nothing(stick: Volume, pull_ledger: PullLedger, inbox: Path) -> None:
    stop = threading.Event()
    stop.set()
    results = _media(pull_ledger, inbox, Detector(stick)).cycle(stop=stop)
    assert all(r.status == TransferStatus.STOPPED for r in results)
    assert _inbox_files(inbox) == []
    assert pull_ledger.query("SELECT * FROM pulls") == []


def test_volume_gone_before_scan_is_skipped(
    tmp_path: Path,
    stick: Volume,
    pull_ledger: PullLedger,
    inbox: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    ghost = Volume(id="GHOST", label="GHOST", mount=tmp_path / "Volumes" / "GHOST")
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        results = _media(pull_ledger, inbox, Detector(ghost, stick)).cycle(stop=threading.Event())
    assert {r.status for r in results} == {TransferStatus.VERIFIED}
    assert "GHOST" in caplog.text


# --- --no-usb ------------------------------------------------------------------------------


def test_disabled_never_detects_or_scans(
    stick: Volume, pull_ledger: PullLedger, inbox: Path
) -> None:
    detector = Detector(stick)
    media = _media(pull_ledger, inbox, detector, enabled=False)
    assert media.select() == []
    assert media.cycle(stop=threading.Event()) == []
    assert detector.calls == 0
    assert not inbox.exists()


def test_from_config_honours_removable_media(pull_ledger: PullLedger, inbox: Path) -> None:
    detector = Detector()
    config = AcquireConfig().with_overrides(removable_media=False)
    media = RemovableMedia.from_config(config, pull_ledger, inbox, sleep=_no_sleep, detect=detector)
    media.cycle(stop=threading.Event())
    assert detector.calls == 0
    on = RemovableMedia.from_config(
        AcquireConfig(), pull_ledger, inbox, sleep=_no_sleep, detect=detector
    )
    on.cycle(stop=threading.Event())
    assert detector.calls == 1


# --- safe to eject -------------------------------------------------------------------------


def _notices(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if "safe to remove" in r.getMessage()]


def test_safe_to_eject_once_per_insertion(
    stick: Volume, pull_ledger: PullLedger, inbox: Path, caplog: pytest.LogCaptureFixture
) -> None:
    detector = Detector(stick)
    media = _media(pull_ledger, inbox, detector)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        media.cycle(stop=threading.Event())
        assert len(_notices(caplog)) == 1
        assert "ROBOTLOGS" in _notices(caplog)[0]
        media.cycle(stop=threading.Event())
        assert len(_notices(caplog)) == 1  # still plugged in: not repeated

        detector.plugged = []  # ejected
        media.cycle(stop=threading.Event())
        detector.plugged = [stick]  # reinserted: a new insertion, nothing new to copy
        media.cycle(stop=threading.Event())
        assert len(_notices(caplog)) == 2


def test_no_safe_to_eject_while_a_file_is_pending_or_failed(
    stick: Volume,
    pull_ledger: PullLedger,
    inbox: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    target = stick.mount / HOOT

    def corrupt(path: Path, n: int) -> BinaryIO | None:
        if path == target and n % 2 == 0:
            return io.BytesIO(b"\0" * target.stat().st_size)
        return None

    monkeypatch.setattr(removable, "_open_readonly", _Opens(corrupt))
    media = _media(pull_ledger, inbox, Detector(stick))
    with caplog.at_level(logging.INFO, logger=LOGGER):
        for _ in range(4):  # retry, retry, failed, then failed stays failed
            media.cycle(stop=threading.Event())
            assert _notices(caplog) == []
    assert [r["remote_path"] for r in pull_ledger.failed()] == [HOOT]


def test_no_safe_to_eject_while_a_file_is_unstable(
    stick: Volume, pull_ledger: PullLedger, inbox: Path, caplog: pytest.LogCaptureFixture
) -> None:
    def sleep(_: float) -> None:
        _put(stick.mount, HOOT, _payload(7000, seed=3), 2000)

    with caplog.at_level(logging.INFO, logger=LOGGER):
        _media(pull_ledger, inbox, Detector(stick), sleep=sleep).cycle(stop=threading.Event())
    assert _notices(caplog) == []


def _unreadable(directory: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real_scandir = os.scandir

    def scandir(path: Any) -> Any:
        if Path(path) == directory:
            raise PermissionError(13, "Permission denied", str(path))
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", scandir)


def test_unreadable_directory_is_recorded_and_withholds_safe_to_eject(
    stick: Volume,
    pull_ledger: PullLedger,
    inbox: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    hidden = stick.mount / "more-logs"
    _put(stick.mount, "more-logs/FRC_9.wpilog", b"unseen")
    _unreadable(hidden, monkeypatch)
    media = _media(pull_ledger, inbox, Detector(stick))
    with caplog.at_level(logging.INFO, logger=LOGGER):
        (selection,) = media.select()
        assert len(selection.scan_errors) == 1 and "more-logs" in selection.scan_errors[0]
        media.pull([selection], stop=threading.Event())
        media.cycle(stop=threading.Event())
    assert _notices(caplog) == []
    assert any(
        r.levelno == logging.WARNING and "more-logs" in r.getMessage() for r in caplog.records
    )
    assert len(_inbox_files(inbox)) == 2  # the readable logs are still copied

    monkeypatch.undo()  # readable again: everything is accounted for
    with caplog.at_level(logging.INFO, logger=LOGGER):
        media.cycle(stop=threading.Event())
    assert len(_notices(caplog)) == 1
