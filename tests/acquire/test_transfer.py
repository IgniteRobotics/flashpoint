import hashlib
import io
import logging
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from flashpoint.acquire.pulls import PullKey, PullLedger, PullStatus
from flashpoint.acquire.robot import RobotClient
from flashpoint.acquire.transfer import (
    CHUNK_BYTES,
    PART_SUFFIX,
    ByteSource,
    CopyJob,
    TransferResult,
    TransferStatus,
    inbox_path,
    pull_robot,
    select_robot_files,
    verified_copy,
)
from tests.acquire.fake_robot import FakeRobot

LOGGER = "flashpoint.acquire.transfer"
ROOT = "/home/lvuser/logs"
S = 10**9
MB = 1024 * 1024


def _put(robot: FakeRobot, path: str, data: bytes, mtime_s: int) -> Path:
    target = robot.root / path.lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    os.utime(target, ns=(mtime_s * S, mtime_s * S))
    return target


def _payload(size: int, seed: int = 1) -> bytes:
    return (hashlib.sha256(str(seed).encode()).digest() * (size // 32 + 1))[:size]


def _no_sleep(_: float) -> None:
    pass


def _cycle(
    connect: Callable[[], RobotClient],
    ledger: PullLedger,
    inbox: Path,
    *,
    include_active: bool = False,
    stop: threading.Event | None = None,
) -> list[TransferResult]:
    client = connect()
    selection = select_robot_files(
        client, ledger, include_active=include_active, settle_s=5, sleep=_no_sleep
    )
    return pull_robot(client, ledger, inbox, selection, stop=stop or threading.Event())


def _inbox_files(inbox: Path) -> list[Path]:
    return sorted(p for p in inbox.rglob("*") if p.is_file())


def _two_logs(robot: FakeRobot) -> tuple[bytes, bytes]:
    """FRC_1 is finished (3.5 MiB, several chunks); FRC_2 is the current, active log."""
    done, current = _payload(3 * MB + MB // 2), _payload(1000, seed=2)
    _put(robot, f"{ROOT}/FRC_1.wpilog", done, 1000)
    _put(robot, f"{ROOT}/FRC_2.wpilog", current, 2000)
    return done, current


@pytest.fixture
def inbox(tmp_path: Path) -> Path:
    return tmp_path / "lake" / "inbox"


def _source_dir(robot: FakeRobot) -> str:
    return f"{robot.host}_{robot.port}"  # ':' is not allowed in a Windows directory name


# --- inbox layout --------------------------------------------------------------------------


def test_inbox_path_keeps_relpath_under_source(tmp_path: Path) -> None:
    assert inbox_path(tmp_path, "10.68.29.2", "S1/rio.hoot") == tmp_path / "10.68.29.2" / "S1" / (
        "rio.hoot"
    )
    assert inbox_path(tmp_path, "127.0.0.1:2222", "a.wpilog") == tmp_path / "127.0.0.1_2222" / (
        "a.wpilog"
    )


@pytest.mark.parametrize(
    "relpath",
    [
        "../escape.wpilog",
        "a/../../b.wpilog",
        "/abs.wpilog",
        "",
        "..\\..\\x.wpilog",  # a separator on Windows
        "a\\b.wpilog",
        "log.wpilog:stream",  # an NTFS alternate data stream
        "C:x.wpilog",
    ],
)
def test_inbox_path_rejects_escaping_relpaths(tmp_path: Path, relpath: str) -> None:
    with pytest.raises(ValueError):
        inbox_path(tmp_path, "host", relpath)


# --- robot pulls ---------------------------------------------------------------------------


def test_clean_pull_is_verified(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    done, _ = _two_logs(fake_robot)
    results = _cycle(connect_robot, pull_ledger, inbox)
    final = inbox / _source_dir(fake_robot) / "FRC_1.wpilog"
    assert [(r.source_path, r.dest, r.bytes_copied, r.status) for r in results] == [
        (f"{ROOT}/FRC_1.wpilog", final, len(done), TransferStatus.VERIFIED)
    ]
    assert hashlib.sha256(final.read_bytes()).hexdigest() == hashlib.sha256(done).hexdigest()
    assert _inbox_files(inbox) == [final]  # no .part left, active FRC_2 not pulled
    row = pull_ledger.query("SELECT * FROM pulls")[0]
    assert (row["status"], row["sha256"], row["include_active"]) == (
        PullStatus.VERIFIED,
        hashlib.sha256(done).hexdigest(),
        0,
    )
    assert row["source_id"] == f"{fake_robot.host}:{fake_robot.port}"


def test_drop_mid_transfer_leaves_nothing_and_retries_next_cycle(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    _put(fake_robot, f"{ROOT}/FRC_1.wpilog", _payload(3 * MB), 1000)
    _put(fake_robot, f"{ROOT}/FRC_2.wpilog", _payload(3 * MB, seed=3), 1500)
    _put(fake_robot, f"{ROOT}/FRC_3.wpilog", b"current", 2000)
    fake_robot.drop_after_bytes = MB + MB // 2
    results = _cycle(connect_robot, pull_ledger, inbox)
    assert [(r.status, r.source_lost) for r in results] == [(TransferStatus.RETRY, True)]
    assert _inbox_files(inbox) == []  # neither a final file nor a .part
    rows = pull_ledger.query("SELECT remote_path, status, attempts FROM pulls")
    assert rows == [{"remote_path": f"{ROOT}/FRC_1.wpilog", "status": "pending", "attempts": 1}]

    fake_robot.drop_after_bytes = None
    results = _cycle(connect_robot, pull_ledger, inbox)
    assert [r.status for r in results] == [TransferStatus.VERIFIED, TransferStatus.VERIFIED]
    assert [p.name for p in _inbox_files(inbox)] == ["FRC_1.wpilog", "FRC_2.wpilog"]


def test_hash_mismatch_retries_then_fails_for_good(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    _two_logs(fake_robot)
    fake_robot.wrong_hash = True
    statuses = [_cycle(connect_robot, pull_ledger, inbox)[0].status for _ in range(3)]
    assert statuses == [TransferStatus.RETRY, TransferStatus.RETRY, TransferStatus.FAILED]
    assert _inbox_files(inbox) == []
    (failed,) = pull_ledger.failed()
    assert (failed["reason"], failed["attempts"]) == ("hash-mismatch", 3)

    fake_robot.wrong_hash = False
    fake_robot.reset_counters()
    assert _cycle(connect_robot, pull_ledger, inbox) == []  # no automatic retry
    assert fake_robot.bytes_served == 0


def test_no_sha256sum_is_size_verified_with_one_warning_per_cycle(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _put(fake_robot, f"{ROOT}/FRC_1.wpilog", b"one", 1000)
    _put(fake_robot, f"{ROOT}/FRC_2.wpilog", b"two", 1500)
    _put(fake_robot, f"{ROOT}/FRC_3.wpilog", b"current", 2000)
    fake_robot.no_sha256sum = True
    caplog.set_level(logging.WARNING, logger=LOGGER)
    results = _cycle(connect_robot, pull_ledger, inbox)
    assert [r.status for r in results] == [TransferStatus.SIZE_VERIFIED] * 2
    warnings = [r for r in caplog.records if r.name == LOGGER and r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert f"{fake_robot.host}:{fake_robot.port}" in warnings[0].getMessage()
    statuses = {r["status"] for r in pull_ledger.query("SELECT status FROM pulls")}
    assert statuses == {PullStatus.SIZE_VERIFIED}


def test_second_cycle_transfers_zero_bytes(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    _two_logs(fake_robot)
    assert _cycle(connect_robot, pull_ledger, inbox)
    fake_robot.reset_counters()
    assert _cycle(connect_robot, pull_ledger, inbox) == []
    assert fake_robot.bytes_served == 0


def _snapshot(robot: FakeRobot) -> dict[str, tuple[int, int, str]]:
    return {
        p.relative_to(robot.root).as_posix(): (
            p.stat().st_size,
            p.stat().st_mtime_ns,
            hashlib.sha256(p.read_bytes()).hexdigest(),
        )
        for p in robot.root.rglob("*")
        if p.is_file()
    }


def test_robot_is_never_modified(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    _two_logs(fake_robot)
    _put(fake_robot, f"{ROOT}/S1/rio.hoot", b"hoot", 1000)
    _put(fake_robot, f"{ROOT}/S2/rio.hoot", b"hoot2", 2000)
    before = _snapshot(fake_robot)
    fake_robot.wrong_hash = True  # failure paths must not touch the robot either
    assert _cycle(connect_robot, pull_ledger, inbox)
    fake_robot.wrong_hash = False
    assert _cycle(connect_robot, pull_ledger, inbox, include_active=True)
    assert _snapshot(fake_robot) == before
    assert fake_robot.write_attempts == []


def test_include_active_pull_is_flagged(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    _two_logs(fake_robot)
    results = _cycle(connect_robot, pull_ledger, inbox, include_active=True)
    assert [r.status for r in results] == [TransferStatus.VERIFIED] * 2
    flags = {
        r["remote_path"].rsplit("/", 1)[1]: r["include_active"]
        for r in pull_ledger.query("SELECT remote_path, include_active FROM pulls")
    }
    assert flags == {"FRC_1.wpilog": 0, "FRC_2.wpilog": 1}


def test_active_file_that_grows_during_pull_is_size_verified_at_listed_size(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    _put(fake_robot, f"{ROOT}/FRC_1.wpilog", b"0123456789", 1000)
    client = connect_robot()
    selection = select_robot_files(
        client, pull_ledger, include_active=True, settle_s=5, sleep=_no_sleep
    )
    _put(fake_robot, f"{ROOT}/FRC_1.wpilog", b"0123456789-more", 1001)  # robot still writing
    (result,) = pull_robot(client, pull_ledger, inbox, selection, stop=threading.Event())
    assert (result.status, result.bytes_copied) == (TransferStatus.SIZE_VERIFIED, 10)
    assert result.dest.read_bytes() == b"0123456789"


def test_existing_inbox_file_is_never_overwritten(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    _two_logs(fake_robot)
    occupied = inbox / _source_dir(fake_robot) / "FRC_1.wpilog"
    occupied.parent.mkdir(parents=True)
    occupied.write_bytes(b"not yet ingested")
    fake_robot.reset_counters()
    (result,) = _cycle(connect_robot, pull_ledger, inbox)
    assert (result.status, result.reason) == (TransferStatus.SKIPPED, "inbox-occupied")
    assert occupied.read_bytes() == b"not yet ingested"
    assert fake_robot.bytes_served == 0
    assert pull_ledger.query("SELECT * FROM pulls") == []  # not an attempt


def test_stop_flag_set_before_the_pull_copies_nothing(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    _two_logs(fake_robot)
    stop = threading.Event()
    stop.set()
    (result,) = _cycle(connect_robot, pull_ledger, inbox, stop=stop)
    assert result.status == TransferStatus.STOPPED
    assert _inbox_files(inbox) == []
    assert pull_ledger.query("SELECT * FROM pulls") == []


class StopOnSecondCheck(threading.Event):
    """Unset for the pre-copy check, set from the first between-chunk check on."""

    def __init__(self) -> None:
        super().__init__()
        self.checks = 0

    def is_set(self) -> bool:
        self.checks += 1
        return self.checks >= 2


def test_stop_mid_file_returns_promptly_without_draining_the_file(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    _put(fake_robot, f"{ROOT}/FRC_1.wpilog", _payload(16 * MB), 1000)
    _put(fake_robot, f"{ROOT}/FRC_2.wpilog", b"current", 2000)
    client = connect_robot()
    selection = select_robot_files(
        client, pull_ledger, include_active=False, settle_s=5, sleep=_no_sleep
    )
    fake_robot.reset_counters()
    started = time.monotonic()
    (result,) = pull_robot(client, pull_ledger, inbox, selection, stop=StopOnSecondCheck())
    assert time.monotonic() - started < 2.0
    assert result.status == TransferStatus.STOPPED
    # Nothing beyond the first chunk was in flight, so closing did not stream the rest.
    assert fake_robot.bytes_served <= 2 * MB
    assert _inbox_files(inbox) == []
    assert pull_ledger.query("SELECT * FROM pulls") == []  # a stop is not an attempt


def test_robot_path_with_backslash_or_colon_is_skipped(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
) -> None:
    _put(fake_robot, f"{ROOT}/..\\..\\evil.wpilog", b"evil", 1000)
    _put(fake_robot, f"{ROOT}/FRC_1.wpilog", b"one", 1500)
    _put(fake_robot, f"{ROOT}/FRC_2.wpilog", b"current", 2000)
    results = _cycle(connect_robot, pull_ledger, inbox)
    assert [(r.status, r.reason) for r in results] == [
        (TransferStatus.SKIPPED, "bad-path"),
        (TransferStatus.VERIFIED, None),
    ]
    assert [p.name for p in _inbox_files(inbox)] == ["FRC_1.wpilog"]


class _DeleteOnClose:
    """Wraps a remote reader; the robot rotates the file away right after the copy."""

    def __init__(self, inner: ByteSource, local: Path) -> None:
        self.inner = inner
        self.local = local

    def read(self, size: int, /) -> bytes:
        return self.inner.read(size)

    def close(self) -> None:
        self.inner.close()
        self.local.unlink()


@pytest.mark.parametrize(
    ("doomed_name", "include_active"),
    [("FRC_1.wpilog", False), ("FRC_3.wpilog", True)],  # sha256sum path; active re-stat path
)
def test_file_rotated_away_before_the_hash_does_not_abandon_the_robot(
    fake_robot: FakeRobot,
    connect_robot: Callable[[], RobotClient],
    pull_ledger: PullLedger,
    inbox: Path,
    monkeypatch: pytest.MonkeyPatch,
    doomed_name: str,
    include_active: bool,
) -> None:
    _put(fake_robot, f"{ROOT}/FRC_1.wpilog", b"one", 1000)
    _put(fake_robot, f"{ROOT}/FRC_2.wpilog", b"two", 1500)
    _put(fake_robot, f"{ROOT}/FRC_3.wpilog", b"current", 2000)
    doomed = fake_robot.root / ROOT.lstrip("/") / doomed_name
    client = connect_robot()
    original = client.open_remote

    def open_remote(remote_path: str, size: int) -> ByteSource:
        reader = original(remote_path, size)
        return _DeleteOnClose(reader, doomed) if remote_path.endswith(doomed_name) else reader

    monkeypatch.setattr(client, "open_remote", open_remote)
    selection = select_robot_files(
        client, pull_ledger, include_active=include_active, settle_s=5, sleep=_no_sleep
    )
    results = pull_robot(client, pull_ledger, inbox, selection, stop=threading.Event())
    outcomes = {
        r.source_path.rsplit("/", 1)[1]: (r.status, r.reason, r.source_lost) for r in results
    }
    expected: dict[str, tuple[TransferStatus, str | None, bool]] = {
        name: (TransferStatus.VERIFIED, None, False)
        for name in ("FRC_1.wpilog", "FRC_2.wpilog", "FRC_3.wpilog")[: 3 if include_active else 2]
    }
    expected[doomed_name] = (TransferStatus.RETRY, "vanished", False)
    assert outcomes == expected
    assert doomed_name not in [p.name for p in _inbox_files(inbox)]


# --- the shared copy core (also used by volumes) -------------------------------------------


class StopAfterFirstRead(io.BytesIO):
    def __init__(self, data: bytes, stop: threading.Event) -> None:
        super().__init__(data)
        self.stop = stop
        self.reads: list[int] = []

    def read(self, size: int | None = -1, /) -> bytes:
        self.reads.append(size if size is not None else -1)
        self.stop.set()
        return super().read(size)


def _job(
    dest: Path,
    data: bytes,
    *,
    size: int | None = None,
    expected: str | None = None,
    opener: Callable[[], io.BytesIO] | None = None,
) -> CopyJob:
    key = PullKey("volume", "VOL-UUID", "logs/a.wpilog", size or len(data), 7)
    return CopyJob(
        key=key,
        dest=dest,
        size=size if size is not None else len(data),
        open_source=opener or (lambda: io.BytesIO(data)),
        expected_sha256=lambda: expected,
    )


def test_core_reads_in_1_mib_chunks_and_stops_between_them(
    tmp_path: Path, pull_ledger: PullLedger
) -> None:
    stop = threading.Event()
    data = _payload(3 * MB)
    source = StopAfterFirstRead(data, stop)
    dest = tmp_path / "inbox" / "usb-STICK" / "logs" / "a.wpilog"
    result = verified_copy(_job(dest, data, opener=lambda: source), pull_ledger, stop)
    assert result.status == TransferStatus.STOPPED
    assert source.reads == [CHUNK_BYTES]  # stopped after one chunk
    assert not dest.exists()
    assert not dest.with_name(dest.name + PART_SUFFIX).exists()
    assert pull_ledger.query("SELECT * FROM pulls") == []  # a stop is not a failed attempt


def test_core_verifies_against_source_rehash(tmp_path: Path, pull_ledger: PullLedger) -> None:
    data = _payload(2 * MB + 5)
    dest = tmp_path / "inbox" / "usb-STICK" / "a.wpilog"
    good = hashlib.sha256(data).hexdigest()
    result = verified_copy(_job(dest, data, expected=good), pull_ledger, threading.Event())
    assert (result.status, result.bytes_copied) == (TransferStatus.VERIFIED, len(data))
    assert dest.read_bytes() == data
    assert pull_ledger.query("SELECT sha256 FROM pulls") == [{"sha256": good}]


def test_core_short_source_is_size_mismatch(tmp_path: Path, pull_ledger: PullLedger) -> None:
    dest = tmp_path / "inbox" / "a.wpilog"
    result = verified_copy(_job(dest, b"short", size=10), pull_ledger, threading.Event())
    assert (result.status, result.reason) == (TransferStatus.RETRY, "size-mismatch")
    assert _inbox_files(tmp_path / "inbox") == []


def test_core_source_removed_mid_copy(tmp_path: Path, pull_ledger: PullLedger) -> None:
    class Unplugged(io.BytesIO):
        def read(self, size: int | None = -1, /) -> bytes:
            raise OSError(5, "Input/output error")

    dest = tmp_path / "inbox" / "a.wpilog"
    result = verified_copy(
        _job(dest, b"data", opener=lambda: Unplugged(b"data")), pull_ledger, threading.Event()
    )
    assert (result.status, result.source_lost) == (TransferStatus.RETRY, True)
    assert result.reason is not None and result.reason.startswith("read-error")
    assert _inbox_files(tmp_path / "inbox") == []


def test_core_vanished_source_does_not_stop_the_source(
    tmp_path: Path, pull_ledger: PullLedger
) -> None:
    def gone() -> io.BytesIO:
        raise FileNotFoundError(2, "No such file")

    result = verified_copy(
        _job(tmp_path / "inbox" / "a.wpilog", b"x", opener=gone), pull_ledger, threading.Event()
    )
    assert (result.status, result.reason, result.source_lost) == (
        TransferStatus.RETRY,
        "vanished",
        False,
    )


def test_core_never_records_failure_on_a_pulled_key(
    tmp_path: Path, pull_ledger: PullLedger
) -> None:
    job = _job(tmp_path / "inbox" / "a.wpilog", b"data", expected="0" * 64)
    pull_ledger.record_success(job.key, "f" * 64, PullStatus.VERIFIED, include_active=False)
    result = verified_copy(job, pull_ledger, threading.Event())
    assert result.status == TransferStatus.SKIPPED
    assert pull_ledger.get(job.key)["status"] == PullStatus.VERIFIED  # type: ignore[index]


def test_record_failure_cannot_demote_a_verified_pull(pull_ledger: PullLedger) -> None:
    key = PullKey("robot", "10.68.29.2", "/home/lvuser/logs/a.wpilog", 1, 1)
    pull_ledger.record_success(key, "f" * 64, PullStatus.VERIFIED, include_active=False)
    assert pull_ledger.record_failure(key, "hash-mismatch") == PullStatus.VERIFIED
    row = pull_ledger.get(key)
    assert row is not None
    assert (row["status"], row["attempts"], row["reason"]) == (PullStatus.VERIFIED, 0, None)
