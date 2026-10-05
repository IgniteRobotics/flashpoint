import functools
import hashlib
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.cycle import (
    CycleResult,
    DeriveOutcome,
    SourceOutcome,
    SourceStatus,
    run_cycle,
)
from flashpoint.acquire.pulls import PullLedger, PullStatus
from flashpoint.acquire.robot import LowSpaceWarning, RemoteFile, SpaceReading
from flashpoint.acquire.transfer import PART_SUFFIX, robot_key
from flashpoint.acquire.watch import (
    EXIT_INTERRUPTED,
    EXIT_LOCKED,
    AcquireLock,
    LockHeldError,
    StatusTracker,
    run_acquire,
)
from flashpoint.cli import main
from flashpoint.lake.paths import LakePaths
from tests.acquire.fake_robot import FakeRobot

ROOT = "/home/lvuser/logs"
S = 10**9
NO_ROBOT = "127.0.0.1:1"  # connection refused at once
HOLDER = """
import sys, time
from pathlib import Path
from flashpoint.acquire.watch import AcquireLock
AcquireLock(Path(sys.argv[1])).acquire()
print("locked", flush=True)
time.sleep(120)
"""


def _put(base: Path, relpath: str, data: bytes, mtime_s: int) -> Path:
    target = base / relpath.lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    os.utime(target, ns=(mtime_s * S, mtime_s * S))
    return target


def _tree(root: Path) -> dict[str, tuple[int, int, str]] | None:
    """Every file under `root` with its size, mtime and hash; None if `root` is missing."""
    if not root.exists():
        return None
    return {
        p.relative_to(root).as_posix(): (
            p.stat().st_size,
            p.stat().st_mtime_ns,
            hashlib.sha256(p.read_bytes()).hexdigest(),
        )
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


@pytest.fixture
def config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / "config"
    directory.mkdir()
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(directory))
    monkeypatch.setenv("FLASHPOINT_CACHE", str(tmp_path / "cache"))
    return directory


@pytest.fixture
def lake(tmp_path: Path) -> LakePaths:
    return LakePaths(tmp_path / "lake")


@pytest.fixture
def holder(lake: LakePaths) -> Iterator["subprocess.Popen[str]"]:
    """Another process holding the lake's acquire lock."""
    lake.meta.mkdir(parents=True)
    proc = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", HOLDER, str(lake.meta / "acquire.lock")],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    assert proc.stdout.readline().strip() == "locked"
    yield proc
    proc.kill()
    proc.wait()
    proc.stdout.close()


# --- single watcher --------------------------------------------------------------------------


def test_second_acquire_exits_non_zero_naming_the_holder(
    config_dir: Path,
    lake: LakePaths,
    holder: "subprocess.Popen[str]",
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(["acquire", "--lake", str(lake.root), "--host", NO_ROBOT, "--no-usb"])

    assert code == EXIT_LOCKED != 0
    err = capsys.readouterr().err
    assert f"pid {holder.pid}" in err and "acquire.lock" in err
    assert holder.poll() is None  # the first is unaffected and still holds the lock
    with pytest.raises(LockHeldError):
        AcquireLock(lake.meta / "acquire.lock").acquire()
    assert not lake.status.exists()  # the second never ran a cycle


def test_lock_of_a_killed_holder_is_taken_over(
    config_dir: Path, lake: LakePaths, holder: "subprocess.Popen[str]"
) -> None:
    with pytest.raises(LockHeldError, match=f"pid {holder.pid}"):
        AcquireLock(lake.meta / "acquire.lock").acquire()
    holder.kill()
    holder.wait()

    code = main(["acquire", "--lake", str(lake.root), "--host", NO_ROBOT, "--no-usb"])

    assert code == 0  # no robot in range is a normal cycle
    status = json.loads(lake.status.read_text())
    assert status["sources"][0]["status"] == "unreachable"
    holder_info = json.loads((lake.meta / "acquire.lock").read_text())
    assert holder_info["pid"] == os.getpid()


def test_lock_is_released_and_reacquired_in_process(lake: LakePaths) -> None:
    path = lake.meta / "acquire.lock"
    with AcquireLock(path), pytest.raises(LockHeldError):
        AcquireLock(path).acquire()
    with AcquireLock(path):
        pass


# --- interrupt -------------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT to a child process is POSIX-only")
def test_interrupt_mid_transfer_exits_within_5s_with_no_partial_final_file(
    config_dir: Path, lake: LakePaths, fake_robot: FakeRobot, tmp_path: Path
) -> None:
    (config_dir / "acquire.toml").write_text("settle_s = 0\n")
    big = "FRC_20260314_101500_GACMP_Q6.wpilog"
    _put(fake_robot.root, f"{ROOT}/{big}", bytes(64 * 1024 * 1024), 1000)
    _put(fake_robot.root, f"{ROOT}/FRC_20260314_102233_GACMP_Q7.wpilog", b"x", 2000)  # active
    fake_robot.read_delay_s = 0.005  # ~6 MB/s: the 64 MiB copy takes ~10 s
    log_path = tmp_path / "watch.log"
    argv = [sys.executable, "-m", "flashpoint", "acquire", "--watch", "--lake", str(lake.root),
            "--host", f"{fake_robot.host}:{fake_robot.port}", "--no-usb"]  # fmt: skip
    with log_path.open("w") as log_file:
        proc = subprocess.Popen(argv, stdout=log_file, stderr=subprocess.STDOUT)  # noqa: S603
        try:
            deadline = time.monotonic() + 30
            while not any(
                p.stat().st_size > 2 * 1024 * 1024 for p in lake.inbox.rglob(f"*{PART_SUFFIX}")
            ):
                assert proc.poll() is None, log_path.read_text()
                assert time.monotonic() < deadline, log_path.read_text()
                time.sleep(0.05)
            proc.send_signal(signal.SIGINT)
            interrupted = time.monotonic()
            code = proc.wait(timeout=15)
            elapsed = time.monotonic() - interrupted
        finally:
            proc.kill()
            proc.wait()

    assert elapsed <= 5.0, log_path.read_text()
    assert code == 0, log_path.read_text()
    assert [p for p in lake.inbox.rglob("*") if p.is_file()] == []  # .part removed, no final file
    status = json.loads(lake.status.read_text())
    assert status["last_cycle"]["stopped"] is True
    assert fake_robot.bytes_served < 64 * 1024 * 1024


# --- dry run ---------------------------------------------------------------------------------


@pytest.mark.parametrize("existing_lake", [False, True])
def test_dry_run_lists_files_with_sizes_and_changes_nothing(
    config_dir: Path,
    lake: LakePaths,
    fake_robot: FakeRobot,
    capsys: pytest.CaptureFixture[str],
    existing_lake: bool,
) -> None:
    (config_dir / "acquire.toml").write_text("settle_s = 0\n")
    host = f"{fake_robot.host}:{fake_robot.port}"
    files = {
        "FRC_20260314_100000_GACMP_Q4.wpilog": (b"p" * 30, 900),  # pulled before (existing lake)
        "FRC_20260314_101000_GACMP_Q5.wpilog": (b"a" * 40, 1000),
        "FRC_20260314_101500_GACMP_Q6.wpilog": (b"b" * 50, 1500),
        "FRC_20260314_102233_GACMP_Q7.wpilog": (b"n" * 60, 2000),  # active
    }
    for name, (data, mtime) in files.items():
        _put(fake_robot.root, f"{ROOT}/{name}", data, mtime)
    if existing_lake:
        pulls = PullLedger(lake.ledger)
        pulled = RemoteFile(ROOT, "FRC_20260314_100000_GACMP_Q4.wpilog", 30, 900 * S)
        pulls.record_success(robot_key(host, pulled), "0" * 64, PullStatus.VERIFIED, False)
        pulls.close()
    before = _tree(lake.root)

    code = main(["acquire", "--dry-run", "--lake", str(lake.root), "--host", host, "--no-usb"])

    assert code == 0
    out = capsys.readouterr().out
    sizes = {"Q5.wpilog": 40, "Q6.wpilog": 50} | ({} if existing_lake else {"Q4.wpilog": 30})
    for name, size in sizes.items():
        (line,) = [line for line in out.splitlines() if name in line]
        assert host in line and f"{ROOT}/FRC_" in line and f"{size} bytes" in line
    assert ("Q4.wpilog" in out) is not existing_lake
    assert "Q7.wpilog" not in out
    assert _tree(lake.root) == before  # nothing created or modified, no lock, no status
    assert fake_robot.bytes_served == 0


def test_dry_run_reads_a_ledger_that_a_watch_is_writing(
    lake: LakePaths, fake_robot: FakeRobot, capsys: pytest.CaptureFixture[str]
) -> None:
    host = f"{fake_robot.host}:{fake_robot.port}"
    _put(fake_robot.root, f"{ROOT}/FRC_1.wpilog", b"p" * 30, 900)  # pulled by the watch
    _put(fake_robot.root, f"{ROOT}/FRC_2.wpilog", b"a" * 40, 1000)
    _put(fake_robot.root, f"{ROOT}/FRC_3.wpilog", b"n" * 50, 2000)  # active
    writer = PullLedger(lake.ledger)  # the watch: its commits sit in the -wal file
    try:
        pulled = RemoteFile(ROOT, "FRC_1.wpilog", 30, 900 * S)
        writer.record_success(robot_key(host, pulled), "0" * 64, PullStatus.VERIFIED, False)
        wal = lake.ledger.with_name(lake.ledger.name + "-wal")
        assert wal.is_file()
        before = {p: p.read_bytes() for p in (lake.ledger, wal)}
        config = AcquireConfig(hosts=[host], roots=[ROOT], settle_s=0, removable_media=False)

        assert run_acquire(config, lake, dry_run=True, stop=threading.Event()) == 0

        out = capsys.readouterr().out
        assert "FRC_2.wpilog" in out and "FRC_1.wpilog" not in out
        assert {p: p.read_bytes() for p in (lake.ledger, wal)} == before
    finally:
        writer.close()


def test_dry_run_exits_1_when_a_source_errored(
    lake: LakePaths, capsys: pytest.CaptureFixture[str]
) -> None:
    def broken_detect() -> list[Any]:
        raise RuntimeError("diskutil exploded")

    config = AcquireConfig(hosts=[NO_ROBOT], removable_media=True)
    code = run_acquire(config, lake, dry_run=True, stop=threading.Event(), detect=broken_detect)
    assert code == 1
    assert "diskutil exploded" in capsys.readouterr().out
    assert not lake.root.exists()


# --- configuration ---------------------------------------------------------------------------


def test_invalid_config_exits_naming_the_key_before_connecting(
    config_dir: Path,
    lake: LakePaths,
    fake_robot: FakeRobot,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (config_dir / "acquire.toml").write_text("pol_s = 3\n")
    host = f"{fake_robot.host}:{fake_robot.port}"

    code = main(["acquire", "--lake", str(lake.root), "--host", host, "--no-usb"])

    assert code != 0
    assert "pol_s" in capsys.readouterr().err
    assert fake_robot.connection_count == 0
    assert not lake.root.exists()


def test_cli_flags_override_the_file_only_when_given(
    config_dir: Path, lake: LakePaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    (config_dir / "acquire.toml").write_text('hosts = ["a", "b"]\nremovable_media = true\n')
    seen: list[tuple[AcquireConfig, dict[str, Any]]] = []

    def fake_run(config: AcquireConfig, _lake: LakePaths, **kwargs: Any) -> int:
        seen.append((config, kwargs))
        return 0

    monkeypatch.setattr("flashpoint.cli.run_acquire", fake_run)
    assert main(["acquire", "--lake", str(lake.root)]) == 0
    assert main(["acquire", "--watch", "--host", "x", "--host", "y", "--no-usb",
                 "--include-active", "--lake", str(lake.root)]) == 0  # fmt: skip

    (plain, plain_kw), (flagged, flagged_kw) = seen
    assert (plain.hosts, plain.removable_media) == (["a", "b"], True)
    assert plain_kw == {"watch": False, "include_active": False, "dry_run": False}
    assert (flagged.hosts, flagged.removable_media) == (["x", "y"], False)
    assert flagged_kw == {"watch": True, "include_active": True, "dry_run": False}


# --- poll loop and status --------------------------------------------------------------------


class FakeCycle:
    """Stands in for `run_cycle`: records each call; derive is owed after the first cycle."""

    def __init__(self) -> None:
        self.calls: list[tuple[float, bool]] = []
        self.results: list[CycleResult] = []

    def __call__(self, *_args: Any, derive_pending: bool = False, **_kwargs: Any) -> CycleResult:
        self.calls.append((time.monotonic(), derive_pending))
        result = CycleResult(
            started=f"2026-10-04T10:00:{len(self.calls):02d}+00:00",
            finished=f"2026-10-04T10:00:{len(self.calls):02d}+00:00",
            derive_pending=len(self.calls) == 1,
        )
        self.results.append(result)
        return result


def _wait_for(predicate: Any, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.01)


def test_watch_polls_threads_derive_pending_and_stops_promptly(lake: LakePaths) -> None:
    config = AcquireConfig(poll_s=1, removable_media=False)
    fake = FakeCycle()
    stop = threading.Event()
    codes: list[int] = []
    thread = threading.Thread(
        target=lambda: codes.append(run_acquire(config, lake, watch=True, stop=stop, cycle=fake)),
        daemon=True,
    )
    thread.start()
    try:
        _wait_for(lambda: len(fake.calls) >= 3)
    finally:  # a failed wait must not leave the watch running (pytest would hang at exit)
        stopped_at = time.monotonic()
        stop.set()
        thread.join(5)

    assert not thread.is_alive() and codes == [0]
    assert time.monotonic() - stopped_at < 0.5  # the poll sleep is interrupted
    assert [pending for _t, pending in fake.calls[:3]] == [False, True, False]
    gaps = [b[0] - a[0] for a, b in zip(fake.calls, fake.calls[1:], strict=False)]
    assert all(0.9 <= gap < 1.5 for gap in gaps)  # start to start, every poll_s
    status = json.loads(lake.status.read_text())
    assert status["last_cycle"]["started"] == fake.results[-1].started


def test_unwritable_status_is_logged_and_the_watch_continues(
    lake: LakePaths, caplog: pytest.LogCaptureFixture
) -> None:
    lake.status.mkdir(parents=True)  # a directory where the file goes: os.replace fails
    config = AcquireConfig(poll_s=1, removable_media=False)
    fake = FakeCycle()
    stop = threading.Event()
    thread = threading.Thread(
        target=run_acquire,
        args=(config, lake),
        kwargs={"watch": True, "stop": stop, "cycle": fake},
        daemon=True,
    )
    with caplog.at_level(logging.ERROR, logger="flashpoint.acquire.watch"):
        thread.start()
        try:
            _wait_for(lambda: len(fake.calls) >= 2)
        finally:  # a failed wait must not leave the watch running (pytest would hang at exit)
            stop.set()
            thread.join(5)

    assert "cannot write acquire status" in caplog.text
    assert [p.name for p in lake.meta.iterdir() if p.name.startswith(".")] == []  # no temp left


def test_one_shot_runs_one_cycle_and_returns_zero(lake: LakePaths) -> None:
    fake = FakeCycle()
    config = AcquireConfig(removable_media=False)
    assert run_acquire(config, lake, stop=threading.Event(), cycle=fake) == 0
    assert len(fake.calls) == 1
    assert lake.status.is_file()


@pytest.mark.parametrize("watch", [False, True])
def test_start_sweeps_stale_parts_from_the_inbox(lake: LakePaths, watch: bool) -> None:
    stale = lake.inbox / "rio" / ("FRC_1.wpilog" + PART_SUFFIX)
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"left by a killed run")
    kept = lake.inbox / "rio" / "FRC_2.wpilog"
    kept.write_bytes(b"whole")
    seen: list[bool] = []
    stop = threading.Event()

    def cycle(*_args: Any, **_kwargs: Any) -> CycleResult:
        seen.append(stale.exists())
        stop.set()
        return CycleResult(started="t", finished="t")

    config = AcquireConfig(removable_media=False)
    run_acquire(config, lake, watch=watch, stop=stop, cycle=cycle)
    assert seen == [False] and kept.exists()


def _robot(host: str, status: SourceStatus = SourceStatus.OK) -> SourceOutcome:
    return SourceOutcome("robot", host, host, status)


def _read(host: str, root: str, free: int) -> SpaceReading:
    return SpaceReading(host, root, free)


def test_status_low_space_persists_until_a_later_reading_recovers(lake: LakePaths) -> None:
    tracker = StatusTracker(lake.status)
    tracker.record(
        CycleResult(
            started="t1",
            sources=[_robot("rio")],
            space_readings=[_read("rio", "/u/logs", 1234), _read("rio", ROOT, 10**9)],
            low_space=[LowSpaceWarning("rio", "/u/logs", 1234)],
        )
    )
    assert tracker.write()
    low = [w for w in json.loads(lake.status.read_text())["warnings"] if w["kind"] == "low-space"]
    assert low == [{"kind": "low-space", "host": "rio", "root": "/u/logs", "free_bytes": 1234}]

    # Robot out of range: no new reading, so the warning stays (and survives a restart).
    tracker.record(CycleResult(started="t2", sources=[_robot("", SourceStatus.UNREACHABLE)]))
    tracker.write()
    restarted = StatusTracker(lake.status)
    restarted.record(CycleResult(started="t3", sources=[_robot("", SourceStatus.UNREACHABLE)]))
    restarted.write()
    assert [w["kind"] for w in json.loads(lake.status.read_text())["warnings"]] == ["low-space"]

    # Robot reachable but df failed for that root (only the other root was read): it stays.
    restarted.record(
        CycleResult(started="t4", sources=[_robot("rio")], space_readings=[_read("rio", ROOT, 1)])
    )
    restarted.write()
    assert [w["root"] for w in json.loads(lake.status.read_text())["warnings"]] == ["/u/logs"]

    # A reading of that root above the threshold clears it.
    restarted.record(
        CycleResult(
            started="t5", sources=[_robot("rio")], space_readings=[_read("rio", "/u/logs", 10**9)]
        )
    )
    restarted.write()
    assert json.loads(lake.status.read_text())["warnings"] == []


def test_low_space_is_per_robot_root_whichever_address_reported_it(lake: LakePaths) -> None:
    tracker = StatusTracker(lake.status)
    tracker.record(
        CycleResult(
            started="t1",
            sources=[_robot("10.68.29.2")],
            space_readings=[_read("10.68.29.2", ROOT, 1234)],
            low_space=[LowSpaceWarning("10.68.29.2", ROOT, 1234)],
        )
    )
    # Still low, now over the tether: one warning, naming the last host that reported it.
    tracker.record(
        CycleResult(
            started="t2",
            sources=[_robot("172.22.11.2")],
            space_readings=[_read("172.22.11.2", ROOT, 999)],
            low_space=[LowSpaceWarning("172.22.11.2", ROOT, 999)],
        )
    )
    low = [w for w in tracker.status["warnings"] if w["kind"] == "low-space"]
    assert low == [{"kind": "low-space", "host": "172.22.11.2", "root": ROOT, "free_bytes": 999}]

    # Recovered, read over mDNS: cleared.
    tracker.record(
        CycleResult(
            started="t3",
            sources=[_robot("roborio-6829-frc.local")],
            space_readings=[_read("roborio-6829-frc.local", ROOT, 10**9)],
        )
    )
    assert tracker.status["warnings"] == []


@pytest.mark.parametrize("free_bytes", ["lots", float("inf")])
def test_corrupt_low_space_in_the_previous_status_is_ignored_at_start(
    lake: LakePaths, free_bytes: object, caplog: pytest.LogCaptureFixture
) -> None:
    warning = {"kind": "low-space", "host": "rio", "root": ROOT, "free_bytes": free_bytes}
    lake.meta.mkdir(parents=True)
    lake.status.write_text(json.dumps({"version": 1, "warnings": [warning]}))
    with caplog.at_level(logging.WARNING, "flashpoint.acquire.watch"):
        tracker = StatusTracker(lake.status)
    assert "malformed" in caplog.text
    assert tracker.record(CycleResult(started="t"))["warnings"] == []


def test_status_holds_sources_failed_files_derive_error_and_backup_slot(lake: LakePaths) -> None:
    failed = {"source_kind": "robot", "source_id": "rio", "remote_path": f"{ROOT}/x.wpilog",
              "reason": "hash-mismatch", "attempts": 3}  # fmt: skip
    result = CycleResult(
        started="2026-10-04T10:00:00+00:00",
        finished="2026-10-04T10:00:09+00:00",
        sources=[_robot("rio"), SourceOutcome("volume", "", "", SourceStatus.ERROR, "boom")],
        failed_pulls=[failed],
        derive=DeriveOutcome(4, "Traceback...\nValueError: bad slot"),
        derive_pending=True,
        errors=["derive: exit 4"],
    )
    tracker = StatusTracker(lake.status)
    tracker.record(result)
    tracker.write()

    status = json.loads(lake.status.read_text())
    assert status["last_cycle"] == {
        "started": "2026-10-04T10:00:00+00:00",
        "finished": "2026-10-04T10:00:09+00:00",
        "stopped": False,
    }
    assert [(s["kind"], s["status"]) for s in status["sources"]] == [
        ("robot", "ok"), ("volume", "error"),
    ]  # fmt: skip
    assert status["sources"][1]["error"] == "boom"
    assert status["failed_files"] == [
        {"source_kind": "robot", "source": "rio", "path": f"{ROOT}/x.wpilog",
         "reason": "hash-mismatch", "attempts": 3},
    ]  # fmt: skip
    assert status["derive"]["pending"] is True
    assert "ValueError: bad slot" in status["derive"]["error"]
    assert status["errors"] == ["derive: exit 4"]
    assert status["backup"] == {"last_time": None, "result": None, "pending": False}
    assert StatusTracker(lake.status).derive_pending  # a restarted watch still owes derive


# --- doctor ----------------------------------------------------------------------------------


def test_doctor_without_status_says_so(
    lake: LakePaths, config_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["doctor", "--lake", str(lake.root)]) == 0
    assert "no acquire status yet" in capsys.readouterr().out


def test_doctor_shows_the_acquire_status(
    lake: LakePaths, config_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    failed = {"source_kind": "volume", "source_id": "VOL-1", "remote_path": "logs/y.hoot",
              "reason": "read-error", "attempts": 3}  # fmt: skip
    tracker = StatusTracker(lake.status)
    tracker.record(
        CycleResult(
            started="2026-10-04T10:00:00+00:00",
            finished="2026-10-04T10:00:09+00:00",
            sources=[_robot("10.68.29.2")],
            low_space=[LowSpaceWarning("10.68.29.2", "/home/lvuser/logs", 52_428_800)],
            failed_pulls=[failed],
            derive=DeriveOutcome(4, "ValueError: bad slot"),
            derive_pending=True,
        )
    )
    tracker.write()

    assert main(["doctor", "--lake", str(lake.root)]) == 0
    out = capsys.readouterr().out
    assert "last cycle 2026-10-04T10:00:09+00:00" in out
    assert "robot 10.68.29.2: ok" in out
    assert "low-space 10.68.29.2 /home/lvuser/logs: 52428800 bytes free" in out
    assert "failed volume VOL-1 logs/y.hoot: read-error" in out
    assert "derive error" in out and "ValueError: bad slot" in out
    assert "last backup never" in out


@pytest.mark.parametrize(
    "content",
    [
        "{not json",
        "[1, 2]",
        json.dumps({"version": 99, "last_cycle": "future"}),
        json.dumps({"version": 1, "sources": [{"kind": "robot"}], "warnings": "low"}),
        json.dumps({"version": 1, "last_cycle": [], "failed_files": [{}]}),
    ],
)
def test_doctor_reports_an_unreadable_status_instead_of_crashing(
    lake: LakePaths, config_dir: Path, capsys: pytest.CaptureFixture[str], content: str
) -> None:
    lake.meta.mkdir(parents=True)
    lake.status.write_text(content)

    assert main(["doctor", "--lake", str(lake.root)]) == 0
    assert "unreadable acquire status" in capsys.readouterr().out
    tracker = StatusTracker(lake.status)  # a watch starting on it starts fresh
    tracker.record(CycleResult(started="t"))
    assert tracker.write()


def test_doctor_tolerates_missing_optional_fields(
    lake: LakePaths, config_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lake.meta.mkdir(parents=True)
    lake.status.write_text(json.dumps({"version": 1, "sources": [{"kind": "robot"}],
                                       "failed_files": [{"path": "x.hoot"}]}))  # fmt: skip

    assert main(["doctor", "--lake", str(lake.root)]) == 0
    out = capsys.readouterr().out
    assert "robot: ?" in out and "x.hoot" in out and "last backup never" in out


SLOW_INGEST = """
import sys, time
from pathlib import Path
Path(sys.argv[1]).write_text(str(__import__("os").getpid()))
time.sleep(60)
"""


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.mark.skipif(sys.platform == "win32", reason="pid liveness via os.kill(pid, 0) is POSIX")
def test_second_signal_during_ingest_keeps_stopping_and_ends_the_ingest(
    lake: LakePaths, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    pid_file = tmp_path / "ingest.pid"
    drop = _put(lake.inbox, "FRC_20260315_120000_GACMP_Q9.wpilog", b"log" * 10, 100)

    def slow_ingest(_paths: Any, _lake: LakePaths) -> list[str]:
        return [sys.executable, "-c", SLOW_INGEST, str(pid_file)]

    def signal_twice() -> None:
        _wait_for(pid_file.exists)
        time.sleep(0.2)
        # Back to back: both are pending before the main thread runs a handler, so both reach
        # the acquire handlers (not the restored defaults).
        signal.raise_signal(signal.SIGINT)
        signal.raise_signal(signal.SIGTERM)

    config = AcquireConfig(settle_s=0, removable_media=False)
    sender = threading.Thread(target=signal_twice)
    sender.start()
    with caplog.at_level(logging.INFO, logger="flashpoint.acquire.watch"):
        code = run_acquire(
            config,
            lake,
            connect=lambda _c: None,
            cycle=functools.partial(run_cycle, ingest_command=slow_ingest),
        )
    sender.join()

    assert code == EXIT_INTERRUPTED
    assert "already stopping" in caplog.text
    assert not _alive(int(pid_file.read_text()))
    assert drop.exists()
    assert json.loads(lake.status.read_text())["last_cycle"]["stopped"] is True
