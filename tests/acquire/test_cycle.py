import _thread
import hashlib
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from types import SimpleNamespace

import pytest

from flashpoint.acquire import cycle
from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.cycle import (
    CycleResult,
    InboxOutcome,
    SourceStatus,
    run_cycle,
)
from flashpoint.acquire.pulls import PullLedger, PullStatus
from flashpoint.acquire.removable import RemovableMedia
from flashpoint.acquire.robot import AuthErrorThrottle, RobotClient
from flashpoint.acquire.transfer import PART_SUFFIX, TransferStatus
from flashpoint.acquire.volumes import Volume
from flashpoint.lake.ledger import INCOMPLETE_READ, Ledger, Stage
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.raw import file_sha256
from tests.acquire.fake_robot import FakeRobot
from tests.wpilog_builder import WpilogBuilder

ROOT = "/home/lvuser/logs"
S = 10**9
OLD = "FRC_20260314_101500_GACMP_Q6.wpilog"
NEWEST = "FRC_20260314_102233_GACMP_Q7.wpilog"  # the newest wpilog: active
BAD = "FRC_20260314_100000_GACMP_Q5.wpilog"
Connect = Callable[[AcquireConfig], RobotClient | None]


def _wpilog(value: float) -> bytes:
    b = (
        WpilogBuilder()
        .start(1, "NT:/FMSInfo/EventName", "string")
        .start(2, "motor/current", "double")
    )
    return b.string(1, 10, "GACMP").double(2, 20, value).double(2, 30, value + 1).raw_bytes()


def _put(base: Path, relpath: str, data: bytes, mtime_s: int) -> Path:
    target = base / relpath.lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    os.utime(target, ns=(mtime_s * S, mtime_s * S))
    return target


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _no_sleep(_: float) -> None:
    pass


def _inbox_files(lake: LakePaths) -> list[Path]:
    return sorted(p for p in lake.inbox.rglob("*") if p.is_file()) if lake.inbox.exists() else []


def _stage(lake: LakePaths, sha: str) -> tuple[Stage | None, str | None, str | None]:
    ledger = Ledger(lake.ledger)
    try:
        rows = ledger.query("SELECT stage, reason, warnings FROM files WHERE sha256 = ?", (sha,))
    finally:
        ledger.close()
    if not rows:
        return None, None, None
    return Stage(rows[0]["stage"]), rows[0]["reason"], rows[0]["warnings"]


def _crash(_paths: Sequence[Path], _lake: LakePaths) -> list[str]:
    return [sys.executable, "-c", "raise SystemExit(3)"]


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASHPOINT_CACHE", str(tmp_path / "cache"))


@pytest.fixture
def lake(tmp_path: Path) -> LakePaths:
    return LakePaths(tmp_path / "lake")


@pytest.fixture
def pulls(lake: LakePaths) -> Iterator[PullLedger]:
    ledger = PullLedger(lake.ledger)
    yield ledger
    ledger.close()


@pytest.fixture
def config(fake_robot: FakeRobot) -> AcquireConfig:
    return AcquireConfig(
        hosts=[f"{fake_robot.host}:{fake_robot.port}"], roots=[ROOT, "/u/logs"], settle_s=5
    )


@pytest.fixture
def connect(tmp_path: Path) -> Connect:
    def connect(config: AcquireConfig) -> RobotClient | None:
        return RobotClient.connect(
            config,
            known_robots=tmp_path / "known-robots.json",
            throttle=AuthErrorThrottle(),
            tcp_timeout=0.5,
            auth_timeout=5.0,
        )

    return connect


def _no_robot(_config: AcquireConfig) -> RobotClient | None:
    return None


class Detector:
    def __init__(self, *plugged: Volume, on_call: Callable[[], None] | None = None) -> None:
        self.plugged = list(plugged)
        self.on_call = on_call

    def __call__(self) -> list[Volume]:
        if self.on_call:
            self.on_call()
        return list(self.plugged)


def _media(
    pulls: PullLedger, lake: LakePaths, detect: Callable[[], list[Volume]] | None = None
) -> RemovableMedia:
    return RemovableMedia(
        pulls, lake.inbox, enabled=True, settle_s=5, sleep=_no_sleep, detect=detect or Detector()
    )


def _run(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    media: RemovableMedia,
    connect: Connect,
    **kwargs: object,
) -> CycleResult:
    kwargs.setdefault("sleep", _no_sleep)
    kwargs.setdefault("stop", threading.Event())
    return run_cycle(config, lake, pulls, media, connect=connect, **kwargs)  # type: ignore[arg-type]


@pytest.fixture
def robot_logs(fake_robot: FakeRobot) -> dict[str, bytes]:
    logs = {OLD: _wpilog(1.0), NEWEST: _wpilog(2.0)}
    _put(fake_robot.root, f"{ROOT}/{OLD}", logs[OLD], 1000)
    _put(fake_robot.root, f"{ROOT}/{NEWEST}", logs[NEWEST], 2000)
    return logs


# --- order, ingest and clearing ------------------------------------------------------------


def test_cycle_pulls_robot_then_volume_then_ingests_and_clears_the_inbox(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    robot_logs: dict[str, bytes],
    tmp_path: Path,
) -> None:
    events: list[str] = []
    stick = Volume("VOL-1", "ROBOTLOGS", tmp_path / "Volumes" / "ROBOTLOGS")
    stick_log = _wpilog(3.0)
    _put(stick.mount, "logs/FRC_20260313_090000_GACMP_Q1.wpilog", stick_log, 500)

    def tracked_connect(c: AcquireConfig) -> RobotClient | None:
        events.append("robot")
        return connect(c)

    def tracked_ingest(paths: Sequence[Path], lk: LakePaths) -> list[str]:
        events.append("ingest")
        assert len(_inbox_files(lake)) == 2  # robot and stick files are both in the inbox
        return cycle.ingest_command(paths, lk)

    media = _media(pulls, lake, Detector(stick, on_call=lambda: events.append("volumes")))
    result = _run(config, lake, pulls, media, tracked_connect, ingest_command=tracked_ingest)

    assert events == ["robot", "volumes", "ingest"]
    robot, volume = result.sources
    assert (robot.kind, robot.status, volume.kind, volume.id) == (
        "robot", SourceStatus.OK, "volume", "VOL-1",
    )  # fmt: skip
    assert [t.status for t in robot.transfers] == [TransferStatus.VERIFIED]  # only the old log
    assert robot.skipped_active == 1
    assert [t.status for t in volume.transfers] == [TransferStatus.VERIFIED]
    assert result.ingest is not None and result.ingest.returncode == 0
    assert not result.ingest.crashed
    for data in (robot_logs[OLD], stick_log):
        assert _stage(lake, _sha(data))[0] == Stage.SUCCESS
    assert {f.outcome for f in result.inbox} == {InboxOutcome.SUCCESS}
    assert _inbox_files(lake) == []
    assert [p for p in lake.inbox.iterdir()] == []  # source dirs pruned, inbox root kept
    assert result.raw_changed and result.ledger_changed
    assert result.derive is not None and result.derive.returncode == 0
    assert not result.derive_pending


def test_quarantined_file_is_cleared_and_not_pulled_again(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    fake_robot: FakeRobot,
) -> None:
    bad = b"NOTALOG" * 4
    _put(fake_robot.root, f"{ROOT}/{BAD}", bad, 1000)
    _put(fake_robot.root, f"{ROOT}/{NEWEST}", _wpilog(2.0), 2000)  # active; keeps BAD stable
    media = _media(pulls, lake)

    result = _run(config, lake, pulls, media, connect)

    (entry,) = result.inbox
    assert entry.outcome == InboxOutcome.QUARANTINED
    assert _stage(lake, entry.sha256)[:2] == (Stage.QUARANTINED, "invalid-header")
    assert _inbox_files(lake) == []
    fake_robot.reset_counters()
    again = _run(config, lake, pulls, media, connect)
    assert again.sources[0].transfers == []
    assert fake_robot.bytes_served == 0
    assert again.ingest is None


def test_already_ingested_file_is_skipped_and_cleared(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    robot_logs: dict[str, bytes],
) -> None:
    media = _media(pulls, lake)
    _run(config, lake, pulls, media, connect)
    dropped = _put(lake.inbox, "copies/again.wpilog", robot_logs[OLD], 100)
    ingests: list[object] = []

    def ingest(paths: Sequence[Path], lk: LakePaths) -> list[str]:
        ingests.append(paths)
        return cycle.ingest_command(paths, lk)

    result = _run(config, lake, pulls, media, _no_robot, ingest_command=ingest)

    assert ingests == [] and result.ingest is None  # already done: no ingest launched
    (entry,) = result.inbox
    assert (entry.path, entry.outcome) == (dropped, InboxOutcome.SKIPPED)
    assert entry.origin == "pull"  # same bytes as a pulled file: known by its hash
    assert _inbox_files(lake) == []
    assert not result.raw_changed


def test_ingest_crash_keeps_the_inbox_and_the_next_cycle_retries(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    robot_logs: dict[str, bytes],
) -> None:
    media = _media(pulls, lake)

    crashed = _run(config, lake, pulls, media, connect, ingest_command=_crash)

    assert crashed.ingest is not None
    assert (crashed.ingest.returncode, crashed.ingest.crashed) == (3, True)
    (kept,) = _inbox_files(lake)
    assert kept.read_bytes() == robot_logs[OLD]
    assert [f.outcome for f in crashed.inbox] == [InboxOutcome.KEPT]
    assert not crashed.raw_changed

    retried = _run(config, lake, pulls, media, connect)

    assert retried.sources[0].transfers == []  # already pulled; the inbox copy is retried
    assert [(f.origin, f.outcome) for f in retried.inbox] == [("pull", InboxOutcome.SUCCESS)]
    assert _inbox_files(lake) == []


def test_partial_files_and_other_files_are_never_ingested_or_cleared(
    config: AcquireConfig, lake: LakePaths, pulls: PullLedger
) -> None:
    part = _put(lake.inbox, f"10.68.29.2/{OLD}{PART_SUFFIX}", _wpilog(1.0), 100)
    notes = _put(lake.inbox, "notes.txt", b"hello", 100)

    result = _run(config, lake, pulls, _media(pulls, lake), _no_robot)

    assert result.ingest is None
    assert result.inbox == []
    assert part.exists() and notes.exists()


# --- manual drops --------------------------------------------------------------------------


def test_manual_drop_is_ingested_only_once_stable(
    config: AcquireConfig, lake: LakePaths, pulls: PullLedger
) -> None:
    dropped = _put(lake.inbox, "FRC_20260315_120000_GACMP_Q9.wpilog", _wpilog(5.0)[:40], 100)
    waits: list[float] = []

    def still_copying(seconds: float) -> None:
        waits.append(seconds)
        dropped.write_bytes(_wpilog(5.0))  # the copy finishes during the settle wait

    media = _media(pulls, lake)
    first = _run(config, lake, pulls, media, _no_robot, sleep=still_copying)

    assert waits == [5]
    assert first.ingest is None
    assert [(f.origin, f.outcome) for f in first.inbox] == [("manual", InboxOutcome.UNSTABLE)]
    assert dropped.exists()

    second = _run(config, lake, pulls, media, _no_robot)

    assert [(f.origin, f.outcome) for f in second.inbox] == [("manual", InboxOutcome.SUCCESS)]
    assert _stage(lake, _sha(_wpilog(5.0)))[0] == Stage.SUCCESS
    assert not dropped.exists()


def test_unstable_drop_does_not_hold_back_a_pulled_file(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    robot_logs: dict[str, bytes],
) -> None:
    dropped = _put(lake.inbox, "drop/partial.wpilog", b"WPILOG", 100)

    def sleep(_: float) -> None:
        with dropped.open("ab") as f:
            f.write(b"more")

    media = _media(pulls, lake)
    result = _run(config, lake, pulls, media, connect, sleep=sleep)

    outcomes = {f.origin: f.outcome for f in result.inbox}
    assert outcomes == {"pull": InboxOutcome.SUCCESS, "manual": InboxOutcome.UNSTABLE}
    assert _inbox_files(lake) == [dropped]
    assert _stage(lake, file_sha256(dropped))[0] is None  # never handed to ingest


# --- incomplete-read -----------------------------------------------------------------------


def test_include_active_pulls_get_incomplete_read(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    robot_logs: dict[str, bytes],
) -> None:
    result = _run(config, lake, pulls, _media(pulls, lake), connect, include_active=True)

    by_name = {f.path.name: f for f in result.inbox}
    assert set(by_name) == {OLD, NEWEST}
    assert _stage(lake, by_name[NEWEST].sha256)[2] == INCOMPLETE_READ
    assert _stage(lake, by_name[OLD].sha256)[2] is None
    (active_row,) = pulls.query("SELECT * FROM pulls WHERE include_active = 1")
    assert active_row["remote_path"] == f"{ROOT}/{NEWEST}"
    assert _inbox_files(lake) == []


# --- failure isolation, stop, warnings -----------------------------------------------------


def test_a_failing_source_does_not_stop_the_others(
    config: AcquireConfig, lake: LakePaths, pulls: PullLedger, tmp_path: Path
) -> None:
    def broken_robot(_config: AcquireConfig) -> RobotClient | None:
        raise RuntimeError("unexpected")

    def broken_detect() -> list[Volume]:
        raise ValueError("also unexpected")

    drop = _put(lake.inbox, "FRC_20260315_120000_GACMP_Q9.wpilog", _wpilog(9.0), 100)
    result = _run(config, lake, pulls, _media(pulls, lake, broken_detect), broken_robot)

    robot, volumes = result.sources
    assert (robot.status, volumes.status) == (SourceStatus.ERROR, SourceStatus.ERROR)
    assert robot.error is not None and "unexpected" in robot.error
    assert volumes.error is not None and "also unexpected" in volumes.error
    assert [f.outcome for f in result.inbox] == [InboxOutcome.SUCCESS]
    assert not drop.exists()


def test_unreachable_robot_is_reported(
    config: AcquireConfig, lake: LakePaths, pulls: PullLedger
) -> None:
    result = _run(config, lake, pulls, _media(pulls, lake), _no_robot)
    (robot,) = result.sources
    assert (robot.kind, robot.status) == ("robot", SourceStatus.UNREACHABLE)
    assert result.ingest is None and not result.ledger_changed


def test_stop_skips_the_remaining_steps(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    robot_logs: dict[str, bytes],
    tmp_path: Path,
) -> None:
    stop = threading.Event()
    stick = Volume("VOL-1", "ROBOTLOGS", tmp_path / "stick")
    _put(stick.mount, "a.wpilog", _wpilog(3.0), 500)
    ingests: list[object] = []

    def ingest(paths: Sequence[Path], lk: LakePaths) -> list[str]:
        ingests.append(paths)
        return cycle.ingest_command(paths, lk)

    media = _media(pulls, lake, Detector(stick, on_call=stop.set))  # stop during volume step
    result = _run(config, lake, pulls, media, connect, stop=stop, ingest_command=ingest)

    assert result.stopped
    assert result.sources[0].transfers[0].status == TransferStatus.VERIFIED
    assert ingests == [] and result.ingest is None
    assert len(_inbox_files(lake)) == 1  # the robot file waits for the next cycle


def test_low_space_and_size_verified_warnings(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    fake_robot: FakeRobot,
    robot_logs: dict[str, bytes],
) -> None:
    fake_robot.df_free_kb = 50 * 1024  # 50 MiB < 100 MB threshold
    fake_robot.no_sha256sum = True

    result = _run(config, lake, pulls, _media(pulls, lake), connect)

    host = f"{fake_robot.host}:{fake_robot.port}"
    assert {(w.host, w.root, w.free_bytes) for w in result.low_space} == {
        (host, ROOT, 50 * 1024 * 1024), (host, "/u/logs", 50 * 1024 * 1024),
    }  # fmt: skip
    assert {(r.host, r.root) for r in result.space_readings} == {(host, ROOT), (host, "/u/logs")}
    assert result.size_verified_hosts == [host]
    assert result.sources[0].transfers[0].status == TransferStatus.SIZE_VERIFIED


def test_low_space_is_logged_once_per_reading_each_cycle(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    fake_robot: FakeRobot,
    robot_logs: dict[str, bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_robot.df_free_kb = 50 * 1024
    host = f"{fake_robot.host}:{fake_robot.port}"
    for _ in range(2):
        caplog.clear()
        with caplog.at_level(logging.WARNING, "flashpoint.acquire.cycle"):
            _run(config, lake, pulls, _media(pulls, lake), connect)
        low = [r.getMessage() for r in caplog.records if "low space" in r.getMessage()]
        assert sorted(low) == [
            f"robot {host}: low space on {root}: 50 MB free" for root in (ROOT, "/u/logs")
        ]


def test_failed_pulls_are_reported(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    fake_robot: FakeRobot,
    robot_logs: dict[str, bytes],
) -> None:
    fake_robot.wrong_hash = True
    media = _media(pulls, lake)
    for _ in range(3):
        result = _run(config, lake, pulls, media, connect)

    (failed,) = result.failed_pulls
    assert (failed["remote_path"], failed["status"], failed["reason"]) == (
        f"{ROOT}/{OLD}", PullStatus.FAILED, "hash-mismatch",
    )  # fmt: skip
    assert result.ingest is None


def test_result_serializes_to_json(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    fake_robot: FakeRobot,
    robot_logs: dict[str, bytes],
) -> None:
    fake_robot.df_free_kb = 1
    result = _run(config, lake, pulls, _media(pulls, lake), connect)

    data = json.loads(json.dumps(result.as_dict()))
    assert data["sources"][0]["transfers"][0]["status"] == "verified"
    assert data["low_space"][0]["free_bytes"] == 1024
    assert data["inbox"][0]["outcome"] == "success"
    assert data["ingest"]["returncode"] == 0


def test_backup_hook_runs_last_with_the_result(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    connect: Connect,
    robot_logs: dict[str, bytes],
) -> None:
    seen: list[tuple[bool, int]] = []

    def backup(result: CycleResult) -> None:
        seen.append((result.raw_changed, len(_inbox_files(lake))))

    _run(config, lake, pulls, _media(pulls, lake), connect, backup=backup)

    assert seen == [(True, 0)]


_SLOW_INGEST = """
import json, pathlib, subprocess, sys, time
real, marker, beat = sys.argv[1], sys.argv[2], sys.argv[3]
subprocess.run(json.loads(real), check=True, stdout=subprocess.DEVNULL)
pulse = "import pathlib, sys, time\\nwhile True:\\n"
pulse += "    pathlib.Path(sys.argv[1]).write_text(str(time.monotonic_ns()))\\n    time.sleep(0.05)"
subprocess.Popen([sys.executable, "-c", pulse, beat])
# Signal "ingested" only once the grandchild is beating, or a stop can end it before it starts.
while not pathlib.Path(beat).exists():
    time.sleep(0.01)
pathlib.Path(marker).write_text("done")
time.sleep(30)
"""


def test_stop_during_ingest_ends_its_process_group_and_keeps_the_inbox(
    config: AcquireConfig, lake: LakePaths, pulls: PullLedger, tmp_path: Path
) -> None:
    drop = _put(lake.inbox, "FRC_20260315_120000_GACMP_Q9.wpilog", _wpilog(9.0), 100)
    stop = threading.Event()
    marker, beat = tmp_path / "ingested", tmp_path / "heartbeat"

    def stop_once_ingested() -> None:
        while not marker.exists():
            time.sleep(0.05)
        stop.set()

    def slow_ingest(paths: Sequence[Path], lk: LakePaths) -> list[str]:
        threading.Thread(target=stop_once_ingested, daemon=True).start()
        real = json.dumps(cycle.ingest_command(paths, lk))
        return [sys.executable, "-c", _SLOW_INGEST, real, str(marker), str(beat)]

    result = _run(
        config, lake, pulls, _media(pulls, lake), _no_robot, stop=stop, ingest_command=slow_ingest
    )
    stopped_at = time.monotonic()

    assert result.ingest is not None and result.ingest.stopped
    assert result.stopped and result.inbox == []
    assert drop.exists()  # not cleared on a stop
    # What ingest stored before the stop still counts for backup and derive.
    assert result.raw_changed and result.ledger_changed and result.derive_pending
    assert _stage(lake, _sha(_wpilog(9.0)))[0] == Stage.SUCCESS
    # The grandchild (same process group) is gone: its heartbeat stops.
    time.sleep(0.3)
    last = beat.read_text()
    time.sleep(0.3)
    assert beat.read_text() == last
    assert time.monotonic() - stopped_at < 5


@pytest.mark.skipif(sys.platform == "win32", reason="pid liveness via os.kill(pid, 0) is POSIX")
def test_keyboard_interrupt_during_ingest_ends_its_process_group(
    config: AcquireConfig, lake: LakePaths, pulls: PullLedger, tmp_path: Path
) -> None:
    _put(lake.inbox, "FRC_20260315_120000_GACMP_Q9.wpilog", _wpilog(9.0), 100)
    pid_file = tmp_path / "ingest.pid"
    script = "import os, sys, time; open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(60)"

    def interrupt_once_started() -> None:
        while not pid_file.exists() or not pid_file.read_text():
            time.sleep(0.02)
        _thread.interrupt_main()

    def slow_ingest(_paths: Sequence[Path], _lk: LakePaths) -> list[str]:
        threading.Thread(target=interrupt_once_started, daemon=True).start()
        return [sys.executable, "-c", script, str(pid_file)]

    with pytest.raises(KeyboardInterrupt):
        _run(config, lake, pulls, _media(pulls, lake), _no_robot, ingest_command=slow_ingest)

    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)


def test_drop_landing_during_the_settle_wait_is_not_ingested(
    config: AcquireConfig, lake: LakePaths, pulls: PullLedger
) -> None:
    stable = _put(lake.inbox, "FRC_20260315_120000_GACMP_Q9.wpilog", _wpilog(9.0), 100)
    late = lake.inbox / "late" / "FRC_20260315_130000_GACMP_Q10.wpilog"

    def sleep(_: float) -> None:
        late.parent.mkdir(parents=True, exist_ok=True)
        late.write_bytes(_wpilog(10.0)[:30])  # a copy that has only just started

    result = _run(config, lake, pulls, _media(pulls, lake), _no_robot, sleep=sleep)

    assert [(f.path, f.outcome) for f in result.inbox] == [(stable, InboxOutcome.SUCCESS)]
    assert _stage(lake, _sha(late.read_bytes()))[0] is None
    assert late.exists()


def _derive_exit(code: int) -> Callable[[LakePaths], list[str]]:
    def command(_lake: LakePaths) -> list[str]:
        return [sys.executable, "-c", f"print('derive said boom'); raise SystemExit({code})"]

    return command


def test_failing_derive_is_reported_and_retried_until_it_succeeds(
    config: AcquireConfig, lake: LakePaths, pulls: PullLedger
) -> None:
    drop = _put(lake.inbox, "FRC_20260315_120000_GACMP_Q9.wpilog", _wpilog(9.0), 100)
    media = _media(pulls, lake)

    failed = _run(config, lake, pulls, media, _no_robot, derive_command=_derive_exit(4))

    assert failed.derive is not None and failed.derive.returncode == 4
    assert "derive said boom" in failed.derive.output
    assert "derive: exit 4" in failed.errors
    assert failed.derive_pending
    assert not drop.exists()  # ingested: raw holds it, derive is retried on its own
    assert failed.as_dict()["derive"]["returncode"] == 4

    idle = _run(config, lake, pulls, media, _no_robot, derive_pending=True)

    assert idle.ingest is None  # nothing new: only derive runs
    assert idle.derive is not None and idle.derive.returncode == 0
    assert not idle.derive_pending and idle.errors == []

    after = _run(config, lake, pulls, media, _no_robot, derive_pending=idle.derive_pending)

    assert after.derive is None and not after.derive_pending


# --- ingest batches and process groups ---------------------------------------------------------


def test_ingest_batches_stay_under_the_windows_command_line_limit(lake: LakePaths) -> None:
    long_paths = [lake.inbox / ("d" * 200) / f"{'x' * 800}_{i}.wpilog" for i in range(100)]
    batches = list(cycle._path_batches(long_paths, lake))
    assert len(batches) > 1
    for batch in batches:
        argv = cycle.ingest_command(batch, lake)
        assert len(subprocess.list2cmdline(argv)) <= cycle.INGEST_ARGV_CHARS
    assert [p for b in batches for p in b] == long_paths

    short_paths = [Path(f"{i}.wpilog") for i in range(450)]
    counts = [len(b) for b in cycle._path_batches(short_paths, lake)]
    assert counts == [cycle.INGEST_BATCH, cycle.INGEST_BATCH, 50]


class _UnsignalableProc:
    """A Windows child whose CTRL_BREAK cannot be delivered (it already left its console)."""

    pid = 4242

    def __init__(self) -> None:
        self.calls: list[str] = []

    def send_signal(self, _sig: int) -> None:
        self.calls.append("signal")
        raise OSError(87, "The parameter is incorrect")

    def kill(self) -> None:
        self.calls.append("kill")

    def wait(self, timeout: float | None = None) -> int:
        self.calls.append("wait")
        return 1


def test_windows_stop_kills_and_waits_when_ctrl_break_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cycle, "sys", SimpleNamespace(platform="win32"))
    monkeypatch.setattr(signal, "CTRL_BREAK_EVENT", 1, raising=False)
    proc = _UnsignalableProc()
    cycle._end_process_group(proc)  # type: ignore[arg-type]
    assert proc.calls == ["signal", "kill", "wait"]
