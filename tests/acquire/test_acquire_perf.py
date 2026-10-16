"""Acquisition budgets (design: Budgets): idle cycle < 3 s, watch process RSS <= 150 MB."""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.cycle import SourceStatus, run_cycle
from flashpoint.acquire.pulls import PullLedger, PullStatus
from flashpoint.acquire.removable import RemovableMedia
from flashpoint.acquire.robot import RemoteFile
from flashpoint.acquire.transfer import robot_key
from flashpoint.lake.paths import LakePaths
from tests.acquire.fake_robot import FakeRobot
from tests.acquire.helpers import S, put_robot_file, synthetic_wpilog

pytestmark = pytest.mark.perf

ROOT = "/home/lvuser/logs"
REPO = Path(__file__).resolve().parents[2]
IDLE_BUDGET_S = 3.0
RSS_BUDGET_BYTES = 150 * 2**20
KNOWN_FILES = 150  # the design's "under 200 files"

# The fake robot in its own process, so its memory is never counted with the watch's.
_ROBOT = """
import sys
from pathlib import Path
from tests.acquire.fake_robot import FakeRobot

robot = FakeRobot(Path(sys.argv[1]))
robot.start()
print(robot.port, flush=True)
sys.stdin.read()
robot.stop()
"""

# The watch, run as `flashpoint acquire --watch` would be, reporting its own peak RSS. Linux
# keeps ru_maxrss across exec (a child inherits the parent's high-water mark), so there the
# peak comes from VmHWM, which exec resets. Children (ingest, derive) are not measured.
_WATCH = """
import json, resource, sys
from flashpoint.cli import main

def own_peak():
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    scale = 1 if sys.platform == "darwin" else 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * scale

code = main(sys.argv[1:])
print(json.dumps({"code": code, "peak_bytes": own_peak()}))
"""


def test_idle_cycle_with_a_reachable_robot_is_under_3s(
    tmp_path: Path, fake_robot: FakeRobot, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLASHPOINT_CACHE", str(tmp_path / "cache"))
    lake = LakePaths(tmp_path / "lake")
    host = f"{fake_robot.host}:{fake_robot.port}"
    config = AcquireConfig(hosts=[host], removable_media=False)  # default settle_s=5
    pulls = PullLedger(lake.ledger)
    try:
        for i in range(KNOWN_FILES):
            name = f"FRC_20260314_{100000 + i}_GACMP_Q{i}.wpilog"
            data = synthetic_wpilog()
            put_robot_file(fake_robot.root, f"{ROOT}/{name}", data, 1000 + i)
            known = RemoteFile(ROOT, name, len(data), (1000 + i) * S)
            pulls.record_success(robot_key(host, known), "0" * 64, PullStatus.VERIFIED, False)
        media = RemovableMedia.from_config(config, pulls, lake.inbox, sleep=lambda _s: None)
        slept: list[float] = []  # the settle wait is excluded: nothing new, so it is never asked

        def run() -> float:
            started = time.perf_counter()
            result = run_cycle(
                config, lake, pulls, media, stop=threading.Event(), sleep=slept.append
            )
            elapsed = time.perf_counter() - started
            assert [s.status for s in result.sources] == [SourceStatus.OK]
            assert result.ingest is None and not result.inbox and not result.errors
            return elapsed

        cold, warm = run(), run()
    finally:
        pulls.close()

    print(json.dumps({"idle_cycle_cold_s": round(cold, 3), "idle_cycle_warm_s": round(warm, 3)}))
    assert slept == []
    assert cold < IDLE_BUDGET_S and warm < IDLE_BUDGET_S


@pytest.fixture
def robot_process(tmp_path: Path) -> Iterator[tuple[int, Path]]:
    """A fake robot in a separate process: (port, root)."""
    root = tmp_path / "rio"
    proc = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", _ROBOT, str(root)],
        cwd=REPO,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None and proc.stdin is not None
    try:
        yield int(proc.stdout.readline()), root
    finally:
        proc.stdin.close()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        proc.stdout.close()


def _wait_for(predicate: "object", timeout: float, log: Path) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():  # type: ignore[operator]
        assert time.monotonic() < deadline, f"timed out; watch log:\n{log.read_text()}"
        time.sleep(0.1)


def _verified(db: Path) -> int:
    if not db.exists():
        return 0
    con = sqlite3.connect(db, timeout=10)
    try:
        return int(
            con.execute("SELECT count(*) FROM pulls WHERE status = 'verified'").fetchone()[0]
        )
    except sqlite3.OperationalError:
        return 0
    finally:
        con.close()


def _cycle_stamp(status: Path) -> str | None:
    try:
        return str(json.loads(status.read_text())["last_cycle"]["started"])
    except (OSError, ValueError, KeyError):
        return None


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT to a child process is POSIX-only")
def test_watch_process_peak_rss_is_within_150mb(
    tmp_path: Path, robot_process: tuple[int, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    port, robot_root = robot_process
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "acquire.toml").write_text("settle_s = 1\npoll_s = 1\n")
    env = {
        "FLASHPOINT_CONFIG": str(config_dir),
        "FLASHPOINT_CACHE": str(tmp_path / "cache"),
    }
    lake = LakePaths(tmp_path / "lake")
    log = tmp_path / "watch.log"
    # About 2 MB of samples: a real pull, ingest and derive, not just an empty cycle.
    big = synthetic_wpilog(records=100_000)
    put_robot_file(robot_root, f"{ROOT}/FRC_20260409_215113_GACMP_Q7.wpilog", big, 1000)
    put_robot_file(robot_root, f"{ROOT}/FRC_20260409_221500_GACMP_Q8.wpilog", big, 2000)  # active
    argv = [sys.executable, "-c", _WATCH, "acquire", "--watch", "--lake", str(lake.root),
            "--host", f"127.0.0.1:{port}", "--no-usb"]  # fmt: skip
    with log.open("w") as log_file:
        proc = subprocess.Popen(  # noqa: S603
            argv,
            env={**os.environ, **env},
            stdout=subprocess.PIPE,
            stderr=log_file,
            text=True,
            cwd=REPO,
        )
        assert proc.stdout is not None
        try:
            _wait_for(lambda: _verified(lake.ledger) >= 1, 120, log)
            # The robot gets a newer log: Q8 stops being active and is pulled in a second cycle.
            put_robot_file(robot_root, f"{ROOT}/FRC_20260409_223000_GACMP_Q9.wpilog", big, 3000)
            _wait_for(lambda: _verified(lake.ledger) >= 2, 120, log)

            def derived() -> bool:
                try:
                    status = json.loads(lake.status.read_text())
                except (OSError, ValueError):
                    return False
                return bool(status["derive"]["pending"] is False and status["ingest"] is None)

            _wait_for(derived, 120, log)  # a later, idle cycle: everything settled
            seen = _cycle_stamp(lake.status)
            _wait_for(lambda: _cycle_stamp(lake.status) != seen, 30, log)  # and one more
            proc.send_signal(signal.SIGINT)
            out = proc.stdout.read()
            code = proc.wait(timeout=30)
        finally:
            proc.kill()
            proc.wait()
            proc.stdout.close()

    report = json.loads(out.strip().splitlines()[-1])
    peak_mb = report["peak_bytes"] / 2**20
    print(json.dumps({"watch_peak_rss_mb": round(peak_mb, 1)}))
    assert code == 0 and report["code"] == 0, log.read_text()
    assert report["peak_bytes"] <= RSS_BUDGET_BYTES
