"""One acquisition cycle end to end: fake robot, real ingest and derive, a real lake."""

import json
from pathlib import Path

import pytest

from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.watch import run_acquire
from flashpoint.lake.paths import LakePaths
from tests.acquire.fake_robot import FakeRobot
from tests.acquire.helpers import ledger_rows, put_robot_file, synthetic_wpilog, tree

ROOT = "/home/lvuser/logs"
STABLE = "FRC_20260409_215113_GACMP_Q7.wpilog"
ACTIVE = "FRC_20260409_221500_GACMP_Q8.wpilog"  # newer: the open file, never pulled


@pytest.fixture(autouse=True)
def _isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASHPOINT_CACHE", str(tmp_path / "cache"))


def test_one_cycle_takes_a_stable_log_from_the_robot_to_the_lake(
    tmp_path: Path, fake_robot: FakeRobot
) -> None:
    data = synthetic_wpilog()
    put_robot_file(fake_robot.root, f"{ROOT}/{STABLE}", data, 1000)
    put_robot_file(fake_robot.root, f"{ROOT}/{ACTIVE}", synthetic_wpilog(3), 2000)
    robot_before = tree(fake_robot.root)
    lake = LakePaths(tmp_path / "lake")
    host = f"{fake_robot.host}:{fake_robot.port}"
    config = AcquireConfig(hosts=[host], settle_s=0, removable_media=False)

    code = run_acquire(config, lake, watch=False)

    assert code == 0
    # Pulled, verified and ingested into the ledger.
    (pull,) = ledger_rows(lake.ledger, "SELECT * FROM pulls")
    assert (pull["status"], pull["source_kind"], pull["source_id"]) == ("verified", "robot", host)
    assert pull["remote_path"] == f"{ROOT}/{STABLE}"
    files = ledger_rows(lake.ledger, "SELECT stage FROM files")
    assert [f["stage"] for f in files] == ["success"]
    # Derived: the match is in sessions (a synthetic log has no motors, so no gold rows).
    sessions = ledger_rows(lake.ledger, "SELECT match_key, wpilog_id FROM sessions")
    assert [s["match_key"] for s in sessions] == ["2026gacmp_qm7"]
    assert ledger_rows(lake.ledger, "SELECT gold_rows FROM derived_state") == [{"gold_rows": 0}]
    # The inbox is cleared; raw holds the verified copy.
    assert [p for p in lake.inbox.rglob("*") if p.is_file()] == []
    assert [p.read_bytes() for p in lake.raw.rglob("*") if p.is_file()] == [data]
    # Status file.
    status = json.loads(lake.status.read_text())
    assert status["errors"] == [] and status["failed_files"] == []
    assert status["last_cycle"]["stopped"] is False
    (source,) = status["sources"]
    assert (source["kind"], source["status"], source["skipped_active"]) == ("robot", "ok", 1)
    assert source["transfers"] == {"verified": 1}
    assert status["ingest"]["returncode"] == 0 and status["ingest"]["crashed"] is False
    assert status["derive"] == {"pending": False, "error": None}
    # The robot is untouched.
    assert fake_robot.write_attempts == []
    assert tree(fake_robot.root) == robot_before
