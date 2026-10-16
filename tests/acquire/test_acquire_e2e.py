"""One acquisition cycle end to end: fake robot, real ingest, derive and Replay build, a real
lake."""

import json
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.watch import run_acquire
from flashpoint.lake.paths import LakePaths
from flashpoint.report.paths import data_dir, file_stem
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.server import FlashpointServer
from tests.acquire.fake_robot import FakeRobot
from tests.acquire.helpers import ledger_rows, put_robot_file, synthetic_wpilog, tree

ROOT = "/home/lvuser/logs"
STABLE = "FRC_20260409_215113_GACMP_Q7.wpilog"
ACTIVE = "FRC_20260409_221500_GACMP_Q8.wpilog"  # newer: the open file, never pulled
NEWER = "FRC_20260409_223000_GACMP_Q9.wpilog"  # once it appears, Q8 is stable and pulled


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
    assert status["report"] == {"pending": False, "error": None}
    # The robot is untouched.
    assert fake_robot.write_attempts == []
    assert tree(fake_robot.root) == robot_before


def _listed(lake: LakePaths) -> list[str]:
    text = (data_dir(lake) / "matches.js").read_text()
    payload = json.loads(text[text.index("(") + 1 : text.rindex(")")])
    return sorted(m["key"] for m in payload["matches"])


def _snapshot(path: Path) -> tuple[int, bytes]:
    return path.stat().st_mtime_ns, path.read_bytes()


@pytest.fixture
def robot_lake(tmp_path: Path, fake_robot: FakeRobot) -> tuple[AcquireConfig, LakePaths]:
    put_robot_file(fake_robot.root, f"{ROOT}/{STABLE}", synthetic_wpilog(), 1000)
    put_robot_file(fake_robot.root, f"{ROOT}/{ACTIVE}", synthetic_wpilog(3), 2000)
    host = f"{fake_robot.host}:{fake_robot.port}"
    return AcquireConfig(hosts=[host], settle_s=0, removable_media=False), LakePaths(
        tmp_path / "lake"
    )


def test_each_cycle_builds_replay_data_for_new_matches_only(
    robot_lake: tuple[AcquireConfig, LakePaths], fake_robot: FakeRobot
) -> None:
    config, lake = robot_lake
    q7 = data_dir(lake) / f"{file_stem('2026gacmp_qm7')}.js"

    assert run_acquire(config, lake, watch=False) == 0
    assert _listed(lake) == ["2026gacmp_qm7"] and q7.is_file()
    built = _snapshot(q7)
    index = _snapshot(data_dir(lake) / "matches.js")

    assert run_acquire(config, lake, watch=False) == 0  # nothing new: no build
    assert _snapshot(q7) == built and _snapshot(data_dir(lake) / "matches.js") == index

    put_robot_file(fake_robot.root, f"{ROOT}/{NEWER}", synthetic_wpilog(4), 3000)
    assert run_acquire(config, lake, watch=False) == 0  # Q8 is stable now
    assert _listed(lake) == ["2026gacmp_qm7", "2026gacmp_qm8"]
    assert (data_dir(lake) / f"{file_stem('2026gacmp_qm8')}.js").is_file()
    assert _snapshot(q7) == built  # only the new match was rebuilt
    status = json.loads(lake.status.read_text())
    assert status["report"] == {"pending": False, "error": None} and status["errors"] == []


@pytest.fixture
def served(robot_lake: tuple[AcquireConfig, LakePaths]) -> Iterator[str]:
    """The app served on the (still empty) lake before any cycle ran."""
    _config, lake = robot_lake
    server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}/"
    server.shutdown()
    server.server_close()
    server.queries.close()


@pytest.mark.browser
def test_running_app_lists_a_new_match_after_a_reload(
    robot_lake: tuple[AcquireConfig, LakePaths], served: str, page: Any
) -> None:
    config, lake = robot_lake
    page.goto(served + "#view=replay")
    page.wait_for_selector(".app-header")
    assert page.query_selector("nav.rail-l a.match-btn") is None

    assert run_acquire(config, lake, watch=False) == 0
    page.reload()
    page.wait_for_selector("nav.rail-l a.match-btn")
    keys = page.eval_on_selector_all("nav.rail-l a.match-btn", "as => as.map(a => a.dataset.match)")
    assert keys == ["2026gacmp_qm7"]
    page.click("nav.rail-l a.match-btn")
    page.wait_for_function("() => location.hash.includes('m=2026gacmp_qm7')")
