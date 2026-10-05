"""Time to lake on real logs (spec: acquisition-service / Time to lake): within 5 minutes."""

import json
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from flashpoint import config as flashpoint_config
from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.watch import run_acquire
from flashpoint.lake.paths import LakePaths
from tests.acquire.fake_robot import FakeRobot
from tests.acquire.helpers import ledger_rows, put_robot_file, set_dir_mtime, synthetic_wpilog

pytestmark = pytest.mark.corpus

BUDGET_S = 300.0
WPILOG_ROOT = "/home/lvuser/logs"
HOOT_ROOT = "/u/logs"
SESSION_DIR = "session-2026-04-09_21-51-06"


# Above the global 300 s timeout so a slow run fails on its own budget assertion, not a kill.
@pytest.mark.timeout(BUDGET_S * 2)
def test_q7_match_reaches_the_derived_tables_within_5_minutes(
    corpus_group: Callable[[str], list[Path]],
    fake_robot: FakeRobot,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An isolated cache (known-robots.json) that still has the downloaded converter.
    cache = tmp_path / "cache"
    cache.mkdir()
    owlet = flashpoint_config.cache_root() / "owlet"
    if owlet.is_dir():
        (cache / "owlet").symlink_to(owlet, target_is_directory=True)
    monkeypatch.setenv("FLASHPOINT_CACHE", str(cache))

    wpilogs = [p for p in corpus_group("2026-gacmp-q7") if p.suffix == ".wpilog"]
    hoots = [p for p in corpus_group("2026-gacmp-q7") if p.suffix == ".hoot"]
    assert len(wpilogs) == 1 and len(hoots) == 2
    # The match's wpilog and hoots are old; a newer wpilog and hoot session (the robot's
    # current power-on) are the active ones, so the match is stable and gets pulled.
    put_robot_file(fake_robot.root, f"{WPILOG_ROOT}/{wpilogs[0].name}", wpilogs[0], 1000)
    put_robot_file(
        fake_robot.root, f"{WPILOG_ROOT}/FRC_20260409_230000.wpilog", synthetic_wpilog(), 3000
    )
    for hoot in hoots:
        put_robot_file(fake_robot.root, f"{HOOT_ROOT}/{SESSION_DIR}/{hoot.name}", hoot, 1000)
    put_robot_file(fake_robot.root, f"{HOOT_ROOT}/session-newest/rio.hoot", b"open", 3000)
    set_dir_mtime(fake_robot.root / HOOT_ROOT.lstrip("/") / SESSION_DIR, 1000)
    set_dir_mtime(fake_robot.root / HOOT_ROOT.lstrip("/") / "session-newest", 3000)
    lake = LakePaths(tmp_path / "lake")
    config = AcquireConfig(
        hosts=[f"{fake_robot.host}:{fake_robot.port}"], removable_media=False
    )  # the default 5 s settle counts toward the time

    started = time.perf_counter()
    code = run_acquire(config, lake, watch=False)
    elapsed = time.perf_counter() - started

    print(json.dumps({"time_to_lake_s": round(elapsed, 1)}))
    assert code == 0
    assert elapsed < BUDGET_S
    pulls = ledger_rows(lake.ledger, "SELECT remote_path, status FROM pulls")
    assert sorted(str(p["remote_path"]).rsplit("/", 1)[1] for p in pulls) == sorted(
        [wpilogs[0].name, *(h.name for h in hoots)]
    )
    assert {p["status"] for p in pulls} == {"verified"}
    q7 = ledger_rows(
        lake.ledger,
        "SELECT session_id, wpilog_id FROM sessions WHERE match_key = '2026gacmp_qm7'",
    )
    assert len(q7) == 1 and q7[0]["wpilog_id"]
    state = ledger_rows(
        lake.ledger,
        f"SELECT gold_rows FROM derived_state WHERE session_id = '{q7[0]['session_id']}'",
    )
    assert state and int(state[0]["gold_rows"]) > 0  # type: ignore[call-overload]
    assert any((lake.root / "gold" / "match_features").glob("season=*/session_id=*/*.parquet"))
    assert [p for p in lake.inbox.rglob("*") if p.is_file()] == []
