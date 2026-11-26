"""Q7 device enable state in silver (spec: telemetry-lake / Device enable state in silver)."""

from typing import Any

import duckdb
import pytest

from flashpoint import config
from flashpoint.lake.ledger import Ledger
from flashpoint.semantics.robot_config import load_robots, load_seasons
from flashpoint.semantics.silver import silver_dir

pytestmark = pytest.mark.corpus

ENABLED_PHASES = ("auto", "teleop")
DISABLED_PHASES = ("pre", "gap", "post")


@pytest.fixture(scope="module")
def q7(derived_corpus_lake: Any) -> dict[str, Any]:
    ledger = Ledger(derived_corpus_lake.ledger)
    try:
        session = ledger.query("SELECT * FROM sessions WHERE match_key = '2026gacmp_qm7'")[0]
    finally:
        ledger.close()
    path = silver_dir(derived_corpus_lake) / "season=*" / f"session_id={session['session_id']}"
    rel = duckdb.sql(f"SELECT slot_id, metric, phase, value FROM read_parquet('{path}/*.parquet')")
    assert rel is not None
    rows = rel.fetchall()
    root = config.config_root()
    robot = next(
        r for r in load_robots(root / "robots", load_seasons(root / "seasons"))
        if r.robot == session["robot"]
    )  # fmt: skip
    talonfx = {s.id for s in robot.slots if s.model == "TalonFX"}
    return {"rows": rows, "talonfx": talonfx}


def test_q7_every_talonfx_slot_has_enable_rows(q7: dict[str, Any]) -> None:
    present = {slot for slot, _, _, _ in q7["rows"]} & q7["talonfx"]
    with_enable = {slot for slot, metric, _, _ in q7["rows"] if metric == "device_enable"}
    assert len(present) == 17
    assert present <= with_enable


def test_q7_enable_values_follow_the_match(q7: dict[str, Any]) -> None:
    enable = [(phase, value) for _, metric, phase, value in q7["rows"] if metric == "device_enable"]
    assert {value for _, value in enable} == {0.0, 1.0}
    on = [value for phase, value in enable if phase in ENABLED_PHASES]
    off = [value for phase, value in enable if phase in DISABLED_PHASES]
    assert sum(on) / len(on) >= 0.95
    assert sum(off) / len(off) <= 0.01
