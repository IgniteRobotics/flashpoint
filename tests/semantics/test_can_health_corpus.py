"""CAN health on the golden corpus (spec: can-health)."""

from typing import Any

import duckdb
import pytest

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics import can_health
from flashpoint.semantics.derive import Deriver
from flashpoint.semantics.gold import gold_dir

pytestmark = pytest.mark.corpus

Q7 = "2026gacmp_qm7"


def _rows(lake: LakePaths, table: str) -> list[dict[str, Any]]:
    path = gold_dir(lake, table)
    if not any(path.glob("season=*/session_id=*/*.parquet")):
        return []
    cursor = duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{path}/*/*/*.parquet', hive_partitioning = false)"
    )
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, r, strict=True)) for r in cursor.fetchall()]


def _hoot_logs(lake: LakePaths) -> dict[str, str]:
    """hoot log_id -> its file name."""
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT log_id, filename FROM read_parquet('{lake.meta / 'logs.parquet'}')"
        " WHERE kind = 'hoot'"
    ).fetchall()
    return dict(rows)


@pytest.fixture(scope="module")
def tables(derived_corpus_lake: LakePaths) -> dict[str, list[dict[str, Any]]]:
    return {
        t: _rows(derived_corpus_lake, t)
        for t in (can_health.TABLE, can_health.BUS_TABLE, can_health.EVENTS_TABLE)
    }


def test_q7_has_a_row_per_hoot_device_and_phase(tables: dict[str, Any]) -> None:
    q7 = [r for r in tables[can_health.TABLE] if r["match_key"] == Q7]
    slots = {r["slot_id"] for r in q7}
    assert len(slots) == 22  # 17 motors, the gyro, and 4 steer encoders
    assert {(r["slot_id"], r["phase"]) for r in q7} == {
        (s, p) for s in slots for p in ("auto", "teleop", "match")
    }
    assert all(r["reboots"] == 0 and r["reset_count_logged"] for r in q7)
    assert all(r["alignment"] == "high" for r in q7)


def test_q7_logger_gaps_are_not_charged(tables: dict[str, Any]) -> None:
    q7 = [r for r in tables[can_health.TABLE] if r["match_key"] == Q7 and r["phase"] == "match"]
    canivore = [r for r in q7 if r["bus"] != "rio"]
    assert canivore
    for row in canivore:
        assert row["isolated_gaps"] <= 5, row
        assert row["longest_gap_ms"] <= 50, row
    encoders = [r for r in canivore if r["role"].endswith("steer encoder")]
    assert len(encoders) == 4 and all(r["isolated_gaps"] == 0 for r in encoders)


def test_q7_bus_rows(tables: dict[str, Any]) -> None:
    q7 = {(r["bus"], r["phase"]): r for r in tables[can_health.BUS_TABLE] if r["match_key"] == Q7}
    buses = {bus for bus, _ in q7}
    assert len(buses) == 2 and "rio" in buses
    canivore = next(b for b in buses if b != "rio")
    assert q7[(canivore, "match")]["logger_gaps"] > 0
    assert q7[(canivore, "match")]["bus_name"] == "DriveTrain"
    assert q7[("rio", "match")]["bus_name"] == "rio"
    assert all(r["util_max"] is None and r["tx_err_max"] is None for r in q7.values())


def test_q7_has_no_reboot_or_dropout_event(tables: dict[str, Any]) -> None:
    kinds = {e["kind"] for e in tables[can_health.EVENTS_TABLE] if e["match_key"] == Q7}
    assert kinds <= {"bus_gap"}


def test_p2_has_no_rows(tables: dict[str, Any]) -> None:
    for table in tables.values():
        assert not [r for r in table if r["match_key"] == "2026gacmp_pm2"]


def test_2025_hoots_unaligned(tables: dict[str, Any], derived_corpus_lake: LakePaths) -> None:
    q19 = {i for i, name in _hoot_logs(derived_corpus_lake).items() if "GACMP_Q19" in name}
    assert len(q19) == 2
    events = [e for e in tables[can_health.EVENTS_TABLE] if e["log_id"] in q19]
    assert not [e for e in events if e["kind"] == "reboot"]
    assert all(e["t_us"] is None and e["hoot_t_us"] is not None for e in events)
    sessions = {e["session_id"] for e in events}
    assert not [r for r in tables[can_health.TABLE] if r["session_id"] in sessions]


def test_truncated_hoot_has_no_gap_past_its_end(derived_corpus_lake: LakePaths) -> None:
    (truncated,) = [i for i, name in _hoot_logs(derived_corpus_lake).items() if "truncated" in name]
    health = can_health.analyse_hoot(duckdb.connect(), derived_corpus_lake, truncated)
    assert health.last_frame_us is not None
    assert all(g.end_us <= health.last_frame_us for g in health.gaps)


def test_rows_are_deterministic(derived_corpus_lake: LakePaths, tables: dict[str, Any]) -> None:
    deriver = Deriver(derived_corpus_lake, config.config_root())
    try:
        deriver.run(force=True)
    finally:
        deriver.close()
    for table, rows in tables.items():
        key = sorted(rows, key=repr)
        assert sorted(_rows(derived_corpus_lake, table), key=repr) == key, table
