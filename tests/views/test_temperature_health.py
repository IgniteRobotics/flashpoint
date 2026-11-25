"""History health with no temperature: N/A, NEEDS PRO and NO DATA (lifetime-trends, "Device
table and summary tiles"; mvp-outside-team-logs 6.3)."""

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from flashpoint import config
from flashpoint.anomaly import detect
from flashpoint.lake.paths import LakePaths
from flashpoint.views.anomalies import AnomalyQueries
from flashpoint.views.queries import Filters, HistoryQueries

MOTORS = ("drive-fl", "hood")
SENSORS = (("gyro", "not-tracked"), ("encoder-fl", "not-tracked"))


def _key(i: int) -> str:
    return f"2026syn_qm{i + 1}"


def _write(lake: LakePaths, table: str, i: int, rows: list[dict[str, Any]]) -> None:
    path = lake.root / "gold" / table / "season=2026" / f"session_id=s{i:02d}" / "part-0.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, infer_schema_length=None).write_parquet(path)


def _lake(root: Path, matches: int, temp: Any) -> LakePaths:
    """`temp(slot, i)` -> (temp_max_c, temp_status) for each motor and match."""
    lake = LakePaths(root)
    for i in range(matches):
        base = {"match_key": _key(i), "session_id": f"s{i:02d}", "robot": "2026-comp",
                "phase": "match", "alignment": "high", "subsystem": "x"}  # fmt: skip
        features, cans = [], []
        for slot in MOTORS:
            value, status = temp(slot, i)
            unit = {"slot_id": slot, "unit_id": f"ctre:{slot.upper()}", "role": slot}
            features.append({**base, **unit, "temp_max_c": value, "temp_status": status,
                             "supply_current_p95": 30.0})  # fmt: skip
            cans.append({**base, **unit, "isolated_gaps": 0.0, "temp_status": status})
        for slot, status in SENSORS:
            unit = {"slot_id": slot, "unit_id": f"ctre:{slot.upper()}", "role": slot}
            cans.append({**base, **unit, "isolated_gaps": 0.0, "temp_status": status})
        _write(lake, "match_features", i, features)
        _write(lake, "can_health", i, cans)
    return lake


def _devices(queries: HistoryQueries, metric: str) -> dict[str, dict[str, Any]]:
    return {r["unit_id"]: r for r in queries.devices(metric, Filters())}


def _health(queries: HistoryQueries, metric: str) -> dict[str, str]:
    return {u: r["health"] for u, r in _devices(queries, metric).items()}


def test_gyro_and_encoders_read_n_a(tmp_path: Path) -> None:
    lake = _lake(tmp_path / "lake", 3, lambda slot, i: (40.0, "logged"))
    queries = HistoryQueries(lake)
    try:
        assert _health(queries, "isolated_gaps") == {
            "ctre:DRIVE-FL": "OK", "ctre:HOOD": "OK", "ctre:GYRO": "N/A", "ctre:ENCODER-FL": "N/A",
        }  # fmt: skip
    finally:
        queries.close()


def test_non_pro_lake(tmp_path: Path) -> None:
    lake = _lake(tmp_path / "lake", 3, lambda slot, i: (None, "needs-pro"))
    queries = HistoryQueries(lake)
    try:
        peak = _devices(queries, "peak_temp")
        assert {u: r["health"] for u, r in peak.items()} == {
            "ctre:DRIVE-FL": "NEEDS-PRO", "ctre:HOOD": "NEEDS-PRO",
        }  # fmt: skip
        assert all(r["latest"] is None for r in peak.values())
        assert _health(queries, "isolated_gaps")["ctre:GYRO"] == "N/A"
        assert queries.summary(Filters())["units_warn_hot"] == 0
    finally:
        queries.close()


def test_pro_for_part_of_the_range(tmp_path: Path) -> None:
    lake = _lake(
        tmp_path / "lake", 10, lambda slot, i: (50.0, "logged") if i < 5 else (None, "needs-pro")
    )
    queries = HistoryQueries(lake)
    try:
        row = _devices(queries, "peak_temp")["ctre:HOOD"]
        assert row["health"] == "OK"
        # The fixture has no session times, so the line is in match-key order: count the gaps.
        assert sorted(row["spark"], key=lambda v: v is None) == [50.0] * 5 + [None] * 5
    finally:
        queries.close()


def test_missing_temperature_on_a_pro_hoot(tmp_path: Path) -> None:
    lake = _lake(tmp_path / "lake", 3, lambda slot, i: (None, "missing"))
    queries = HistoryQueries(lake)
    try:
        assert set(_health(queries, "peak_temp").values()) == {"NO-DATA"}
    finally:
        queries.close()


def test_old_gold_without_a_status_reads_no_data(tmp_path: Path) -> None:
    lake = _lake(tmp_path / "lake", 3, lambda slot, i: (None, None))
    queries = HistoryQueries(lake)
    try:
        assert set(_health(queries, "peak_temp").values()) == {"NO-DATA"}
    finally:
        queries.close()


def test_anomaly_on_a_non_pro_unit(tmp_path: Path) -> None:
    lake = _lake(tmp_path / "lake", 3, lambda slot, i: (None, "needs-pro"))
    row: dict[str, Any] = dict.fromkeys(detect.SCHEMA)
    row.update({
        "id": "6" * 16, "subject": "ctre:HOOD", "subject_key": "ctre:HOOD", "subject_kind": "unit",
        "robot": "2026-comp", "slot_id": "hood", "method": "STEP", "metric": "stator_current_p95",
        "severity": "MED", "sensitivity": "normal", "title": "step", "onset_match_key": _key(1),
        "last_match_key": _key(2), "flagged_keys": json.dumps([_key(1), _key(2)]), "evidence": "{}",
    })  # fmt: skip
    path = lake.root / "gold" / "anomalies" / "anomalies.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame([row], schema=detect.SCHEMA).write_parquet(path)
    anomalies = AnomalyQueries(lake, config.config_root())
    queries = HistoryQueries(lake, anomalies=anomalies)
    try:
        health = _health(queries, "peak_temp")
        assert (health["ctre:HOOD"], health["ctre:DRIVE-FL"]) == ("WARN", "NEEDS-PRO")
        assert queries.summary(Filters())["units_warn_hot"] == 1
    finally:
        queries.close()


@pytest.mark.corpus
def test_corpus_q7_gyro_and_encoders_read_n_a(derived_corpus_lake: LakePaths) -> None:
    queries = HistoryQueries(derived_corpus_lake)
    try:
        rows = {
            r["slot_id"]: r["health"]
            for r in queries.devices("isolated_gaps", Filters(event="2026gacmp"))
        }
    finally:
        queries.close()
    assert rows["gyro"] == "N/A"
    assert all(rows[s] == "N/A" for s in rows if s.startswith("encoder-"))
    assert all(h in ("OK", "WARN", "HOT", "N/A") for h in rows.values())
    assert "NO-DATA" not in rows.values()
