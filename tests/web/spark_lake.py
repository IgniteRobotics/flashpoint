"""A small History lake for the device table's shared sparkline scale (issue #39).

Five matches of 2026-comp (2026gacmp_qm1..5), match phase only. Isolated CAN gaps (a count):
- intake: ctre:LOW 3, 0, 1, 3 and ctre:HIGH 7, 20, 1, 42 (qm1-4); ctre:ONE 4 (qm5 only)
- shooter: ctre:GAPPY 10, -, -, 11, 12 (rows with no value in qm2 and qm3); ctre:PEAK up to 30
Supply current P95 runs from -2 to 5 A over every unit.
"""

from datetime import timedelta
from pathlib import Path
from typing import Any

from flashpoint.lake.paths import LakePaths
from tests.views.season import START, _log, _observation, _session, _write

ROBOT = "2026-comp"
MATCHES = 5
# unit: (subsystem, isolated gaps per match, supply current P95 per match); None in a list is
# a row with no value, and a short list means the unit has no row in the later matches.
UNITS: dict[str, tuple[str, list[float | None], list[float | None]]] = {
    "ctre:LOW": ("intake", [3, 0, 1, 3], [-2, 0, 1, 3]),
    "ctre:HIGH": ("intake", [7, 20, 1, 42], [5, 4, 3, 2]),
    "ctre:ONE": ("intake", [4], [1]),
    "ctre:GAPPY": ("shooter", [10, None, None, 11, 12], [0, 1, 2, 1, 0]),
    "ctre:PEAK": ("shooter", [5, 30, 8, 2, 9], [1, 1, 1, 1, 1]),
}
FIRST_MATCH = {"ctre:ONE": 4}  # ctre:ONE is installed only in the last match


def spark_key(i: int) -> str:
    return f"2026gacmp_qm{i + 1}"


def _row(sid: str, key: str, unit: str, subsystem: str) -> dict[str, Any]:
    slot = unit.split(":")[1].lower()
    return {"match_key": key, "session_id": sid, "robot": ROBOT, "phase": "match",
            "slot_id": slot, "unit_id": unit, "subsystem": subsystem, "role": slot,
            "alignment": "high", "source_logs": f"w{sid}"}  # fmt: skip


def _schema(rows: list[dict[str, Any]], metric: str) -> dict[str, Any]:
    """Text columns, plus the metric as a float column (it holds nulls and whole numbers)."""
    return {k: (float if k == metric else str) for k in rows[0]}


def build_spark_lake(root: Path) -> LakePaths:
    lake = LakePaths(root)
    gold = lake.root / "gold"
    sessions, logs, observations = [], [], []
    for i in range(MATCHES):
        sid, key = f"s{i:03d}", spark_key(i)
        sessions.append(_session(sid, ROBOT, key))
        logs.append(_log(sid, START + timedelta(hours=i)))
        features, can = [], []
        for unit, (subsystem, gaps, current) in UNITS.items():
            j = i - FIRST_MATCH.get(unit, 0)
            if not 0 <= j < len(gaps):
                continue
            row = _row(sid, key, unit, subsystem)
            observations.append(_observation(sid, row["slot_id"], unit))
            features.append({**row, "supply_current_p95": current[j]})
            can.append({**row, "isolated_gaps": gaps[j]})
        part = f"season=2026/session_id={sid}/part-0.parquet"
        _write(gold / "match_features" / part, features, _schema(features, "supply_current_p95"))
        _write(gold / "can_health" / part, can, _schema(can, "isolated_gaps"))
    lake.meta.mkdir(parents=True, exist_ok=True)
    _write(lake.meta / "sessions.parquet", sessions)
    _write(lake.meta / "logs.parquet", logs)
    _write(lake.meta / "slot_observations.parquet", observations)
    return lake
