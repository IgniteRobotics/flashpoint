"""Gold: one row per (match, phase, slot, unit) of motor features, read by trends and anomalies."""

import shutil
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.framing import Framing
from flashpoint.semantics.physics import motor_features
from flashpoint.semantics.robot_config import RobotConfig
from flashpoint.semantics.silver import silver_dir

PHASES = ("auto", "teleop")


def gold_dir(lake: LakePaths) -> Path:
    return lake.root / "gold" / "match_features"


def _periods(framing: Framing) -> dict[str, tuple[int, int]]:
    """auto, teleop, and match (auto start to teleop end; the gap's rows are excluded)."""
    found = {p.name: (p.start_us, p.end_us) for p in framing.phases if p.name in PHASES}
    periods = {
        name: (start, end)
        for name, (start, end) in found.items()
        if start is not None and end is not None
    }
    if periods:
        periods["match"] = (
            min(s for s, _ in periods.values()),
            max(e for _, e in periods.values()),
        )
    return periods


def session_features(
    con: duckdb.DuckDBPyConnection,
    lake: LakePaths,
    session: dict[str, Any],
    framing: Framing,
    robot: RobotConfig,
    alignment: str,
    source_logs: str,
) -> list[dict[str, Any]]:
    path = silver_dir(lake) / f"season={session['season']}" / f"session_id={session['session_id']}"
    if not path.is_dir() or framing.match_start_us is None:
        return []
    source = f"read_parquet('{path}/*.parquet')"
    motors = con.sql(
        f"SELECT DISTINCT slot_id, unit_id FROM {source}"  # noqa: S608 - internal path
        " WHERE metric = 'stator_current' AND phase IN ('auto', 'teleop')"
    ).fetchall()
    slots = {s.id: s for s in robot.slots}
    rows = []
    for slot_id, unit_id in sorted(motors):
        # One motor at a time keeps memory bounded by a motor's samples, not the session's.
        motor = con.sql(
            f"SELECT metric, t_us, value FROM {source}"  # noqa: S608
            " WHERE slot_id = ? AND unit_id = ? AND phase IN ('auto', 'teleop')",
            params=[slot_id, unit_id],
        ).pl()
        for period, (start, end) in _periods(framing).items():
            rows.append(
                {
                    "match_key": session["match_key"],
                    "session_id": session["session_id"],
                    "season": session["season"],
                    "robot": session["robot"],
                    "phase": period,
                    "slot_id": slot_id,
                    "unit_id": unit_id,
                    "subsystem": slots[slot_id].subsystem if slot_id in slots else None,
                    "role": slots[slot_id].role if slot_id in slots else None,
                    "alignment": alignment,
                    "source_logs": source_logs,
                    **motor_features(motor, start, end),
                }
            )
    return rows


def write_session(
    lake: LakePaths, season: str, session_id: str, rows: list[dict[str, Any]], run_id: str
) -> None:
    final = gold_dir(lake) / f"season={season}" / f"session_id={session_id}"
    for existing in gold_dir(lake).glob(f"season=*/session_id={session_id}"):
        shutil.rmtree(existing)
    if not rows:
        return
    stage = lake.root / "gold" / "_staging" / run_id / f"session_id={session_id}"
    stage.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, infer_schema_length=None).write_parquet(
        stage / "part-0.parquet", compression="zstd"
    )
    final.parent.mkdir(parents=True, exist_ok=True)
    stage.rename(final)
