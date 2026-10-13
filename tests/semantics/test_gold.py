from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pytest

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics import gold
from flashpoint.semantics.framing import Framing, Phase
from flashpoint.semantics.robot_config import load_robot
from flashpoint.semantics.silver import silver_dir

S = 1_000_000
ROBOT = load_robot(config.config_root() / "robots" / "2026-comp.toml")
FRAMING = Framing(
    [Phase("pre", None, 0), Phase("auto", 0, 15 * S), Phase("gap", 15 * S, 18 * S),
     Phase("teleop", 18 * S, 38 * S), Phase("post", 38 * S, None)],
    0,
)  # fmt: skip
SESSION = {"session_id": "s1", "season": "2026", "match_key": "2026gacmp_qm7", "robot": "2026-comp"}


def _rows(slot: str, metric: str, values: list[tuple[float, float]]) -> list[dict[str, Any]]:
    return [
        {"slot_id": slot, "unit_id": f"ctre:{slot.upper()}", "metric": metric,
         "t_us": round(t * S), "value": v, "phase": FRAMING.phase_at(round(t * S))}
        for t, v in values
    ]  # fmt: skip


def _lake(tmp_path: Path, proxy: bool = True) -> LakePaths:
    lake = LakePaths(tmp_path / "lake")
    times = [i / 50 for i in range(38 * 50)]
    amps = [10.0 + 50.0 * ((i * 37) % 100) / 100 for i in range(len(times))]
    rows: list[dict[str, Any]] = []
    for slot, ohms in (("drive-fl", 0.018), ("drive-fr", 0.030)):
        rows += _rows(slot, "stator_current", list(zip(times, amps, strict=True)))
        rows += _rows(slot, "supply_current", list(zip(times, amps, strict=True)))
        rows += _rows(
            slot, "supply_voltage", [(t, 12.5 - ohms * a) for t, a in zip(times, amps, strict=True)]
        )
    if proxy:
        rows += _rows("gyro", "supply_voltage", [(i / 100, 12.5) for i in range(38 * 100)])
    path = silver_dir(lake) / "season=2026" / "session_id=s1"
    path.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(path / "part-0.parquet")
    return lake


def _features(lake: LakePaths, con: Any = None) -> dict[tuple[str, str], dict[str, Any]]:
    rows = gold.session_features(con or duckdb.connect(), lake, SESSION, FRAMING, ROBOT, "high", "")
    return {(r["slot_id"], r["phase"]): r for r in rows}


def test_rows_carry_supply_path_resistance(tmp_path: Path) -> None:
    rows = _features(_lake(tmp_path))
    assert {k for k in rows} == {
        (slot, phase) for slot in ("drive-fl", "drive-fr") for phase in ("auto", "teleop", "match")
    }
    for phase in ("auto", "teleop", "match"):
        assert rows["drive-fl", phase]["supply_resistance_mohm"] == pytest.approx(18.0, abs=1.0)
        assert rows["drive-fr", phase]["supply_resistance_mohm"] == pytest.approx(30.0, abs=1.0)
        assert rows["drive-fl", phase]["supply_resistance_flag"] is None


def test_no_declared_proxy_logged(tmp_path: Path) -> None:
    rows = _features(_lake(tmp_path, proxy=False))
    assert {r["supply_resistance_flag"] for r in rows.values()} == {"no-proxy"}
    assert {r["supply_resistance_mohm"] for r in rows.values()} == {None}


def test_proxy_series_read_once_per_session(tmp_path: Path) -> None:
    lake = _lake(tmp_path)
    calls: list[Any] = []

    class Spy:
        def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
            self.con = con

        def sql(self, query: str, params: Any = None) -> Any:
            calls.append(params)
            return self.con.sql(query, params=params)

    _features(lake, Spy(duckdb.connect()))
    assert sum(1 for params in calls if params and "gyro" in params) == 1  # not once per motor
