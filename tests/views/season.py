"""A synthetic season lake for History queries: gold match features and unit usage, plus
the meta snapshots they join to.

60 matches (30 at 2026gadal, 28 quals + 2 eliminations at 2026gacmp) x 3 phases x the 23
slots of `2026-comp`, plus 5 practice sessions on `2026-practice`. Scenarios:
- swap: intake-roller holds ctre:ROLLER-A for matches 0-29, then ctre:ROLLER-B
- gap: hood holds ctre:HOOD-G except matches 10-20, when ctre:HOOD-T is installed
- move: ctre:MOVER sits in the practice robot's drive-fl, then the comp robot's drive-fr
- legacy: steer-fl is a legacy unit for matches 0-4, then ctre:STEER-FL (inventory)
- temperatures: flywheel-right reaches 66 C at match 42 (WARN), indexer-main 76 C at 50 (HOT)
- alignment: match 7 is low confidence
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.robot_config import RobotConfig, load_robot

HOSTILE_ROLE = "<img src=x onerror=alert(1)>"
MATCHES = 60
PRACTICE = 5
START = datetime(2026, 3, 6, 9, 0, tzinfo=UTC)


@dataclass(frozen=True)
class SeasonLake:
    lake: LakePaths
    robots: list[RobotConfig]
    match_keys: list[str]


def comp_robot() -> RobotConfig:
    return load_robot(config.config_root() / "robots" / "2026-comp.toml")


def practice_robot() -> RobotConfig:
    return comp_robot().model_copy(update={"robot": "2026-practice", "project": "Practice-2026"})


def match_key(i: int) -> str:
    if i < 30:
        return f"2026gadal_qm{i + 1}"
    if i < 58:
        return f"2026gacmp_qm{i - 29}"
    return f"2026gacmp_e{i - 57}"


def unit_for(slot_id: str, i: int) -> str:
    if slot_id == "intake-roller":
        return "ctre:ROLLER-A" if i < 30 else "ctre:ROLLER-B"
    if slot_id == "hood":
        return "ctre:HOOD-T" if 10 <= i <= 20 else "ctre:HOOD-G"
    if slot_id == "drive-fr":
        return "ctre:MOVER"
    if slot_id == "steer-fl" and i < 5:
        return "legacy:2026-comp:steer-fl:0"
    return f"ctre:{slot_id.upper()}"


def _temp(slot_index: int, slot_id: str, i: int) -> float:
    if slot_id == "flywheel-right" and i == 42:
        return 66.0
    if slot_id == "indexer-main" and i == 50:
        return 76.0
    return 30.0 + (i % 7) + slot_index * 0.5


def _write(path: Path, rows: list[dict[str, Any]], schema: dict[str, Any] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=schema, infer_schema_length=None).write_parquet(path)


def build_season(root: Path, hostile: bool = False) -> SeasonLake:
    lake = LakePaths(root)
    comp, practice = comp_robot(), practice_robot()
    sessions, logs, observations = [], [], []
    keys = [match_key(i) for i in range(MATCHES)]
    gold = lake.root / "gold"
    for p in range(PRACTICE):
        sid, start = f"p{p:03d}", START - timedelta(days=7) + timedelta(hours=p)
        sessions.append(_session(sid, "2026-practice", None))
        logs.append(_log(sid, start))
        observations.append(_observation(sid, "drive-fl", "ctre:MOVER"))
        _write(
            gold / "unit_usage" / "season=2026" / f"session_id={sid}" / "part-0.parquet",
            [_usage(sid, "2026-practice", None, practice, "drive-fl", "ctre:MOVER", start, 0)],
        )
    for i, key in enumerate(keys):
        sid, start = f"s{i:03d}", START + timedelta(hours=i * 2 if i < 30 else 24 * 30 + i)
        sessions.append(_session(sid, "2026-comp", key))
        logs.append(_log(sid, start))
        features, usage = [], []
        for slot_index, slot in enumerate(comp.slots):
            unit = unit_for(slot.id, i)
            observations.append(_observation(sid, slot.id, unit))
            role = HOSTILE_ROLE if hostile and slot.id == "hood" else slot.role
            temp = _temp(slot_index, slot.id, i)
            for phase in ("auto", "teleop", "match"):
                scale = {"auto": 0.1, "teleop": 0.9, "match": 1.0}[phase]
                features.append(
                    {
                        "match_key": key, "session_id": sid, "robot": "2026-comp",
                        "phase": phase, "slot_id": slot.id, "unit_id": unit,
                        "subsystem": slot.subsystem, "role": role,
                        "alignment": "low" if i == 7 else "high", "source_logs": f"w{sid}",
                        "supply_current_p95": 20.0 + slot_index + i % 5,
                        "supply_voltage_min": 11.0 - (i % 9) * 0.3 - slot_index * 0.01,
                        "temp_max_c": temp - (0 if phase != "auto" else 5),
                        "supply_energy_wh": scale * (1.0 + slot_index * 0.1),
                        "stall_s": scale * (2.0 if slot.subsystem == "intake" else 0.0),
                        "flags": "",
                    }
                )  # fmt: skip
            usage.append(_usage(sid, "2026-comp", key, comp, slot.id, unit, start, slot_index))
        _write(
            gold / "match_features" / "season=2026" / f"session_id={sid}" / "part-0.parquet",
            features,
        )
        _write(gold / "unit_usage" / "season=2026" / f"session_id={sid}" / "part-0.parquet", usage)
    lake.meta.mkdir(parents=True, exist_ok=True)
    _write(lake.meta / "sessions.parquet", sessions)
    _write(lake.meta / "logs.parquet", logs)
    _write(lake.meta / "slot_observations.parquet", observations)
    return SeasonLake(lake, [comp, practice], keys)


def _session(sid: str, robot: str, key: str | None) -> dict[str, Any]:
    return {
        "session_id": sid, "wpilog_id": f"w{sid}", "robot": robot, "season": "2026",
        "match_key": key, "match_source": "fms" if key else "none",
        "kind": "match" if key else "non-match", "warnings": None,
    }  # fmt: skip


def _log(sid: str, start: datetime) -> dict[str, Any]:
    return {
        "log_id": f"w{sid}",
        "kind": "wpilog",
        "season": "2026",
        "filename": f"FRC_{start:%Y%m%d_%H%M%S}.wpilog",
        "utc_start": start.isoformat(),
    }


def _observation(sid: str, slot_id: str, unit: str) -> dict[str, Any]:
    return {"session_id": sid, "slot_id": slot_id, "unit_id": unit,
            "source": "legacy" if unit.startswith("legacy:") else "inventory",
            "from_ts_us": None, "to_ts_us": None}  # fmt: skip


def _usage(
    sid: str, robot: str, key: str | None, config_: RobotConfig, slot_id: str, unit: str,
    start: datetime, slot_index: int,
) -> dict[str, Any]:  # fmt: skip
    slot = next(s for s in config_.slots if s.id == slot_id)
    return {
        "session_id": sid, "season": "2026", "robot": robot, "match_key": key,
        "alignment": "high", "session_start": start.isoformat(), "slot_id": slot_id,
        "unit_id": unit, "subsystem": slot.subsystem, "role": slot.role, "powered_s": 360.0,
        "enabled_s": 150.0, "supply_energy_wh": 1.0 + slot_index * 0.1,
        "motor_energy_wh": 0.5, "stall_s": 0.0, "thermal_cycles": 1,
        "temp_max_c": 40.0, "flags": "",
    }  # fmt: skip
