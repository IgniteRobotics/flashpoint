"""A synthetic gold lake for detection tests: per-unit metric series written as the whole-match
rows of `match_features`, `can_health`, and `unit_usage`, one session per match.

Every slot gets steady default values; tests override a (slot, metric) series, swap a slot's
unit from a match on, drop a slot's rows from a match, or mark a match low alignment.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics import can_health, usage
from flashpoint.semantics.robot_config import RobotConfig, load_robot

START = datetime(2026, 3, 6, 9, 0, tzinfo=UTC)
FEATURE_METRICS = (
    "temp_max_c",
    "temp_mean_c",
    "temp_rise_c_per_min",
    "stator_current_p95",
    "residual_p95_a",
    "stall_s",
    "supply_resistance_mohm",
)
CAN_METRICS = ("reboots", "reboots_enabled", "isolated_gaps", "missed_per_min", "longest_gap_ms")
DEFAULTS: dict[str, float] = {
    "temp_max_c": 40.0,
    "temp_mean_c": 30.0,
    "temp_rise_c_per_min": 1.0,
    "stator_current_p95": 40.0,
    "residual_p95_a": 10.0,
    "stall_s": 0.0,
    "supply_resistance_mohm": 20.0,
    "reboots": 0,
    "reboots_enabled": 0,
    "isolated_gaps": 2,
    "missed_per_min": 1.0,
    "longest_gap_ms": 25.0,
}
WOBBLE = (0.0, 1.0, -1.0, 0.5, -0.5, 0.0, 1.0, -1.0, 0.5, -0.5, 0.25)  # "± 1" around a level


def steady(level: float, n: int, spread: float = 1.0, start: int = 0) -> list[float]:
    """`level ± spread`, deterministic."""
    return [level + spread * WOBBLE[(start + i) % len(WOBBLE)] for i in range(n)]


def comp_robot() -> RobotConfig:
    return load_robot(config.config_root() / "robots" / "2026-comp.toml")


@dataclass
class GoldLake:
    root: Path
    matches: int
    robot: RobotConfig = field(default_factory=comp_robot)
    slots: list[str] | None = None  # default: every slot of the robot
    event: str = "2026syn"
    series: dict[tuple[str, str], list[float | None]] = field(default_factory=dict)
    swaps: dict[str, list[tuple[int, str]]] = field(default_factory=dict)
    absent: set[tuple[str, int]] = field(default_factory=set)
    no_can: set[int] = field(default_factory=set)
    low_alignment: set[int] = field(default_factory=set)
    keys: list[str] | None = None

    @property
    def lake(self) -> LakePaths:
        return LakePaths(self.root)

    def match_key(self, i: int) -> str:
        return self.keys[i] if self.keys else f"{self.event}_qm{i + 1}"

    def session_id(self, i: int) -> str:
        return f"{self.event}-s{i:03d}"

    def slot_ids(self) -> list[str]:
        return self.slots if self.slots is not None else [s.id for s in self.robot.slots]

    def set(self, slot: str, metric: str, values: Sequence[float | None]) -> "GoldLake":
        """Values for matches 0..len(values)-1; later matches keep the default."""
        self.series[(slot, metric)] = list(values)
        return self

    def swap(self, slot: str, at: int, unit: str) -> "GoldLake":
        self.swaps.setdefault(slot, []).append((at, unit))
        return self

    def unit(self, slot: str, i: int) -> str:
        unit = f"ctre:{slot.upper()}"
        for at, new in sorted(self.swaps.get(slot, [])):
            if i >= at:
                unit = new
        return unit

    def value(self, slot: str, metric: str, i: int) -> float | None:
        values = self.series.get((slot, metric))
        if values is not None and i < len(values):
            return values[i]
        return DEFAULTS[metric]

    def write(self, upto: int | None = None) -> LakePaths:
        """Write matches 0..upto-1 (default all)."""
        gold = self.root / "gold"
        slots = {s.id: s for s in self.robot.slots}
        for i in range(self.matches if upto is None else upto):
            sid, key = self.session_id(i), self.match_key(i)
            start = START + timedelta(hours=2 * i)
            base = {"match_key": key, "session_id": sid, "season": str(self.robot.season),
                    "robot": self.robot.robot}  # fmt: skip
            features, cans, usages = [], [], []
            for slot_id in self.slot_ids():
                if (slot_id, i) in self.absent:
                    continue
                slot, unit = slots[slot_id], self.unit(slot_id, i)
                common = {**base, "slot_id": slot_id, "unit_id": unit,
                          "subsystem": slot.subsystem, "role": slot.role,
                          "alignment": "low" if i in self.low_alignment else "high",
                          "source_logs": f"w-{sid}"}  # fmt: skip
                for phase in ("auto", "match"):
                    scale = 1.0 if phase == "match" else 0.2
                    features.append(
                        {**common, "phase": phase, "flags": "",
                         **{m: _scaled(self.value(slot_id, m, i), scale)
                            for m in FEATURE_METRICS}}
                    )  # fmt: skip
                    if i not in self.no_can:
                        cans.append(
                            {**common, "phase": phase, "bus": slot.bus,
                             "reset_count_logged": True,
                             **{m: _can_value(m, self.value(slot_id, m, i))
                                for m in CAN_METRICS}}
                        )  # fmt: skip
                usages.append(
                    {"session_id": sid, "season": base["season"], "robot": base["robot"],
                     "match_key": key, "alignment": common["alignment"],
                     "session_start": start.isoformat(), "slot_id": slot_id, "unit_id": unit,
                     "subsystem": slot.subsystem, "role": slot.role, "powered_s": 300.0,
                     "enabled_s": 150.0, "supply_energy_wh": 1.0, "motor_energy_wh": 0.5,
                     "stall_s": 0.0, "thermal_cycles": 0, "temp_max_c": 40.0, "flags": ""}
                )  # fmt: skip
            part = Path(f"season={base['season']}") / f"session_id={sid}" / "part-0.parquet"
            _write(gold / "match_features" / part, features, _FEATURE_SCHEMA)
            if cans:
                _write(gold / can_health.TABLE / part, cans, _can_schema())
            _write(gold / usage.TABLE / part, usages, usage.SCHEMA)
        return self.lake


_FEATURE_SCHEMA: dict[str, Any] = {
    "match_key": pl.String(), "session_id": pl.String(), "season": pl.String(),
    "robot": pl.String(), "phase": pl.String(), "slot_id": pl.String(), "unit_id": pl.String(),
    "subsystem": pl.String(), "role": pl.String(), "alignment": pl.String(),
    "source_logs": pl.String(), "flags": pl.String(),
    **{m: pl.Float64() for m in FEATURE_METRICS},
}  # fmt: skip


def _can_schema() -> dict[str, Any]:
    return dict(can_health.SCHEMA)


def _can_value(metric: str, value: float | None) -> float | int | None:
    if value is None or can_health.SCHEMA[metric] != pl.Int64():
        return value
    return int(value)


def _scaled(value: float | None, scale: float) -> float | None:
    return None if value is None else value * scale


def _write(path: Path, rows: list[dict[str, Any]], schema: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    frame.write_parquet(path)
