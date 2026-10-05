from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pytest

from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.framing import Framing, frame
from flashpoint.semantics.physics import PhysicsConfig
from flashpoint.semantics.robot_config import RobotConfig
from flashpoint.semantics.silver import silver_dir
from flashpoint.semantics.usage import session_usage, thermal_cycles

S = 1_000_000
ROBOT = RobotConfig.model_validate(
    {
        "robot": "2026-comp",
        "season": 2026,
        "project": "Robot-2026",
        "slot": [
            {
                "id": "intake-roller",
                "bus": "rio",
                "model": "TalonFX",
                "can_id": 2,
                "subsystem": "intake",
                "role": "roller leader",
            },
            {
                "id": "drive-fl",
                "bus": "canivore",
                "model": "TalonFX",
                "can_id": 11,
                "subsystem": "drivetrain",
                "role": "front-left drive",
            },
        ],
    }
)


# --- thermal cycles -----------------------------------------------------------------------


def test_one_cycle_with_cool_down() -> None:
    assert thermal_cycles([22, 26, 31, 35, 40, 38, 34, 30]) == 1


def test_powered_off_hot_counts() -> None:
    assert thermal_cycles([22, 25, 30, 33, 36]) == 1


def test_small_wobble_is_no_cycle() -> None:
    assert thermal_cycles([30, 33, 37, 34, 31, 36, 30, 37]) == 0


def test_no_temperature_is_absent() -> None:
    assert thermal_cycles([]) is None


def test_two_cycles_need_a_new_rise_from_the_low() -> None:
    # 22 -> 40 (armed) -> 36 counts; the low since then is 30, so 38 does not re-arm; 41 does.
    assert thermal_cycles([22, 40, 36, 30, 38, 34, 41, 37]) == 2


def test_fall_short_of_threshold_then_session_end_counts_once() -> None:
    assert thermal_cycles([22, 40, 38.5]) == 1


def test_configurable_thresholds() -> None:
    temps = [22, 29, 26]
    assert thermal_cycles(temps) == 0
    assert thermal_cycles(temps, rise_c=5, fall_c=2) == 1
    assert thermal_cycles([22, 40, 38, 36], rise_c=10, fall_c=5) == 1  # counted at session end


# --- session usage ------------------------------------------------------------------------


def _series(
    slot: str, unit: str, metric: str, start_s: float, end_s: float, hz: float, value: float
) -> list[dict[str, Any]]:
    n = round((end_s - start_s) * hz)
    return [
        {"slot_id": slot, "unit_id": unit, "metric": metric, "t_us": int((start_s + i / hz) * S),
         "value": value}
        for i in range(n)
    ]  # fmt: skip


def _write_silver(lake: LakePaths, session_id: str, rows: list[dict[str, Any]]) -> None:
    path = silver_dir(lake) / "season=2026" / f"session_id={session_id}"
    path.mkdir(parents=True)
    frame_ = pl.DataFrame(rows).with_columns(
        pl.lit(session_id).alias("session_id"),
        pl.lit(None, pl.Int64).alias("match_time_us"),
        pl.lit(None, pl.String).alias("phase"),
    )
    frame_.write_parquet(path / "part-0.parquet")


def _session(session_id: str, match_key: str | None) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "season": "2026",
        "robot": "2026-comp",
        "match_key": match_key,
    }


def _usage(
    tmp_path: Path,
    rows: list[dict[str, Any]],
    framing: Framing,
    match_key: str | None = None,
    config: PhysicsConfig | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    lake = LakePaths(tmp_path / "lake")
    _write_silver(lake, "s1", rows)
    out = session_usage(
        duckdb.connect(),
        lake,
        _session("s1", match_key),
        framing,
        ROBOT,
        "high",
        "2026-04-09T21:51:13+00:00",
        **({"config": config} if config else {}),
    )
    return {(r["slot_id"], r["unit_id"]): r for r in out}


def _motor(slot: str, unit: str, start_s: float, end_s: float, hz: float = 50) -> list[Any]:
    return (
        _series(slot, unit, "supply_voltage", start_s, end_s, hz, 12.0)
        + _series(slot, unit, "supply_current", start_s, end_s, hz, 10.0)
        + _series(slot, unit, "stator_current", start_s, end_s, hz, 20.0)
        + _series(slot, unit, "motor_voltage", start_s, end_s, hz, 6.0)
        + _series(slot, unit, "rotor_velocity_rps", start_s, end_s, hz, 50.0)
        + _series(slot, unit, "stall_current", start_s, start_s + 1, 1, 279.0)
    )


def test_practice_session_counted(tmp_path: Path) -> None:
    rows = _motor("intake-roller", "legacy:2026-comp:intake-roller:0", 0, 600, hz=4)
    framing = frame([(0, "Disabled"), (60 * S, "Teleop"), (360 * S, "Disabled")])
    usage = _usage(tmp_path, rows, framing)
    row = usage[("intake-roller", "legacy:2026-comp:intake-roller:0")]
    assert row["match_key"] is None
    assert row["powered_s"] == pytest.approx(600, abs=0.5)
    assert row["enabled_s"] == pytest.approx(300, abs=0.5)
    assert row["supply_energy_wh"] == pytest.approx(12 * 10 * 600 / 3600, rel=1e-2)
    assert row["motor_energy_wh"] == pytest.approx(6 * 20 * 600 / 3600, rel=1e-2)
    assert row["stall_s"] == 0.0
    assert row["thermal_cycles"] is None and row["temp_max_c"] is None
    assert "no-temperature" in row["flags"]
    assert row["session_start"] == "2026-04-09T21:51:13+00:00"
    assert (row["robot"], row["alignment"], row["subsystem"], row["role"]) == (
        "2026-comp", "high", "intake", "roller leader"
    )  # fmt: skip


def test_logging_gap_does_not_count_as_powered(tmp_path: Path) -> None:
    unit = "ctre:A"
    rows = _motor("drive-fl", unit, 0, 100) + _motor("drive-fl", unit, 160, 200)
    usage = _usage(tmp_path, rows, frame([]))
    # 100 s + 40 s of samples; the 60 s gap contributes only the 1 s hold cap
    assert usage[("drive-fl", unit)]["powered_s"] == pytest.approx(141, abs=0.1)
    assert usage[("drive-fl", unit)]["enabled_s"] == 0.0


def test_powered_falls_back_to_any_metric(tmp_path: Path) -> None:
    rows = _series("drive-fl", "ctre:A", "temp_c", 0, 100, 4, 30.0)
    usage = _usage(tmp_path, rows, frame([]))
    assert usage[("drive-fl", "ctre:A")]["powered_s"] == pytest.approx(100, abs=0.5)


def test_swap_splits_usage(tmp_path: Path) -> None:
    rows = _motor("drive-fl", "ctre:A", 0, 120) + _motor("drive-fl", "ctre:B", 120, 300)
    usage = _usage(tmp_path, rows, frame([(0, "Teleop")]))
    a, b = usage[("drive-fl", "ctre:A")], usage[("drive-fl", "ctre:B")]
    assert a["powered_s"] == pytest.approx(120, abs=0.05)
    assert b["powered_s"] == pytest.approx(180, abs=0.05)
    assert a["powered_s"] + b["powered_s"] == pytest.approx(300 - 1 / 50, abs=1e-6)
    assert a["enabled_s"] == pytest.approx(a["powered_s"])
    assert a["supply_energy_wh"] < b["supply_energy_wh"]


def test_stall_and_thermal_cycles(tmp_path: Path) -> None:
    unit = "ctre:A"
    rows = (
        _series("intake-roller", unit, "stator_current", 0, 10, 4, 80.0)  # limited stall
        + _series("intake-roller", unit, "rotor_velocity_rps", 0, 10, 4, 0.0)
        + _series("intake-roller", unit, "stall_current", 0, 1, 1, 279.0)
        + [
            {"slot_id": "intake-roller", "unit_id": unit, "metric": "temp_c", "t_us": t * S,
             "value": v}
            for t, v in [(0, 22.0), (3, 35.0), (6, 41.0), (9, 37.0)]
        ]
    )  # fmt: skip
    row = _usage(tmp_path, rows, frame([]))[("intake-roller", unit)]
    assert row["stall_s"] == pytest.approx(9.75, abs=0.01)
    assert row["thermal_cycles"] == 1
    assert row["temp_max_c"] == 41.0
    assert "no-temperature" not in row["flags"]


def test_match_session_carries_match_key(tmp_path: Path) -> None:
    rows = _motor("drive-fl", "ctre:A", 0, 10)
    usage = _usage(tmp_path, rows, frame([(0, "Autonomous")]), match_key="2026gacmp_qm7")
    assert usage[("drive-fl", "ctre:A")]["match_key"] == "2026gacmp_qm7"


def test_no_silver_no_rows(tmp_path: Path) -> None:
    lake = LakePaths(tmp_path / "lake")
    rows = session_usage(
        duckdb.connect(), lake, _session("none", None), frame([]), ROBOT, "high", None
    )
    assert rows == []


def test_unknown_unit_samples_are_skipped(tmp_path: Path) -> None:
    rows = _motor("drive-fl", "ctre:A", 0, 10)
    for row in rows[:5]:
        row["unit_id"] = None
    usage = _usage(tmp_path, rows, frame([]))
    assert set(usage) == {("drive-fl", "ctre:A")}
