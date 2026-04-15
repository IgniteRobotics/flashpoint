from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from utils.analyzer import (
    DT,
    SECONDS_PER_HOUR,
    build_match,
    compute_motor_data,
    compute_totals,
    normalize,
)


def _make_df(with_supply: bool = False) -> pd.DataFrame:
    data: dict = {
        "Timestamp": [0.0, 0.02, 0.04],
        "Phoenix6/TalonFX-1/MotorVoltage": [-5.0, 6.0, -3.0],
        "Phoenix6/TalonFX-1/StatorCurrent": [-2.0, 3.0, -1.0],
        "Phoenix6/TalonFX-2/MotorVoltage": [4.0, -5.0, 2.0],
        "Phoenix6/TalonFX-2/StatorCurrent": [1.5, -2.0, 0.5],
    }
    if with_supply:
        data["Phoenix6/TalonFX-1/SupplyVoltage"] = [12.0, 12.0, 12.0]
        data["Phoenix6/TalonFX-1/SupplyCurrent"] = [3.0, 4.0, 2.0]
        data["Phoenix6/TalonFX-2/SupplyVoltage"] = [12.0, 12.0, 12.0]
        data["Phoenix6/TalonFX-2/SupplyCurrent"] = [2.0, 3.0, 1.0]
    return pd.DataFrame(data)


def test_normalize_makes_all_motor_cols_non_negative() -> None:
    df = normalize(_make_df())
    for col in df.columns:
        if col != "Timestamp":
            assert (df[col] >= 0).all(), f"{col} has negative values"


def test_normalize_leaves_timestamp_unchanged() -> None:
    df = _make_df()
    result = normalize(df)
    pd.testing.assert_series_equal(df["Timestamp"], result["Timestamp"])


def test_compute_motor_data_creates_entry_per_motor() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    assert "TalonFX-1" in motors
    assert "TalonFX-2" in motors


def test_motor_power_equals_voltage_times_current() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    data = motors["TalonFX-1"]
    expected = data.motor_voltage * data.stator_current
    np.testing.assert_array_almost_equal(data.motor_power, expected)


def test_motor_energy_is_monotonically_non_decreasing() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    energy = motors["TalonFX-1"].motor_energy
    assert np.all(np.diff(energy) >= 0)


def test_motor_energy_units_are_watt_hours() -> None:
    # 1000 W for one 20ms sample → 1000 * 0.02 / 3600 Wh
    df = pd.DataFrame({
        "Timestamp": [0.0, 0.02],
        "Phoenix6/TalonFX-1/MotorVoltage": [100.0, 100.0],
        "Phoenix6/TalonFX-1/StatorCurrent": [10.0, 10.0],
    })
    motors = compute_motor_data(df)
    expected_first = 1000.0 * DT / SECONDS_PER_HOUR
    assert motors["TalonFX-1"].motor_energy[0] == pytest.approx(expected_first)


def test_supply_fields_none_when_columns_absent() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    assert motors["TalonFX-1"].supply_power is None
    assert motors["TalonFX-1"].supply_energy is None


def test_supply_power_computed_when_columns_present() -> None:
    motors = compute_motor_data(normalize(_make_df(with_supply=True)))
    data = motors["TalonFX-1"]
    assert data.supply_power is not None
    np.testing.assert_array_almost_equal(
        data.supply_power,
        data.supply_voltage * data.supply_current,  # type: ignore[operator]
    )


def test_compute_totals_sums_motor_power() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    totals = compute_totals(motors)
    expected = motors["TalonFX-1"].motor_power + motors["TalonFX-2"].motor_power
    np.testing.assert_array_almost_equal(totals.motor_power, expected)


def test_compute_totals_supply_none_when_any_motor_missing_supply() -> None:
    motors = compute_motor_data(normalize(_make_df()))  # no supply cols
    totals = compute_totals(motors)
    assert totals.supply_power is None


def test_compute_totals_supply_computed_when_all_motors_have_supply() -> None:
    motors = compute_motor_data(normalize(_make_df(with_supply=True)))
    totals = compute_totals(motors)
    assert totals.supply_power is not None


def test_build_match_sets_match_id() -> None:
    df = normalize(_make_df())
    match = build_match("GACMP_E5", df)
    assert match.match_id == "GACMP_E5"


def test_build_match_timestamps_start_at_zero() -> None:
    df = normalize(_make_df())
    match = build_match("GACMP_E5", df)
    assert match.timestamps[0] == pytest.approx(0.0)
