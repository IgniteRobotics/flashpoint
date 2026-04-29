from __future__ import annotations

import numpy as np
import pandas as pd

from .hoot_loader import MOTOR_COL_PATTERN, get_motor_ids
from .models import Match, MotorData

SECONDS_PER_HOUR: float = 3600.0


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Take absolute values of all motor voltage and current columns."""
    result = df.copy()
    for col in result.columns:
        if MOTOR_COL_PATTERN.match(col):
            result[col] = result[col].abs()
    return result


def _cumulative_energy(power: np.ndarray, timestamps: np.ndarray) -> np.ndarray:
    dt = np.empty_like(timestamps)
    dt[0] = 0.0
    dt[1:] = np.diff(timestamps)
    return np.cumsum(power * dt) / SECONDS_PER_HOUR


def _col(motor_id: str, metric: str) -> str:
    num = motor_id.split("-")[1]
    return f"Phoenix6/TalonFX-{num}/{metric}"


def compute_motor_data(df: pd.DataFrame) -> dict[str, MotorData]:
    """Build a MotorData instance for each motor found in the DataFrame."""
    n = len(df)
    zeros = np.zeros(n)
    timestamps = df["Timestamp"].to_numpy()
    motors: dict[str, MotorData] = {}

    for motor_id in get_motor_ids(df):
        mv_col = _col(motor_id, "MotorVoltage")
        sc_col = _col(motor_id, "StatorCurrent")
        sv_col = _col(motor_id, "SupplyVoltage")
        su_col = _col(motor_id, "SupplyCurrent")
        vel_col = _col(motor_id, "Velocity")
        temp_col = _col(motor_id, "DeviceTemp")

        mv = df[mv_col].ffill().fillna(0.0).to_numpy() if mv_col in df.columns else zeros.copy()
        sc = df[sc_col].ffill().fillna(0.0).to_numpy() if sc_col in df.columns else zeros.copy()
        mp = mv * sc

        has_supply = sv_col in df.columns and su_col in df.columns
        has_velocity = vel_col in df.columns
        has_temp = temp_col in df.columns

        sv = df[sv_col].ffill().fillna(0.0).to_numpy() if has_supply else None
        su = df[su_col].ffill().fillna(0.0).to_numpy() if has_supply else None
        sp = sv * su if has_supply else None
        vel = df[vel_col].ffill().fillna(0.0).to_numpy() if has_velocity else None
        temp = df[temp_col].ffill().fillna(0.0).to_numpy() if has_temp else None

        motors[motor_id] = MotorData(
            motor_voltage=mv,
            stator_current=sc,
            motor_power=mp,
            motor_energy=_cumulative_energy(mp, timestamps),
            supply_voltage=sv,
            supply_current=su,
            supply_power=sp,
            supply_energy=_cumulative_energy(sp, timestamps) if has_supply else None,
            rotor_velocity=vel,
            device_temp=temp,
        )

    return motors


def compute_totals(motors: dict[str, MotorData], timestamps: np.ndarray) -> MotorData:
    """Sum per-timestep power across all motors to produce robot-wide MotorData."""
    motor_list = list(motors.values())
    if not motor_list:
        raise ValueError("No motors to aggregate")

    total_mp: np.ndarray = sum(m.motor_power for m in motor_list)  # type: ignore[assignment]
    has_supply = all(m.supply_power is not None for m in motor_list)

    if has_supply:
        total_sp: np.ndarray = sum(m.supply_power for m in motor_list)  # type: ignore[assignment]
        total_su: np.ndarray = sum(m.supply_current for m in motor_list)  # type: ignore[assignment]
        n = len(total_mp)
        return MotorData(
            motor_voltage=np.zeros(n),
            stator_current=np.zeros(n),
            motor_power=total_mp,
            motor_energy=_cumulative_energy(total_mp, timestamps),
            supply_voltage=np.zeros(n),
            supply_current=total_su,
            supply_power=total_sp,
            supply_energy=_cumulative_energy(total_sp, timestamps),
        )

    n = len(total_mp)
    return MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=total_mp,
        motor_energy=_cumulative_energy(total_mp, timestamps),
    )


def build_match(match_id: str, df: pd.DataFrame) -> Match:
    """Construct a Match from a normalized, trimmed DataFrame."""
    timestamps = df["Timestamp"].to_numpy()
    motors = compute_motor_data(df)
    totals = compute_totals(motors, timestamps)
    return Match(match_id=match_id, timestamps=timestamps, motors=motors, totals=totals)
