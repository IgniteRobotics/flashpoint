from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class MotorData:
    """Per-motor telemetry arrays, normalized to absolute values."""

    # Motor power path: MotorVoltage × StatorCurrent (torque proxy)
    motor_voltage: np.ndarray
    stator_current: np.ndarray
    motor_power: np.ndarray
    motor_energy: np.ndarray  # cumulative Wh

    # Supply power path: SupplyVoltage × SupplyCurrent (actual battery draw)
    # None when source CSV does not contain these columns.
    supply_voltage: np.ndarray | None = None
    supply_current: np.ndarray | None = None
    supply_power: np.ndarray | None = None
    supply_energy: np.ndarray | None = None  # cumulative Wh


@dataclass
class Match:
    """A single FRC match worth of trimmed, normalized motor data."""

    match_id: str
    timestamps: np.ndarray        # seconds, re-zeroed to match start
    motors: dict[str, MotorData]  # keyed by "TalonFX-{id}"
    totals: MotorData              # robot-wide sum across all motors
