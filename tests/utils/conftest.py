from __future__ import annotations
from pathlib import Path

import numpy as np
from typing import Any
import pytest

from utils.models import Match, MotorData


def _make_motor_data(n: int = 10, with_supply: bool = False, with_velocity: bool = False) -> MotorData:
    power = np.ones(n) * 100.0
    energy = np.cumsum(power * 0.02) / 3600
    kwargs: dict[str, Any] = dict(
        motor_voltage=np.ones(n) * 5.0,
        stator_current=np.ones(n) * 20.0,
        motor_power=power,
        motor_energy=energy,
    )
    if with_supply:
        kwargs.update(
            supply_voltage=np.ones(n) * 12.0,
            supply_current=np.ones(n) * 8.5,
            supply_power=np.ones(n) * 102.0,
            supply_energy=np.cumsum(np.ones(n) * 102.0 * 0.02) / 3600,
        )
    if with_velocity:
        kwargs["rotor_velocity"] = np.linspace(5.0, 15.0, n)
    return MotorData(**kwargs)


@pytest.fixture
def simple_match() -> Match:
    n = 10
    t = np.arange(n) * 0.02
    data = _make_motor_data(n)
    motors = {"TalonFX-1": data, "TalonFX-2": data}
    totals = MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=data.motor_power * 2,
        motor_energy=data.motor_energy * 2,
    )
    return Match(match_id="TEST_Q1", timestamps=t, motors=motors, totals=totals)


@pytest.fixture
def simple_match_with_supply() -> Match:
    n = 10
    t = np.arange(n) * 0.02
    data = _make_motor_data(n, with_supply=True)
    motors = {"TalonFX-1": data}
    totals = MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=data.motor_power,
        motor_energy=data.motor_energy,
        supply_voltage=np.zeros(n),
        supply_current=np.zeros(n),
        supply_power=data.supply_power,
        supply_energy=data.supply_energy,
    )
    return Match(match_id="TEST_Q1_SUPPLY", timestamps=t, motors=motors, totals=totals)


@pytest.fixture
def simple_match_with_velocity() -> Match:
    n = 150  # 3s of data at 50Hz — matches WATTS_PER_RPS_SMOOTH_S window
    t = np.arange(n) * 0.02
    data = _make_motor_data(n, with_supply=True, with_velocity=True)
    motors = {"TalonFX-1": data, "TalonFX-2": data}
    totals = MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=data.motor_power * 2,
        motor_energy=data.motor_energy * 2,
        supply_voltage=np.zeros(n),
        supply_current=np.zeros(n),
        supply_power=data.supply_power * 2,  # type: ignore[operator]
        supply_energy=data.supply_energy * 2,  # type: ignore[operator]
    )
    return Match(match_id="TEST_VEL", timestamps=t, motors=motors, totals=totals)
