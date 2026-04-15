from __future__ import annotations
from pathlib import Path

import numpy as np
from typing import Any
import pytest

from utils.models import Match, MotorData

MINIMAL_RIO_CSV = """\
Timestamp,Phoenix6/TalonFX-1/MotorVoltage,Phoenix6/TalonFX-2/MotorVoltage,Phoenix6/TalonFX-1/StatorCurrent,Phoenix6/TalonFX-2/StatorCurrent
0.0,0.0,0.0,0.0,0.0
0.02,0.0,0.0,0.0,0.0
0.04,5.0,3.0,-2.0,1.5
0.06,6.0,4.0,-3.0,2.0
0.08,0.0,0.0,0.0,0.0
"""

MINIMAL_CARNIVORE_UUID = "a1b2c3d4-e29b-41d4-a716-446655440000"

MINIMAL_CARNIVORE_CSV = """\
Timestamp,Phoenix6/TalonFX-11/MotorVoltage,Phoenix6/TalonFX-11/StatorCurrent
0.0,0.0,0.0
0.02,0.0,0.0
0.04,4.0,-1.0
0.06,5.0,-2.0
0.08,0.0,0.0
"""


@pytest.fixture
def rio_csv(tmp_path: Path) -> Path:
    f = tmp_path / "GACMP_Q1-rio.csv"
    f.write_text(MINIMAL_RIO_CSV)
    return f


@pytest.fixture
def carnivore_csv(tmp_path: Path) -> Path:
    f = tmp_path / f"GACMP_Q1_{MINIMAL_CARNIVORE_UUID}.csv"
    f.write_text(MINIMAL_CARNIVORE_CSV)
    return f


def _make_motor_data(n: int = 10, with_supply: bool = False) -> MotorData:
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
