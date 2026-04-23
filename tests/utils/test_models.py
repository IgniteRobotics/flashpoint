import numpy as np
from utils.models import MotorData, Match


def test_motor_data_requires_core_fields():
    n = 5
    data = MotorData(
        motor_voltage=np.ones(n),
        stator_current=np.ones(n),
        motor_power=np.ones(n),
        motor_energy=np.ones(n),
    )
    assert data.supply_power is None
    assert data.supply_energy is None


def test_motor_data_accepts_supply_fields():
    n = 5
    data = MotorData(
        motor_voltage=np.ones(n),
        stator_current=np.ones(n),
        motor_power=np.ones(n),
        motor_energy=np.ones(n),
        supply_voltage=np.ones(n),
        supply_current=np.ones(n),
        supply_power=np.ones(n),
        supply_energy=np.ones(n),
    )
    assert data.supply_power is not None


def test_match_holds_motors_and_totals():
    n = 5
    t = np.arange(n) * 0.02
    data = MotorData(
        motor_voltage=np.ones(n),
        stator_current=np.ones(n),
        motor_power=np.ones(n),
        motor_energy=np.ones(n),
    )
    match = Match(
        match_id="GACMP_E5",
        timestamps=t,
        motors={"TalonFX-1": data},
        totals=data,
    )
    assert match.match_id == "GACMP_E5"
    assert "TalonFX-1" in match.motors


def test_motor_data_rotor_velocity_defaults_to_none():
    n = 5
    data = MotorData(
        motor_voltage=np.ones(n),
        stator_current=np.ones(n),
        motor_power=np.ones(n),
        motor_energy=np.ones(n),
    )
    assert data.rotor_velocity is None


def test_motor_data_accepts_rotor_velocity():
    n = 5
    vel = np.linspace(0.0, 10.0, n)
    data = MotorData(
        motor_voltage=np.ones(n),
        stator_current=np.ones(n),
        motor_power=np.ones(n),
        motor_energy=np.ones(n),
        rotor_velocity=vel,
    )
    np.testing.assert_array_equal(data.rotor_velocity, vel)
