import polars as pl
import pytest

from flashpoint.semantics.physics import PhysicsConfig, motor_features

S = 1_000_000  # microseconds per second


def _series(metric: str, points: list[tuple[float, float]]) -> list[tuple[str, int, float]]:
    return [(metric, int(t * S), v) for t, v in points]


def _frame(*series: list[tuple[str, int, float]]) -> pl.DataFrame:
    rows = [r for s in series for r in s]
    return pl.DataFrame(
        rows, schema={"metric": pl.String, "t_us": pl.Int64, "value": pl.Float64}, orient="row"
    )


def test_time_weighted_mean_irregular_sampling() -> None:
    low = [(i * 0.5, 10.0) for i in range(18)]  # 10 A for 9 s, every 0.5 s
    high = [(9.0 + i * 0.1, 100.0) for i in range(10)]  # 100 A for 1 s, every 0.1 s
    features = motor_features(_frame(_series("supply_current", low + high)), 0, 10 * S)
    assert features["supply_current_mean"] == pytest.approx(19.0)


def test_time_before_first_sample_is_excluded() -> None:
    temps = _series("temp_c", [(5.0 + i, 40.0) for i in range(5)])
    features = motor_features(_frame(temps), 0, 10 * S)
    assert features["temp_mean_c"] == pytest.approx(40.0)


def test_constant_draw_energy() -> None:
    times = [i * 0.02 for i in range(3000)]  # 60 s at 50 Hz
    frame = _frame(
        _series("supply_voltage", [(t, 12.0) for t in times]),
        _series("supply_current", [(t, 10.0) for t in times]),
    )
    features = motor_features(frame, 0, 60 * S)
    assert features["supply_energy_wh"] == pytest.approx(2.0, rel=1e-3)


def _motor_constants(t_end: float) -> list[tuple[str, int, float]]:
    return _series("motor_kv_rpm_per_v", [(0, 500.0), (t_end, 500.0)]) + _series(
        "stall_current", [(0, 400.0), (t_end, 400.0)]
    )


def test_stall_time() -> None:
    times = [i * 0.01 for i in range(400)]  # 4 s
    frame = _frame(
        _series(
            "stator_current", [(t, 240.0 if 1.0 <= t < 3.0 else 5.0) for t in times]
        ),  # 60 % of stall
        _series("rotor_velocity_rps", [(t, 0.0 if 1.0 <= t < 3.0 else 50.0) for t in times]),
        _motor_constants(4.0),
    )
    features = motor_features(frame, 0, 4 * S)
    assert features["stall_s"] == pytest.approx(2.0, abs=0.02)


def test_free_spin_residual_near_zero() -> None:
    times = [i * 0.01 for i in range(200)]
    free_rps = 500.0 * 12.0 / 60.0  # Kv (rpm/V) * V -> rpm -> rps
    frame = _frame(
        _series("motor_voltage", [(t, 12.0) for t in times]),
        _series("rotor_velocity_rps", [(t, free_rps) for t in times]),
        _series("stator_current", [(t, 0.0) for t in times]),
        _motor_constants(2.0),
    )
    features = motor_features(frame, 0, 2 * S)
    assert features["residual_p95_a"] == pytest.approx(0.0, abs=0.5)
    assert features["flags"] == ""


def test_missing_constants_flagged() -> None:
    times = [i * 0.01 for i in range(100)]
    frame = _frame(
        _series("motor_voltage", [(t, 12.0) for t in times]),
        _series("stator_current", [(t, 20.0) for t in times]),
        _series("rotor_velocity_rps", [(t, 10.0) for t in times]),
    )
    features = motor_features(frame, 0, 1 * S)
    assert features["residual_p95_a"] is None
    assert "no-motor-constants" in features["flags"]


def test_velocity_keeps_sign_and_temp_rise() -> None:
    times = [i * 0.1 for i in range(601)]  # 60 s
    frame = _frame(
        _series("rotor_velocity_rps", [(t, -20.0) for t in times]),
        _series("temp_c", [(t, 30.0 + t / 6.0) for t in times]),  # +10 C per minute
    )
    features = motor_features(frame, 0, 61 * S)  # periods are [start, end)
    assert features["rotor_velocity_mean_rps"] == pytest.approx(-20.0)
    assert features["temp_max_c"] == pytest.approx(40.0)
    assert features["temp_rise_c_per_min"] == pytest.approx(10.0, rel=1e-3)


def test_custom_stall_thresholds() -> None:
    times = [i * 0.01 for i in range(100)]
    frame = _frame(
        _series("stator_current", [(t, 100.0) for t in times]),  # 25 % of stall
        _series("rotor_velocity_rps", [(t, 0.0) for t in times]),
        _motor_constants(1.0),
    )
    assert motor_features(frame, 0, S)["stall_s"] == 0.0
    loose = PhysicsConfig(stall_current_fraction=0.2)
    assert motor_features(frame, 0, S, loose)["stall_s"] == pytest.approx(1.0, abs=0.02)
