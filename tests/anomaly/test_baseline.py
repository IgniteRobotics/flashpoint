import numpy as np
import pytest

from flashpoint.anomaly.baseline import MAD_SCALE, baseline_at, judge

FLOOR = 0.1


def test_median_and_scaled_mad_over_the_window() -> None:
    x = np.array([1.0, 2.0, 3.0, 4.0, 100.0, 5.0])
    b = baseline_at(x, 5, window=4, floor=FLOOR)
    assert b is not None
    prior = np.array([2.0, 3.0, 4.0, 100.0])  # the 4 values before index 5
    assert b.n == 4
    assert b.center == np.median(prior)
    assert b.spread == pytest.approx(MAD_SCALE * np.median(np.abs(prior - np.median(prior))))


def test_floor_applies_to_a_steady_history() -> None:
    b = baseline_at(np.full(10, 45.0), 9, window=20, floor=1.0)
    assert b is not None and b.spread == 1.0


def test_restart_index_limits_the_window() -> None:
    x = np.array([10.0, 10.0, 10.0, 50.0, 50.0, 50.0])
    b = baseline_at(x, 5, window=20, floor=FLOOR, start=3)
    assert b is not None and (b.n, b.center) == (2, 50.0)
    assert baseline_at(x, 3, window=20, floor=FLOOR, start=3) is None
    before = baseline_at(x, 2, window=20, floor=FLOOR, start=3)  # before the restart
    assert before is not None and before.n == 2


def test_min_baseline_gives_no_z() -> None:
    x = np.arange(8, dtype=float)
    judged = judge(x, window=20, floor=1.0, min_baseline=5)
    assert np.isnan(judged.z[:5]).all()
    assert not np.isnan(judged.z[5:]).any()
    assert judged.n.tolist() == [0, 1, 2, 3, 4, 5, 6, 7]


def test_z_score() -> None:
    x = np.array([45.0] * 10 + [55.0])
    judged = judge(x, window=20, floor=1.0, min_baseline=5)
    assert judged.z[10] == pytest.approx(10.0)
    assert judged.center[10] == 45.0 and judged.spread[10] == 1.0


def test_no_lookahead() -> None:
    rng = np.random.default_rng(7)
    x = rng.normal(40.0, 2.0, 40)
    short = judge(x[:30], window=20, floor=1.0, min_baseline=5)
    full = judge(x, window=20, floor=1.0, min_baseline=5)
    np.testing.assert_array_equal(short.z, full.z[:30])
    np.testing.assert_array_equal(short.center, full.center[:30])
