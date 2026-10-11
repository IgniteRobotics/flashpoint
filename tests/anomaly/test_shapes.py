import numpy as np

from flashpoint.anomaly.shapes import Rule, drift_flags, step_flags
from tests.anomaly.gold_builder import steady

NORMAL_TEMP = Rule(window=20, floor=1.0, min_baseline=5, step_z=3.5, persist=3, drift_z=2.5,
                   span=5)  # fmt: skip
STRICT_TEMP = Rule(window=20, floor=1.0, min_baseline=5, step_z=2.5, persist=2, drift_z=1.5,
                   span=5)  # fmt: skip
NORMAL_CURRENT = Rule(window=20, floor=2.0, min_baseline=5, step_z=3.5, persist=3, drift_z=2.5,
                      span=5)  # fmt: skip
STRICT_CURRENT = Rule(window=20, floor=2.0, min_baseline=5, step_z=2.5, persist=2, drift_z=1.5,
                      span=5)  # fmt: skip


def _shapes(x: np.ndarray, rule: Rule) -> tuple[np.ndarray, np.ndarray]:
    steps = step_flags(x, rule)
    return steps.flagged, drift_flags(x, rule, steps.flagged).flagged


def test_sudden_step() -> None:
    x = np.array(steady(45.0, 20) + steady(55.0, 25, start=3))
    for rule in (NORMAL_TEMP, STRICT_TEMP):
        steps, drifts = _shapes(x[:23], rule)
        assert steps[20:23].all() and not steps[:20].any()
        assert not drifts.any()
        steps, drifts = _shapes(x, rule)  # and long after: the new level never reads as drift
        assert not drifts.any()
    flags = step_flags(x[:23], NORMAL_TEMP)
    assert flags.known_at[20] == 22  # known after match 23 (index 22)
    assert flags.score[20] > 6


def test_slow_drift() -> None:
    x = np.array(steady(40.0, 15) + [40.0 + 0.6 * k for k in range(1, 26)])
    for rule in (NORMAL_CURRENT, STRICT_CURRENT):
        steps, drifts = _shapes(x[:30], rule)
        assert not steps.any()
    _, drifts = _shapes(x[:30], NORMAL_CURRENT)
    assert drifts.any()


def test_one_bad_match() -> None:
    x = np.array(steady(45.0, 30))
    x[20] = 53.0  # z = 8: the spread is at its 1 °C floor
    steps, drifts = _shapes(x, NORMAL_TEMP)
    assert not steps.any() and not drifts.any()


def test_strict_only_drift() -> None:
    x = np.array([45.0] * 20 + [47.0] * 5)  # floor 1: z = 2.0 for 5 matches
    steps, drifts = _shapes(x, NORMAL_TEMP)
    assert not steps.any() and not drifts.any()
    steps, drifts = _shapes(x, STRICT_TEMP)
    assert not steps.any() and drifts[20:].all()


def test_restart_starts_a_new_baseline() -> None:
    x = np.array(steady(40.0, 20) + steady(60.0, 15))
    assert step_flags(x, NORMAL_CURRENT).flagged.any()
    restarted = step_flags(x, NORMAL_CURRENT, start=20)
    steps = restarted.flagged
    assert not steps.any()
    assert not drift_flags(x, NORMAL_CURRENT, steps, start=20).flagged.any()


def test_no_lookahead() -> None:
    rng = np.random.default_rng(3)
    x = np.concatenate([rng.normal(40, 1, 25), rng.normal(48, 1, 15)])
    short, full = step_flags(x[:30], NORMAL_CURRENT), step_flags(x, NORMAL_CURRENT)
    known = full.known_at < 30
    np.testing.assert_array_equal(short.flagged, (full.flagged & known)[:30])
