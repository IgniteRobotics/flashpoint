import numpy as np
import pytest

from flashpoint.anomaly.settings import SpecLimits
from flashpoint.anomaly.spec_rules import spec_finding

LIMITS = SpecLimits()
KEYS = [f"2026syn_qm{i + 1}" for i in range(6)]


def _finding(metric: str, values: list[float | None], enabled: list[float] | None = None):  # type: ignore[no-untyped-def]
    x = np.array([np.nan if v is None else v for v in values], dtype=float)
    on = None if enabled is None else np.array(enabled, dtype=float)
    return spec_finding("ctre:X", metric, KEYS[: len(values)], x, LIMITS, on)


def test_hot_motor_is_med_with_the_limit() -> None:
    f = _finding("temp_max_c", [40.0, 66.0, 40.0])
    assert f.levels == ["", "MED", ""]
    assert f.expected == "< 65 °C"
    assert f.hi[1] == 65.0
    assert f.normal.flagged.tolist() == f.strict.flagged.tolist() == [False, True, False]


@pytest.mark.parametrize(
    ("metric", "value", "level", "expected"),
    [
        ("temp_max_c", 75.0, "HIGH", "< 65 °C"),
        ("temp_max_c", 64.9, "", "< 65 °C"),
        ("temp_mean_c", 55.0, "MED", "< 55 °C"),
        ("stall_s", 3.0, "MED", "< 3 s"),
        ("stall_s", 2.9, "", "< 3 s"),
        ("longest_gap_ms", 250.0, "MED", "< 250 ms"),
        ("longest_gap_ms", 820.0, "MED", "< 250 ms"),
        ("longest_gap_ms", 1000.0, "HIGH", "< 250 ms"),
        ("brownouts", 1.0, "HIGH", "0 brownouts"),
        ("brownouts", 0.0, "", "0 brownouts"),
    ],
)
def test_limits(metric: str, value: float, level: str, expected: str) -> None:
    f = _finding(metric, [value])
    assert f.levels == [level]
    assert f.expected == expected


def test_reboot_while_enabled_is_high_otherwise_med() -> None:
    f = _finding("reboots", [1.0, 2.0, 0.0], enabled=[1.0, 0.0, 0.0])
    assert f.levels == ["HIGH", "MED", ""]
    assert f.expected == "0 reboots"


def test_missing_values_are_never_flagged() -> None:
    f = _finding("temp_max_c", [None, 70.0])
    assert f.levels == ["", "MED"]


def test_severity_is_the_episode_maximum() -> None:
    from flashpoint.anomaly.episodes import episodes
    from flashpoint.anomaly.spec_rules import episode_level

    f = _finding("temp_max_c", [66.0, 76.0, 66.0, 40.0])
    (episode,) = episodes(f.flagged)
    assert episode_level(f, episode) == "HIGH"
