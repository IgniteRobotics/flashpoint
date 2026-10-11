"""SPEC: fixed limits judged per match (config `[spec]`). A SPEC anomaly's severity is the
highest level any match of its episode reached; it shows at every sensitivity."""

import numpy as np

from flashpoint.anomaly.episodes import Episode
from flashpoint.anomaly.findings import Finding, Flags
from flashpoint.anomaly.settings import SpecLimits, format_value

SPEC_METRICS = ("temp_max_c", "temp_mean_c", "stall_s", "reboots", "longest_gap_ms", "brownouts")
LEVELS = ("", "LOW", "MED", "HIGH")


def _limits(metric: str, limits: SpecLimits) -> tuple[float | None, float | None]:
    """(MED, HIGH) thresholds, reached at or above; None: no such level."""
    return {
        "temp_max_c": (limits.temp_max_med_c, limits.temp_max_high_c),
        "temp_mean_c": (limits.temp_mean_med_c, None),
        "stall_s": (limits.stall_med_s, None),
        "longest_gap_ms": (limits.gap_med_ms, limits.gap_high_ms),
        "brownouts": (None, float(limits.brownout_high)),
    }[metric]


def expected_text(metric: str, limits: SpecLimits) -> str:
    if metric == "reboots":
        return format_value(metric, 0)
    if metric == "brownouts":
        return format_value(metric, limits.brownout_high - 1)
    med, high = _limits(metric, limits)
    first = med if med is not None else high
    assert first is not None
    return f"< {format_value(metric, first)}"


def spec_finding(
    subject: str,
    metric: str,
    keys: list[str],
    values: np.ndarray,
    limits: SpecLimits,
    reboots_enabled: np.ndarray | None = None,
) -> Finding:
    levels = []
    for i, value in enumerate(values):
        level = ""
        if np.isnan(value):
            pass
        elif metric == "reboots":
            on = reboots_enabled[i] if reboots_enabled is not None else np.nan
            level = "HIGH" if on > 0 else "MED" if value > 0 else ""
        else:
            med, high = _limits(metric, limits)
            if high is not None and value >= high:
                level = "HIGH"
            elif med is not None and value >= med:
                level = "MED"
        levels.append(level)
    flags = Flags.empty(len(keys))
    for i, level in enumerate(levels):
        if level:
            flags.mark(i, 0.0, i)
    if metric == "reboots":
        limit = 0.0
    elif metric == "brownouts":
        limit = float(limits.brownout_high)
    else:
        med, high = _limits(metric, limits)
        limit = float(med if med is not None else high)  # type: ignore[arg-type]
    size = len(keys)
    return Finding(
        subject=subject, method="SPEC", metric=metric, keys=keys, values=values,
        strict=flags, normal=flags, lo=np.full(size, np.nan), hi=np.full(size, limit),
        baseline_n=np.zeros(size, dtype=np.int64), levels=levels,
        expected=expected_text(metric, limits),
    )  # fmt: skip


def episode_level(finding: Finding, episode: Episode) -> str:
    assert finding.levels is not None
    return max((finding.levels[i] for i in episode.flagged), key=LEVELS.index)
