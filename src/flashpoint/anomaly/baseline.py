"""Per-unit baselines: the median and scaled MAD of a unit's previous matches for one metric.

Every value at index i is judged only against indices before i (and not before the series'
baseline restart), so appending matches never changes an earlier judgement.
"""

from dataclasses import dataclass

import numpy as np

MAD_SCALE = 1.4826  # MAD -> standard deviation for normal data


@dataclass(frozen=True)
class Baseline:
    center: float
    spread: float
    n: int


def baseline_at(
    x: np.ndarray, i: int, window: int, floor: float, start: int = 0
) -> Baseline | None:
    """The baseline for index i: up to `window` values before i, none before `start` (a
    restart) once i has reached it; indices before a restart keep their full history."""
    lo = start if i >= start else 0
    prior = x[max(lo, i - window) : i]
    if prior.size == 0:
        return None
    center = float(np.median(prior))
    spread = max(MAD_SCALE * float(np.median(np.abs(prior - center))), floor)
    return Baseline(center, spread, int(prior.size))


@dataclass(frozen=True)
class Judged:
    """Per index: baseline center, spread, size, and z (NaN below `min_baseline`)."""

    center: np.ndarray
    spread: np.ndarray
    n: np.ndarray
    z: np.ndarray


def judge(x: np.ndarray, window: int, floor: float, min_baseline: int, start: int = 0) -> Judged:
    size = x.size
    center, spread = np.full(size, np.nan), np.full(size, np.nan)
    n, z = np.zeros(size, dtype=np.int64), np.full(size, np.nan)
    for i in range(size):
        b = baseline_at(x, i, window, floor, start)
        if b is None:
            continue
        center[i], spread[i], n[i] = b.center, b.spread, b.n
        if b.n >= min_baseline:
            z[i] = (x[i] - b.center) / b.spread
    return Judged(center, spread, n, z)
