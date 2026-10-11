"""STEP and DRIFT: how a unit's metric left its own baseline.

- STEP: the last `persist` values are all at least `step_z` from the baseline before them, in
  the same direction, and they jumped there (their median is `step_z` spreads from the values
  just before the run). Onset is the first of them.
- DRIFT: otherwise, the median of the last `span` values is at least `drift_z` from the
  baseline before them. Flagged are the values beyond the band in that direction. A STEP inside
  the drift's baseline window suppresses it: a step must not read as a drift while its new level
  is still entering the baseline.

Flags are per index, with the score (z) that flagged it and the index at which it became known.
"""

from dataclasses import dataclass

import numpy as np

from flashpoint.anomaly.baseline import Baseline, baseline_at

NEVER = np.iinfo(np.int64).max


@dataclass(frozen=True)
class Rule:
    window: int
    floor: float
    min_baseline: int
    step_z: float
    persist: int
    drift_z: float
    span: int


@dataclass
class Flags:
    """Per index: flagged, the signed z that flagged it, and the index it was known at."""

    flagged: np.ndarray
    score: np.ndarray
    known_at: np.ndarray

    @classmethod
    def empty(cls, size: int) -> "Flags":
        return cls(np.zeros(size, bool), np.zeros(size), np.full(size, NEVER, dtype=np.int64))

    def mark(self, j: int, z: float, at: int) -> None:
        self.flagged[j] = True
        if abs(z) > abs(self.score[j]):
            self.score[j] = z
        self.known_at[j] = min(self.known_at[j], at)


def _run_baseline(x: np.ndarray, s: int, i: int, rule: Rule, start: int) -> Baseline | None:
    if s < 0 or (s < start <= i):  # a run must not straddle a baseline restart
        return None
    b = baseline_at(x, s, rule.window, rule.floor, start)
    return b if b is not None and b.n >= rule.min_baseline else None


def step_flags(x: np.ndarray, rule: Rule, start: int = 0) -> Flags:
    flags = Flags.empty(x.size)
    p = rule.persist
    for i in range(x.size):
        s = i - p + 1
        b = _run_baseline(x, s, i, rule, start)
        if b is None:
            continue
        run = (x[s : i + 1] - b.center) / b.spread
        if not ((run >= rule.step_z).all() or (run <= -rule.step_z).all()):
            continue
        lo = start if s >= start else 0
        before = x[max(lo, s - p) : s]
        jump = abs(float(np.median(x[s : i + 1])) - float(np.median(before))) / b.spread
        if jump < rule.step_z:
            continue
        for j, z in zip(range(s, i + 1), run, strict=True):
            flags.mark(j, float(z), i)
    return flags


def drift_flags(x: np.ndarray, rule: Rule, steps: np.ndarray, start: int = 0) -> Flags:
    flags = Flags.empty(x.size)
    for i in range(x.size):
        s = i - rule.span + 1
        b = _run_baseline(x, s, i, rule, start)
        if b is None:
            continue
        d = (float(np.median(x[s : i + 1])) - b.center) / b.spread
        if abs(d) < rule.drift_z:
            continue
        lo = start if s >= start else 0
        if steps[max(lo, s - rule.window) : i + 1].any():
            continue
        sign = 1.0 if d > 0 else -1.0
        for j in range(s, i + 1):
            z = (x[j] - b.center) / b.spread
            if sign * z >= rule.drift_z:
                flags.mark(j, float(z), i)
    return flags
