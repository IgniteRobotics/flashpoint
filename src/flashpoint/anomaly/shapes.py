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

from flashpoint.anomaly.baseline import Baseline, baseline_at, judge
from flashpoint.anomaly.findings import Finding, Flags


@dataclass(frozen=True)
class Rule:
    window: int
    floor: float
    min_baseline: int
    step_z: float
    persist: int
    drift_z: float
    span: int


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


def unit_findings(
    subject: str,
    metric: str,
    keys: list[str],
    values: np.ndarray,
    normal: Rule,
    strict: Rule,
    start_key: str | None = None,
) -> tuple[Finding, Finding, np.ndarray]:
    """STEP and DRIFT findings for one unit's metric, plus its z per match (for the sibling
    pair rule). Missing values are left out of the series and never flagged; the baseline
    restarts at `start_key` when given."""
    present = np.flatnonzero(~np.isnan(values))
    x = values[present]
    # the first present value at or after the restart match
    start = int(np.searchsorted(present, keys.index(start_key))) if start_key in keys else 0
    judged = judge(x, normal.window, normal.floor, normal.min_baseline, start)
    has_band = judged.n >= normal.min_baseline
    size = len(keys)
    out = []
    for method in ("STEP", "DRIFT"):
        results = {}
        for name, rule in (("strict", strict), ("normal", normal)):
            steps = step_flags(x, rule, start)
            results[name] = steps if method == "STEP" else drift_flags(
                x, rule, steps.flagged, start
            )  # fmt: skip
        threshold = normal.step_z if method == "STEP" else normal.drift_z
        lo = np.where(has_band, judged.center - threshold * judged.spread, np.nan)
        hi = np.where(has_band, judged.center + threshold * judged.spread, np.nan)
        out.append(
            Finding(
                subject=subject, method=method, metric=metric, keys=keys, values=values,
                strict=_scatter(size, present, results["strict"]),
                normal=_scatter(size, present, results["normal"]),
                lo=_full(size, present, lo), hi=_full(size, present, hi),
                baseline_n=_full(size, present, judged.n.astype(float), 0).astype(np.int64),
            )
        )  # fmt: skip
    return out[0], out[1], _full(size, present, judged.z)


def _full(size: int, index: np.ndarray, values: np.ndarray, fill: float = np.nan) -> np.ndarray:
    out = np.full(size, fill, dtype=float)
    out[index] = values
    return out


def _scatter(size: int, index: np.ndarray, flags: Flags) -> Flags:
    out = Flags.empty(size)
    for k, i in enumerate(index):
        if flags.flagged[k]:
            out.mark(int(i), float(flags.score[k]), int(index[flags.known_at[k]]))
    return out
