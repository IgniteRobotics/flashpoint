"""MODEL (optional, off by default): an isolation forest per subsystem over unit-match feature
vectors, each feature robust-scaled against that unit's own earlier matches.

The forest for an event is fit only on earlier events, so no match is judged with later data;
it is refit once per event. It needs the `ml` extra (scikit-learn), imported only when enabled.
"""

from collections.abc import Callable
from typing import Any

import numpy as np
import polars as pl

from flashpoint.anomaly.baseline import judge
from flashpoint.anomaly.findings import Finding, Flags
from flashpoint.anomaly.frame import Frame
from flashpoint.anomaly.settings import BASELINE_METRICS, METRIC_INFO, MODEL_METRIC, AnomalyConfig

MISSING = (
    "the model detector is enabled in anomaly.toml, but scikit-learn is not installed:"
    " install the `ml` extra (poetry install --extras ml); MODEL was skipped"
)
FEATURES = BASELINE_METRICS
N_ESTIMATORS = 100


def _forest() -> Any | None:
    try:
        from sklearn.ensemble import IsolationForest
    except ImportError:
        return None
    return IsolationForest


def available() -> bool:
    return _forest() is not None


def _fit(forest: Any, x: np.ndarray, contamination: float, train_keys: list[str], event: str
         ) -> Any:  # fmt: skip
    """`train_keys` and `event` are unused here; they let tests check what each fit saw."""
    del train_keys, event
    model = forest(n_estimators=N_ESTIMATORS, random_state=0, contamination=contamination)
    return model.fit(x)


def _scaled(frame: Frame, cfg: AnomalyConfig) -> tuple[pl.DataFrame, np.ndarray]:
    """Valid unit-match rows (a baseline for at least one feature) and their feature z."""
    parts, vectors = [], []
    for (_unit,), rows in frame.rows.group_by(["unit_id"], maintain_order=True):
        z = np.full((rows.height, len(FEATURES)), np.nan)
        for f, metric in enumerate(FEATURES):
            values = rows.get_column(metric).cast(pl.Float64).fill_null(np.nan).to_numpy()
            present = np.flatnonzero(~np.isnan(values))
            judged = judge(values[present], cfg.baseline.window, cfg.baseline.floors[metric],
                           cfg.baseline.min_baseline)  # fmt: skip
            z[present, f] = judged.z
        valid = ~np.isnan(z).all(axis=1)
        parts.append(rows.filter(pl.Series(valid)))
        vectors.append(z[valid])
    if not parts:
        return frame.rows.clear(), np.zeros((0, len(FEATURES)))
    return pl.concat(parts), np.vstack(vectors)


def _reason(z: np.ndarray) -> str:
    top = np.argsort(-np.nan_to_num(np.abs(z)), kind="stable")[:3]
    return ", ".join(f"{METRIC_INFO[FEATURES[i]][0]} (z {z[i]:+.1f})" for i in top)


def model_findings(frame: Frame, cfg: AnomalyConfig, warn: Callable[[str], None]) -> list[Finding]:
    forest = _forest()
    if forest is None:
        warn(MISSING)
        return []
    rows, z = _scaled(frame, cfg)
    if rows.is_empty():
        return []
    events = rows.get_column("match_key").str.split("_").list.first().to_numpy()
    subsystems = rows.get_column("subsystem").fill_null("").to_numpy()
    starts = rows.get_column("session_start").to_numpy()
    order = sorted(set(events), key=lambda e: (min(starts[events == e]), e))
    filled = np.nan_to_num(z)
    flagged: dict[tuple[str, str], tuple[float, float, str]] = {}  # (unit, key) -> score
    for n, event in enumerate(order):
        earlier = np.isin(events, order[:n])
        for subsystem in sorted(set(subsystems[events == event])):
            train = earlier & (subsystems == subsystem)
            if train.sum() < cfg.model.min_train:
                continue
            test = (events == event) & (subsystems == subsystem)
            match_keys = rows.get_column("match_key").to_numpy()
            model = _fit(
                forest, filled[train], cfg.model.contamination, list(match_keys[train]), event
            )
            hits = np.flatnonzero(test)[model.predict(filled[test]) == -1]
            scores = -model.score_samples(filled[hits]) if hits.size else []
            for i, score in zip(hits, scores, strict=True):
                worst = z[i][np.nanargmax(np.abs(z[i]))]
                unit = str(rows.get_column("unit_id")[int(i)])
                flagged[(unit, str(match_keys[i]))] = (float(score), float(worst), _reason(z[i]))
    findings = []
    units = {u for u, _ in flagged}
    for (unit,), unit_rows in frame.rows.group_by(["unit_id"], maintain_order=True):
        if unit not in units:
            continue
        keys = unit_rows.get_column("match_key").to_list()
        values, flags, reasons = (
            np.full(len(keys), np.nan),
            Flags.empty(len(keys)),
            [""] * len(keys),
        )
        for j, key in enumerate(keys):
            hit = flagged.get((str(unit), key))
            if hit is not None:
                values[j], reasons[j] = hit[0], hit[2]
                flags.mark(j, hit[1], j)
        findings.append(
            Finding(
                subject=str(unit), method="MODEL", metric=MODEL_METRIC, keys=keys, values=values,
                strict=flags, normal=flags, lo=np.full(len(keys), np.nan),
                hi=np.full(len(keys), np.nan),
                baseline_n=np.minimum(np.arange(len(keys)), cfg.baseline.window),
                reasons=reasons,
            )
        )  # fmt: skip
    return findings
