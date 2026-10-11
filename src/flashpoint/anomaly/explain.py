"""One anomaly row per episode: severity, confidence, the facts, a title and "why" a student
can check, and the evidence series. All text is plain text; the views render it as text."""

import json
import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from flashpoint.anomaly.episodes import Episode, anomaly_id, sensitivity
from flashpoint.anomaly.findings import NEVER, Finding
from flashpoint.anomaly.settings import METRIC_INFO, POWER, AnomalyConfig, format_value
from flashpoint.anomaly.spec_rules import episode_level
from flashpoint.report.build import match_label

EVIDENCE_MATCHES = 40
SEVERITIES = ("LOW", "MED", "HIGH")


@dataclass(frozen=True)
class Context:
    """Where the subject sits now (its slot at its latest match) and the configuration."""

    robot: str
    season: str
    slot_id: str
    role: str | None
    subsystem: str | None
    model: str | None
    cfg: AnomalyConfig
    low_alignment: frozenset[str] = field(default_factory=frozenset)  # match keys


def z_severity(z: float, cfg: AnomalyConfig) -> str:
    size = abs(z)
    return "HIGH" if size >= cfg.baseline.high_z else "MED" if size >= cfg.baseline.med_z else "LOW"


def confidence(baseline_n: int, matches: int) -> str:
    if baseline_n < 10 or matches <= 1:
        return "Low"
    if baseline_n >= 20 and matches >= 3:
        return "High"
    return "Medium"


def _num(value: float) -> float | None:
    return None if value is None or not math.isfinite(value) else round(float(value), 4)


def _evidence(finding: Finding, episode: Episode) -> dict[str, Any]:
    first = max(0, episode.last - EVIDENCE_MATCHES + 1)
    flagged = set(episode.flagged)
    points = []
    for i in range(first, episode.last + 1):
        point = {
            "match_key": finding.keys[i],
            "value": _num(finding.values[i]),
            "lo": _num(finding.lo[i]),
            "hi": _num(finding.hi[i]),
            "flagged": i in flagged,
        }
        if finding.peer is not None:
            point["peer"] = _num(finding.peer[i])
        points.append(point)
    return {"points": points, "peer": finding.peer_name}


def _sentence(name: str) -> str:
    """Capitalise the first letter only ("CAN" stays "CAN")."""
    return name[:1].upper() + name[1:]


def _direction(z: float) -> str:
    return "up" if z > 0 else "down"


def _texts(finding: Finding, episode: Episode, z: float, cfg: AnomalyConfig) -> dict[str, str]:
    name, _ = METRIC_INFO[finding.metric]
    onset, last = finding.keys[episode.onset], finding.keys[episode.last]
    flagged = finding.values[list(episode.flagged)]
    flagged = flagged[~np.isnan(flagged)]
    when = match_label(onset) if onset == last else f"{match_label(onset)}–{match_label(last)}"
    method = finding.method
    if method == "SPEC":
        worst = float(flagged.max())
        actual = format_value(finding.metric, worst)
        expected = finding.expected or ""
        if finding.metric == "reboots":
            level = episode_level(finding, episode)
            what = "Rebooted while enabled" if level == "HIGH" else "Rebooted while disabled"
            title = f"{what} ({actual}) in {when}"
        else:
            title = (
                f"{actual} in {when} (expected {expected})"
                if finding.metric == "brownouts"
                else f"{_sentence(name)} reached {actual} in {when} (expected {expected})"
            )
        why = (
            f"SPEC: compared with a fixed limit from config/anomaly.toml. The {name} was"
            f" {actual}; expected {expected}."
        )
        return {"actual": actual, "expected": expected, "title": title, "why": why}
    if method == "MODEL":
        reasons = finding.reasons[episode.last] if finding.reasons else ""
        return {
            "actual": f"score {float(np.max(flagged)):.2f}",
            "expected": "typical for this subsystem",
            "title": f"Unusual combination of motor features in {when}",
            "why": (
                "MODEL: an isolation forest trained on earlier events' unit-matches of this"
                f" subsystem put {match_label(last)} in its most unusual"
                f" {cfg.model.contamination:.0%}. Furthest from this unit's own baseline:"
                f" {reasons}."
            ),
        }
    i = episode.onset
    center = (finding.lo[i] + finding.hi[i]) / 2
    half = (finding.hi[i] - finding.lo[i]) / 2
    actual = format_value(finding.metric, float(np.median(flagged)))
    if method == "SIBLING":
        assert finding.peer is not None
        seen = [i for i in episode.flagged if not np.isnan(finding.values[i])]
        gap = float(np.median(finding.values[seen] - finding.peer[seen]))
        side = "above" if gap > 0 else "below"
        expected = f"within {format_value(finding.metric, half)} of {finding.peer_name}"
        title = (
            f"{_sentence(name)} {format_value(finding.metric, abs(gap))} {side}"
            f" {finding.peer_name} since {match_label(onset)}"
        )
        why = (
            f"SIBLING: compared with {finding.peer_name} in the same matches. The difference"
            f" left its usual range by {abs(z):.1f} spreads in at least {cfg.sibling.need} of"
            f" {cfg.sibling.of} matches the whole group played."
        )
        return {"actual": actual, "expected": expected, "title": title, "why": why}
    base = format_value(finding.metric, center)
    expected = f"{base} ± {format_value(finding.metric, half)}"
    n = int(finding.baseline_n[i])
    if method == "STEP":
        title = f"{_sentence(name)} stepped {_direction(z)} from {base} to {actual} at {when}"
        why = (
            f"STEP: compared with this unit's own previous {n} matches (median {base}). The"
            f" {name} jumped and stayed at least {cfg.baseline.step_z:g} spreads"
            f" {'above' if z > 0 else 'below'} it for {episode.matches} matches: a sudden"
            " change that persisted."
        )
    else:  # DRIFT
        title = f"{_sentence(name)} drifted {_direction(z)} from {base} to {actual} since {when}"
        why = (
            f"DRIFT: the median of the last {cfg.baseline.drift_span} matches moved at least"
            f" {cfg.baseline.drift_z:g} spreads {'above' if z > 0 else 'below'} this unit's own"
            f" earlier {n} matches (median {base}): a gradual change."
        )
    return {"actual": actual, "expected": expected, "title": title, "why": why}


def describe(finding: Finding, episode: Episode, ctx: Context) -> dict[str, Any]:
    flagged = list(episode.flagged)
    scores = np.where(
        np.abs(finding.strict.score[flagged]) >= np.abs(finding.normal.score[flagged]),
        finding.strict.score[flagged],
        finding.normal.score[flagged],
    )
    z = float(scores[np.argmax(np.abs(scores))]) if scores.size else 0.0
    if finding.method == "SPEC":
        severity = episode_level(finding, episode)
    else:
        severity = z_severity(z, ctx.cfg)
        if finding.method == "MODEL":
            severity = min(severity, "MED", key=SEVERITIES.index)
    tag = sensitivity(episode, finding.normal.flagged, severity)
    if finding.method == "MODEL" and tag == "relaxed":
        tag = "normal"  # MODEL shows only at normal and strict
    baseline_n = int(finding.baseline_n[episode.onset])
    if finding.method == "SPEC":
        baseline_n = min(episode.onset, ctx.cfg.baseline.window)  # the subject's history
    known = min(int(finding.strict.known_at[i]) for i in flagged)
    normal_known = [int(finding.normal.known_at[i]) for i in flagged if finding.normal.flagged[i]]
    power = finding.subject.startswith(f"{POWER}:")
    keys = [finding.keys[i] for i in flagged]
    return {
        "id": anomaly_id(finding.subject, finding.method, finding.metric,
                         finding.keys[episode.onset]),
        "subject": POWER if power else finding.subject,
        "subject_key": finding.subject,
        "subject_kind": POWER if power else "unit",
        "robot": ctx.robot,
        "season": ctx.season,
        "slot_id": POWER if power else ctx.slot_id,
        "role": "battery (lowest device supply voltage)" if power else ctx.role,
        "subsystem": POWER if power else ctx.subsystem,
        "model": None if power else ctx.model,
        "method": finding.method,
        "metric": finding.metric,
        "metric_name": METRIC_INFO[finding.metric][0],
        "metric_unit": METRIC_INFO[finding.metric][1],
        "severity": severity,
        "sensitivity": tag,
        "onset_match_key": finding.keys[episode.onset],
        "last_match_key": finding.keys[episode.last],
        "matches_affected": episode.matches,
        "detected_match_key": finding.keys[known] if known != NEVER else None,
        "detected_normal_match_key": finding.keys[min(normal_known)] if normal_known else None,
        **_texts(finding, episode, z, ctx.cfg),
        "actual_value": _num(float(np.nanmax(finding.values[flagged]))
                             if finding.method == "SPEC"
                             else float(np.nanmedian(finding.values[flagged]))),
        "confidence": confidence(baseline_n, episode.matches),
        "baseline_n": baseline_n,
        "max_z": _num(z) if finding.method != "SPEC" else None,
        "peer_name": finding.peer_name,
        "flagged_keys": json.dumps(keys),
        "low_alignment": any(k in ctx.low_alignment for k in keys),
        "evidence": json.dumps(_evidence(finding, episode), ensure_ascii=False),
    }  # fmt: skip
