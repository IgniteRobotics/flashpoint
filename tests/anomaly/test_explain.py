import json

import numpy as np
import pytest

from flashpoint.anomaly.episodes import episodes
from flashpoint.anomaly.explain import Context, confidence, describe, z_severity
from flashpoint.anomaly.settings import AnomalyConfig
from flashpoint.anomaly.shapes import Rule, unit_findings
from flashpoint.anomaly.siblings import Member, sibling_findings
from flashpoint.anomaly.spec_rules import spec_finding
from tests.anomaly.gold_builder import steady

CFG = AnomalyConfig()
NORMAL = Rule(20, 1.0, 5, 3.5, 3, 2.5, 5)
STRICT = Rule(20, 1.0, 5, 2.5, 2, 1.5, 5)
KEYS = [f"2026syn_qm{i + 1}" for i in range(23)]
CTX = Context(robot="2026-comp", season="2026", slot_id="drive-fl", role="front-left drive",
              subsystem="drivetrain", model="TalonFX", cfg=CFG)  # fmt: skip


@pytest.mark.parametrize(
    ("z", "severity"), [(6.0, "HIGH"), (-6.5, "HIGH"), (4.0, "MED"), (3.99, "LOW"), (2.0, "LOW")]
)
def test_z_severity(z: float, severity: str) -> None:
    assert z_severity(z, CFG) == severity


@pytest.mark.parametrize(
    ("baseline", "matches", "level"),
    [(7, 3, "Low"), (25, 1, "Low"), (12, 3, "Medium"), (20, 2, "Medium"), (20, 3, "High")],
)
def test_confidence(baseline: int, matches: int, level: str) -> None:
    assert confidence(baseline, matches) == level


def _step() -> dict[str, object]:
    x = np.array(steady(45.0, 20) + steady(55.0, 3, start=3))
    step, _, _ = unit_findings("ctre:DRIVE-FL", "temp_max_c", KEYS, x, NORMAL, STRICT)
    (episode,) = episodes(step.flagged)
    return describe(step, episode, CTX)


def test_evidence_for_a_step() -> None:
    row = _step()
    evidence = json.loads(str(row["evidence"]))
    points = evidence["points"]
    assert [p["match_key"] for p in points] == KEYS  # matches 1-23
    assert [p["match_key"] for p in points if p["flagged"]] == KEYS[20:23]
    assert all(p["lo"] is None for p in points[:5])
    p = points[21]
    assert p["lo"] == pytest.approx(45.0 - 3.5 * 1.0, abs=0.6)
    assert p["hi"] - p["lo"] == pytest.approx(2 * 3.5 * 1.0, abs=1.2)
    assert evidence["peer"] is None


def test_step_row() -> None:
    row = _step()
    assert (row["method"], row["metric"], row["severity"]) == ("STEP", "temp_max_c", "HIGH")
    assert (row["onset_match_key"], row["last_match_key"], row["matches_affected"]) == (
        "2026syn_qm21", "2026syn_qm23", 3,
    )  # fmt: skip
    assert row["sensitivity"] == "relaxed"
    assert row["confidence"] == "High"  # 20 baseline matches, 3 affected
    assert row["detected_match_key"] == "2026syn_qm22"  # strict: 2 matches in a row
    assert row["detected_normal_match_key"] == "2026syn_qm23"
    assert row["actual"] == "55 °C" and str(row["expected"]).startswith("45")
    assert "stepped up" in str(row["title"]) and "45" in str(row["title"])
    assert str(row["why"]).startswith("STEP:")
    assert row["subject"] == "ctre:DRIVE-FL" and row["slot_id"] == "drive-fl"
    assert isinstance(row["title"], str) and "<" not in str(row["title"])


def test_low_confidence_with_a_short_baseline() -> None:
    x = np.array(steady(45.0, 7) + [55.0, 55.5, 54.5])
    keys = KEYS[:10]
    step, _, _ = unit_findings("ctre:X", "temp_max_c", keys, x, NORMAL, STRICT)
    (episode,) = episodes(step.flagged)
    assert describe(step, episode, CTX)["confidence"] == "Low"


def test_spec_row() -> None:
    x = np.array([40.0, 66.0, 40.0])
    finding = spec_finding("ctre:X", "temp_max_c", KEYS[:3], x, CFG.spec)
    (episode,) = episodes(finding.flagged)
    row = describe(finding, episode, CTX)
    assert (row["severity"], row["actual"], row["expected"]) == ("MED", "66 °C", "< 65 °C")
    assert row["sensitivity"] == "relaxed"
    assert str(row["why"]).startswith("SPEC:")
    evidence = json.loads(str(row["evidence"]))
    assert evidence["points"][1]["hi"] == 65.0 and evidence["points"][1]["flagged"]


def test_sibling_row_carries_the_peer() -> None:
    drive = ["drive-fl", "drive-fr", "drive-bl", "drive-br"]
    keys = KEYS[:20]
    members = [
        Member(s, f"ctre:{s.upper()}", keys,
               np.array([v + (6.0 if s == "drive-bl" and i >= 15 else 0.0)
                         for i, v in enumerate(steady(40.0, 20, start=k))]))
        for k, s in enumerate(drive)
    ]  # fmt: skip
    hot = next(f for f in sibling_findings("temp_max_c", members, keys, CFG, {}) if f.flagged.any())
    row = describe(hot, episodes(hot.flagged)[0], CTX)
    evidence = json.loads(str(row["evidence"]))
    assert evidence["peer"] == "median of drive-fl, drive-fr, drive-br"
    assert all("peer" in p for p in evidence["points"])
    assert "above" in str(row["title"])


def test_evidence_is_capped_at_40_matches() -> None:
    keys = [f"2026syn_qm{i + 1}" for i in range(60)]
    x = np.array(steady(45.0, 57) + [55.0, 55.5, 54.5])
    step, _, _ = unit_findings("ctre:X", "temp_max_c", keys, x, NORMAL, STRICT)
    (episode,) = episodes(step.flagged)
    points = json.loads(str(describe(step, episode, CTX)["evidence"]))["points"]
    assert len(points) == 40 and points[-1]["match_key"] == keys[-1]
