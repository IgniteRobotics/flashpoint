"""Brownout counts on the golden corpus agree with Replay (spec: anomaly-detection)."""

import json
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.anomaly import power
from flashpoint.lake.paths import LakePaths
from flashpoint.report.build import ReportBuilder
from flashpoint.report.paths import data_dir
from flashpoint.report.settings import load_report_config
from flashpoint.semantics.robot_config import load_robots
from tests.brownout_config import Q7_BROWNOUTS

pytestmark = pytest.mark.corpus
Q7 = "2026gacmp_qm7"


def _replay_markers(lake: LakePaths, config_dir: Path) -> list[dict[str, Any]]:
    builder = ReportBuilder(lake, config_dir)
    try:
        builder.build(match_keys=[Q7])
    finally:
        builder.close()
    text = (data_dir(lake) / f"{Q7}.js").read_text(encoding="utf-8")
    markers: list[dict[str, Any]] = json.loads(text[text.index("(") + 1 : -3])["markers"]
    return markers


def _counts(lake: LakePaths, config_dir: Path) -> dict[str, dict[str, Any]]:
    report = load_report_config(config_dir)
    return power.brownout_counts(lake, report, load_robots(config_dir / "robots")).counts


def test_q7_brownouts_agree_with_replay(derived_corpus_lake: Any, brownout_config: Path) -> None:
    """Under the test thresholds (brownout 8.5 V, sag 9.0 V), above Q7's lowest proxy voltage."""
    markers = _replay_markers(derived_corpus_lake, brownout_config)
    faults = [m for m in markers if m["kind"] == "brownout" and m["level"] == "FAULT"]
    counts = _counts(derived_corpus_lake, brownout_config)
    assert len(faults) == Q7_BROWNOUTS
    assert counts[Q7]["brownouts"] == len(faults)
    assert (counts[Q7]["robot"], counts[Q7]["proxy"]) == ("2026-comp", "gyro")
    assert all("gyro" in m["message"] for m in markers if m["source"] == "power")


def test_no_brownouts_from_motor_dips(derived_corpus_lake: Any) -> None:
    """With the shipped thresholds the gyro never reaches 6.75 V, although motors' supply
    voltages dip below it."""
    markers = _replay_markers(derived_corpus_lake, config.config_root())
    assert [m for m in markers if m["kind"] == "brownout"] == []
    counts = _counts(derived_corpus_lake, config.config_root())
    assert counts[Q7]["brownouts"] == 0 and counts[Q7]["lowest_v"] > 6.75
