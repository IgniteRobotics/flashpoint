"""Brownout counts on the golden corpus agree with Replay (spec: anomaly-detection)."""

import json
from typing import Any

import pytest

from flashpoint import config
from flashpoint.anomaly import power
from flashpoint.report.build import ReportBuilder
from flashpoint.report.paths import data_dir
from flashpoint.report.settings import load_report_config

pytestmark = pytest.mark.corpus
Q7 = "2026gacmp_qm7"


def test_q7_brownouts_agree_with_replay(derived_corpus_lake: Any) -> None:
    builder = ReportBuilder(derived_corpus_lake, config.config_root())
    try:
        builder.build(match_keys=[Q7])
    finally:
        builder.close()
    text = (data_dir(derived_corpus_lake) / f"{Q7}.js").read_text(encoding="utf-8")
    data = json.loads(text[text.index("(") + 1 : -3])
    faults = [m for m in data["markers"] if m["kind"] == "brownout" and m["level"] == "FAULT"]

    counts = power.brownout_counts(
        derived_corpus_lake, load_report_config(config.config_root())
    ).counts
    assert faults, "Q7 is the corpus brownout match"
    assert counts[Q7]["brownouts"] == len(faults)
    assert counts[Q7]["robot"] == "2026-comp"
