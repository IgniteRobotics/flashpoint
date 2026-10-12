"""Derive runs detection as its last step (spec: anomaly-detection / Detection inputs and
timing)."""

import json
from pathlib import Path

import pytest

from flashpoint.cli import main
from tests.wpilog_builder import WpilogBuilder


def test_derive_runs_detection(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    lake = tmp_path / "lake"
    b = WpilogBuilder().start(1, "NT:/FMSInfo/EventName", "string").start(2, "x", "double")
    log = b.string(1, 10, "GACMP").double(2, 20, 1.0).write(tmp_path / "FRC_20260409_215113.wpilog")
    assert main(["ingest", "--no-derive", "--lake", str(lake), str(log)]) == 0
    summary = lake / "gold" / "anomalies" / "summary.json"
    assert not summary.exists()

    assert main(["derive", "--lake", str(lake)]) == 0
    assert "detected 0 anomalies" in capsys.readouterr().out
    assert json.loads(summary.read_text())["anomalies"] == 0

    assert main(["derive", "--lake", str(lake)]) == 0  # nothing changed: detection skips
    assert "unchanged: 0 anomalies" in capsys.readouterr().out
