from __future__ import annotations

import matplotlib
matplotlib.use("Agg")

from pathlib import Path
import copy

import pytest

from utils.reporter import build_report


def test_build_report_creates_pdf_file(simple_match, tmp_path: Path) -> None:
    output = tmp_path / "report.pdf"
    build_report([simple_match], output)
    assert output.exists()
    assert output.stat().st_size > 0


def test_build_report_with_per_motor_graphs(simple_match, tmp_path: Path) -> None:
    output = tmp_path / "report_per_motor.pdf"
    build_report([simple_match], output, per_motor=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_build_report_with_supply_data(simple_match_with_supply, tmp_path: Path) -> None:
    output = tmp_path / "report_supply.pdf"
    build_report([simple_match_with_supply], output)
    assert output.exists()
    assert output.stat().st_size > 0


def test_build_report_multi_match_includes_comparison(simple_match, tmp_path: Path) -> None:
    m2 = copy.deepcopy(simple_match)
    m2.match_id = "TEST_Q2"
    output = tmp_path / "report_comparison.pdf"
    build_report([simple_match, m2], output)
    assert output.exists()
    assert output.stat().st_size > 0
