from __future__ import annotations

import matplotlib
matplotlib.use("Agg")

from pathlib import Path
import copy

import numpy as np
import pytest

from utils.models import Match, MotorData
from utils.reporter import _render_table, _stat_rows_combined, build_report


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


def test_build_report_with_motor_names(simple_match, tmp_path: Path) -> None:
    names_config = {"default": {"1": "FL Drive", "2": "FR Drive"}}
    output = tmp_path / "report_named.pdf"
    build_report([simple_match], output, names_config=names_config)
    assert output.exists()
    assert output.stat().st_size > 0


def test_build_report_two_matches_interleaved(simple_match, tmp_path: Path) -> None:
    m2 = copy.deepcopy(simple_match)
    m2.match_id = "TEST_Q2"
    output = tmp_path / "report_interleaved.pdf"
    build_report([simple_match, m2], output)
    assert output.exists()
    assert output.stat().st_size > 0


def test_build_report_supply_current_comparison_emitted(simple_match_with_supply, tmp_path: Path) -> None:
    m2 = copy.deepcopy(simple_match_with_supply)
    m2.match_id = "TEST_Q2_SUPPLY"
    output = tmp_path / "report_supply_comparison.pdf"
    build_report([simple_match_with_supply, m2], output)
    assert output.exists()
    assert output.stat().st_size > 0


def _make_hot_match(avg_temp: float, max_temp_spike: float | None = None) -> Match:
    n = 10
    t = np.arange(n) * 0.02
    temps = np.ones(n) * avg_temp
    if max_temp_spike is not None:
        temps[-1] = max_temp_spike
    data = MotorData(
        motor_voltage=np.ones(n) * 5.0,
        stator_current=np.ones(n) * 20.0,
        motor_power=np.ones(n) * 100.0,
        motor_energy=np.zeros(n),
        device_temp=temps,
    )
    totals = MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=np.ones(n) * 100.0,
        motor_energy=np.zeros(n),
    )
    return Match(match_id="T", timestamps=t, motors={"TalonFX-1": data}, totals=totals)


def test_stat_rows_combined_returns_15_column_headers(simple_match: Match) -> None:
    headers, _, _ = _stat_rows_combined(simple_match)
    assert len(headers) == 15


def test_stat_rows_combined_supply_cols_dash_when_no_supply(simple_match: Match) -> None:
    _, rows, _ = _stat_rows_combined(simple_match)
    motor_row = rows[0]
    # Supply cols are indices 7–12
    assert all(v == "—" for v in motor_row[7:13])


def test_stat_rows_combined_temp_cols_dash_when_no_temp(simple_match: Match) -> None:
    _, rows, _ = _stat_rows_combined(simple_match)
    motor_row = rows[0]
    assert motor_row[13] == "—"
    assert motor_row[14] == "—"


def test_stat_rows_combined_flags_avg_temp_above_55() -> None:
    match = _make_hot_match(avg_temp=60.0)
    _, _, cell_colors = _stat_rows_combined(match)
    assert (1, 13) in cell_colors
    assert cell_colors[(1, 13)] == "#FFCCCC"


def test_stat_rows_combined_does_not_flag_avg_temp_at_55() -> None:
    match = _make_hot_match(avg_temp=55.0)
    _, _, cell_colors = _stat_rows_combined(match)
    assert (1, 13) not in cell_colors


def test_stat_rows_combined_flags_max_temp_above_65() -> None:
    # avg < 55, spike pushes max > 65
    match = _make_hot_match(avg_temp=40.0, max_temp_spike=70.0)
    _, _, cell_colors = _stat_rows_combined(match)
    assert (1, 14) in cell_colors
    assert (1, 13) not in cell_colors


def test_stat_rows_combined_total_row_temp_always_dash() -> None:
    match = _make_hot_match(avg_temp=60.0)
    _, rows, _ = _stat_rows_combined(match)
    total_row = rows[-1]
    assert total_row[13] == "—"
    assert total_row[14] == "—"


def test_render_table_applies_cell_color() -> None:
    fig = _render_table(["A", "B"], [["x", "y"]], "Test", {(1, 1): "#FFCCCC"})
    import matplotlib.pyplot as plt
    plt.close(fig)


def test_build_report_with_temp_data(tmp_path: Path) -> None:
    match = _make_hot_match(avg_temp=60.0)
    output = tmp_path / "report_temp.pdf"
    build_report([match], output)
    assert output.exists()
    assert output.stat().st_size > 0
