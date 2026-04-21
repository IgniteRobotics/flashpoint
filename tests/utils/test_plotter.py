from __future__ import annotations

import matplotlib
matplotlib.use("Agg")

import pytest
from matplotlib.figure import Figure

from utils import plotter


def test_plot_heatmap_motor_power_returns_figure(simple_match) -> None:
    fig = plotter.plot_heatmap(simple_match, "motor_power")
    assert isinstance(fig, Figure)


def test_plot_heatmap_motor_voltage_returns_figure(simple_match) -> None:
    fig = plotter.plot_heatmap(simple_match, "motor_voltage")
    assert isinstance(fig, Figure)


def test_plot_heatmap_supply_skips_none_gracefully(simple_match) -> None:
    # simple_match has no supply data — should not raise
    fig = plotter.plot_heatmap(simple_match, "supply_power")
    assert isinstance(fig, Figure)


def test_plot_cumulative_energy_motor_returns_figure(simple_match) -> None:
    fig = plotter.plot_cumulative_energy(simple_match, "motor")
    assert isinstance(fig, Figure)


def test_plot_cumulative_energy_supply_returns_figure(simple_match_with_supply) -> None:
    fig = plotter.plot_cumulative_energy(simple_match_with_supply, "supply")
    assert isinstance(fig, Figure)


def test_plot_per_motor_returns_figure(simple_match) -> None:
    motor_id = list(simple_match.motors.keys())[0]
    data = simple_match.motors[motor_id]
    fig = plotter.plot_per_motor(motor_id, data, simple_match.timestamps)
    assert isinstance(fig, Figure)


def test_plot_comparison_two_matches_returns_figure(simple_match) -> None:
    import copy
    m2 = copy.deepcopy(simple_match)
    m2.match_id = "TEST_Q2"
    fig = plotter.plot_comparison([simple_match, m2], "motor_energy")
    assert isinstance(fig, Figure)


def test_plot_total_power_returns_figure(simple_match) -> None:
    fig = plotter.plot_total_power(simple_match)
    assert isinstance(fig, Figure)


def test_plot_total_power_with_supply_returns_figure(simple_match_with_supply) -> None:
    fig = plotter.plot_total_power(simple_match_with_supply)
    assert isinstance(fig, Figure)


def test_plot_total_power_has_peak_scatter(simple_match) -> None:
    fig = plotter.plot_total_power(simple_match)
    ax = fig.axes[0]
    assert len(ax.collections) > 0


def test_plot_total_power_motor_only_has_one_line(simple_match) -> None:
    fig = plotter.plot_total_power(simple_match)
    ax = fig.axes[0]
    assert len(ax.lines) == 1


def test_plot_heatmap_uses_motor_names_on_y_axis(simple_match) -> None:
    motor_names = {"TalonFX-1": "FL Drive", "TalonFX-2": "FR Drive"}
    fig = plotter.plot_heatmap(simple_match, "motor_power", motor_names=motor_names)
    ax = fig.axes[0]
    tick_labels = [t.get_text() for t in ax.get_yticklabels()]
    assert "FL Drive" in tick_labels
    assert "FR Drive" in tick_labels
