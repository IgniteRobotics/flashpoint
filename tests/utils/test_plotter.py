from __future__ import annotations

import matplotlib
matplotlib.use("Agg")

import pytest
from matplotlib.figure import Figure

from utils import plotter


def test_plot_instantaneous_motor_voltage_returns_figure(simple_match) -> None:
    fig = plotter.plot_instantaneous(simple_match, "motor_voltage")
    assert isinstance(fig, Figure)


def test_plot_instantaneous_motor_power_returns_figure(simple_match) -> None:
    fig = plotter.plot_instantaneous(simple_match, "motor_power")
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


def test_plot_instantaneous_supply_skips_none_gracefully(simple_match) -> None:
    # simple_match has no supply data — should not raise
    fig = plotter.plot_instantaneous(simple_match, "supply_power")
    assert isinstance(fig, Figure)
