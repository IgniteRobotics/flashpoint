from __future__ import annotations

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.figure import Figure

from .models import Match, MotorData

METRIC_LABELS: dict[str, tuple[str, str]] = {
    "motor_voltage": ("Motor Voltage", "V"),
    "stator_current": ("Stator Current", "A"),
    "motor_power": ("Motor Power", "W"),
    "supply_voltage": ("Supply Voltage", "V"),
    "supply_current": ("Supply Current", "A"),
    "supply_power": ("Supply Power", "W"),
    "motor_energy": ("Cumulative Motor Energy", "Wh"),
    "supply_energy": ("Cumulative Supply Energy", "Wh"),
}


def _get(data: MotorData, metric: str) -> np.ndarray | None:
    return getattr(data, metric, None)


def _trend_line(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Linear trend line y-values fitted over x."""
    coeffs = np.polyfit(x, y, 1)
    return np.polyval(coeffs, x)


def plot_instantaneous(match: Match, metric: str) -> Figure:
    """Time-series line plot of a metric for all motors."""
    label, unit = METRIC_LABELS[metric]
    fig, ax = plt.subplots(figsize=(12, 5))
    for motor_id, data in match.motors.items():
        values = _get(data, metric)
        if values is not None:
            [line] = ax.plot(match.timestamps, values, label=motor_id, linewidth=0.8)
            ax.plot(match.timestamps, _trend_line(match.timestamps, values),
                    color=line.get_color(), linewidth=1.5, linestyle="--", alpha=0.9, zorder=5)
    ax.set_title(f"{match.match_id} — {label}")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel(f"{label} ({unit})")
    ax.legend(fontsize=7, ncol=4)
    fig.tight_layout()
    return fig


def plot_cumulative_energy(match: Match, power_type: str) -> Figure:
    """Running cumulative energy curves per motor and robot total."""
    energy_attr = f"{power_type}_energy"
    label = "Motor Energy" if power_type == "motor" else "Supply Energy"
    fig, ax = plt.subplots(figsize=(12, 5))
    for motor_id, data in match.motors.items():
        values = _get(data, energy_attr)
        if values is not None:
            [line] = ax.plot(match.timestamps, values, label=motor_id, linewidth=0.8)
            ax.plot(match.timestamps, _trend_line(match.timestamps, values),
                    color=line.get_color(), linewidth=1.5, linestyle="--", alpha=0.9, zorder=5)
    total = _get(match.totals, energy_attr)
    if total is not None:
        ax.plot(match.timestamps, total, label="TOTAL", linewidth=2,
                color="black", linestyle="--")
        ax.plot(match.timestamps, _trend_line(match.timestamps, total),
                color="black", linewidth=1.8, linestyle=":", alpha=0.9, zorder=5)
    ax.set_title(f"{match.match_id} — Cumulative {label}")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Energy (Wh)")
    ax.legend(fontsize=7, ncol=4)
    fig.tight_layout()
    return fig


def plot_per_motor(motor_id: str, data: MotorData, timestamps: np.ndarray) -> Figure:
    """All available metrics for one motor on a single figure."""
    candidates = [
        ("motor_voltage", "Motor Voltage", "V"),
        ("stator_current", "Stator Current", "A"),
        ("motor_power", "Motor Power", "W"),
        ("supply_voltage", "Supply Voltage", "V"),
        ("supply_current", "Supply Current", "A"),
        ("supply_power", "Supply Power", "W"),
    ]
    available = [(attr, lbl, unit) for attr, lbl, unit in candidates
                 if _get(data, attr) is not None]
    n = len(available)
    fig, axes = plt.subplots(n, 1, figsize=(12, 3 * n), sharex=True)
    if n == 1:
        axes = [axes]
    for ax, (attr, lbl, unit) in zip(axes, available):
        values = _get(data, attr)
        [line] = ax.plot(timestamps, values, linewidth=0.8)
        ax.plot(timestamps, _trend_line(timestamps, values),
                color=line.get_color(), linewidth=1.5, linestyle="--", alpha=0.9, zorder=5)
        ax.set_ylabel(f"{lbl} ({unit})")
    axes[-1].set_xlabel("Time (s)")
    fig.suptitle(motor_id)
    fig.tight_layout()
    return fig


def plot_total_power(match: Match) -> Figure:
    """Total robot supply power and motor power on the same axes."""
    fig, ax = plt.subplots(figsize=(12, 5))
    motor_p = _get(match.totals, "motor_power")
    supply_p = _get(match.totals, "supply_power")
    if motor_p is not None:
        [line] = ax.plot(match.timestamps, motor_p, label="Motor Power (total)", linewidth=1.2)
        ax.plot(match.timestamps, _trend_line(match.timestamps, motor_p),
                color=line.get_color(), linewidth=1.8, linestyle="--", alpha=0.9, zorder=5)
    if supply_p is not None:
        [line] = ax.plot(match.timestamps, supply_p, label="Supply Power (total)", linewidth=1.2)
        ax.plot(match.timestamps, _trend_line(match.timestamps, supply_p),
                color=line.get_color(), linewidth=1.8, linestyle="--", alpha=0.9, zorder=5)
    ax.set_title(f"{match.match_id} — Total Robot Power")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Power (W)")
    ax.legend()
    fig.tight_layout()
    return fig


def plot_comparison(matches: list[Match], metric: str) -> Figure:
    """Overlay robot-total metric across multiple matches."""
    label, unit = METRIC_LABELS[metric]
    fig, ax = plt.subplots(figsize=(12, 5))
    for match in matches:
        values = _get(match.totals, metric)
        if values is not None:
            [line] = ax.plot(match.timestamps, values, label=match.match_id, linewidth=1.2)
            ax.plot(match.timestamps, _trend_line(match.timestamps, values),
                    color=line.get_color(), linewidth=1.8, linestyle="--", alpha=0.9, zorder=5)
    ax.set_title(f"Match Comparison — {label} (Robot Total)")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel(f"{label} ({unit})")
    ax.legend()
    fig.tight_layout()
    return fig
