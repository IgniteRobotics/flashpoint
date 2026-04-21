from __future__ import annotations

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.figure import Figure

from .models import Match, MotorData

SMOOTH_WINDOW: int = 50   # samples — 1s at 50 Hz
BIN_SECONDS: float = 1.0  # heatmap time bin width in seconds

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


def _smooth(values: np.ndarray, window: int) -> np.ndarray:
    w = min(window, len(values))
    return np.convolve(values, np.ones(w) / w, mode="same")


def _bin_data(values: np.ndarray, n_bins: int) -> np.ndarray:
    chunks = np.array_split(values, n_bins)
    return np.array([c.mean() for c in chunks])


def _trend_line(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Linear trend line y-values fitted over x."""
    coeffs = np.polyfit(x, y, 1)
    return np.polyval(coeffs, x)


def _label(motor_id: str, motor_names: dict[str, str] | None) -> str:
    """Return display name for motor_id, falling back to raw ID if not mapped."""
    return motor_names.get(motor_id, motor_id) if motor_names else motor_id


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


def plot_heatmap(match: Match, metric: str, motor_names: dict[str, str] | None = None) -> Figure:
    """Time × motor heatmap for a metric. Color = global-scaled mean per 1s bin."""
    label, unit = METRIC_LABELS[metric]
    n = len(match.timestamps)
    dt = float(match.timestamps[1] - match.timestamps[0]) if n > 1 else 0.02
    bin_size = max(1, int(round(BIN_SECONDS / dt)))
    n_bins = max(1, n // bin_size)

    motor_ids = list(match.motors.keys())
    rows = []
    for motor_id in motor_ids:
        values = _get(match.motors[motor_id], metric)
        rows.append(_bin_data(values, n_bins) if values is not None else np.zeros(n_bins))

    data = np.array(rows)
    vmax = float(data.max()) if data.max() > 0 else 1.0
    t_bins = np.linspace(match.timestamps[0], match.timestamps[-1], n_bins + 1)

    fig, ax = plt.subplots(figsize=(12, max(3.0, len(motor_ids) * 0.6)))
    mesh = ax.pcolormesh(t_bins, np.arange(len(motor_ids) + 1), data,
                         cmap="plasma", vmin=0, vmax=vmax)
    ax.set_yticks(np.arange(len(motor_ids)) + 0.5)
    ax.set_yticklabels([_label(mid, motor_names) for mid in motor_ids])
    ax.set_xlabel("Time (s)")
    ax.set_title(f"{match.match_id} — {label}")
    fig.colorbar(mesh, ax=ax, label=f"{label} ({unit})")
    fig.tight_layout()
    return fig


def plot_cumulative_energy(match: Match, power_type: str, motor_names: dict[str, str] | None = None) -> Figure:
    """Running cumulative energy curves per motor and robot total."""
    energy_attr = f"{power_type}_energy"
    label = "Motor Energy" if power_type == "motor" else "Supply Energy"
    fig, ax = plt.subplots(figsize=(12, 5))
    for motor_id, data in match.motors.items():
        values = _get(data, energy_attr)
        if values is not None:
            [line] = ax.plot(match.timestamps, values, label=_label(motor_id, motor_names), linewidth=0.8)
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


def plot_per_motor(motor_id: str, data: MotorData, timestamps: np.ndarray, motor_names: dict[str, str] | None = None) -> Figure:
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
    fig.suptitle(_label(motor_id, motor_names))
    fig.tight_layout()
    return fig


def plot_total_power(match: Match) -> Figure:
    """Total robot power — smoothed line with P95 peak scatter markers."""
    fig, ax = plt.subplots(figsize=(12, 5))
    motor_p = _get(match.totals, "motor_power")
    supply_p = _get(match.totals, "supply_power")
    for values, series_label in [
        (motor_p, "Motor Power (total)"),
        (supply_p, "Supply Power (total)"),
    ]:
        if values is None:
            continue
        smoothed = _smooth(values, SMOOTH_WINDOW)
        [line] = ax.plot(match.timestamps, smoothed, label=series_label, linewidth=1.5)
        p95 = float(np.percentile(smoothed, 95))
        mask = smoothed >= p95
        ax.scatter(match.timestamps[mask], smoothed[mask],
                   color=line.get_color(), s=20, marker="x", zorder=6,
                   label=f"{series_label} peaks")
    ax.set_title(f"{match.match_id} — Total Robot Power")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Power (W)")
    ax.legend()
    fig.tight_layout()
    return fig


def plot_comparison(matches: list[Match], metric: str, smooth: bool = False) -> Figure:
    """Overlay robot-total metric across multiple matches."""
    label, unit = METRIC_LABELS[metric]
    fig, ax = plt.subplots(figsize=(12, 5))
    for match in matches:
        values = _get(match.totals, metric)
        if values is not None:
            display = _smooth(values, SMOOTH_WINDOW) if smooth else values
            [line] = ax.plot(match.timestamps, display, label=match.match_id, linewidth=1.2)
            ax.plot(match.timestamps, _trend_line(match.timestamps, display),
                    color=line.get_color(), linewidth=1.8, linestyle="--", alpha=0.9, zorder=5)
    ax.set_title(f"Match Comparison — {label} (Robot Total)")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel(f"{label} ({unit})")
    ax.legend()
    fig.tight_layout()
    return fig
