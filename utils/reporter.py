from __future__ import annotations

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure
from pathlib import Path

from . import names as _names
from . import plotter
from .models import Match


def _cover_page(match_id: str, n_motors: int, duration: float) -> Figure:
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis("off")
    ax.text(0.5, 0.65, "Motor Power Analysis", ha="center", va="center",
            fontsize=20, fontweight="bold", transform=ax.transAxes)
    ax.text(0.5, 0.50, match_id, ha="center", va="center",
            fontsize=16, transform=ax.transAxes)
    ax.text(0.5, 0.35, f"{n_motors} motors  ·  {duration:.1f}s match duration",
            ha="center", va="center", fontsize=12, transform=ax.transAxes)
    fig.tight_layout()
    return fig


def _resolve_names(
    match_id: str,
    names_config: dict[str, dict[str, str]] | None,
) -> dict[str, str] | None:
    if names_config is None:
        return None
    return _names.resolve(match_id, names_config)


def _multi_cover_page(matches: list[Match]) -> Figure:
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis("off")
    ax.text(0.5, 0.80, "Motor Power Analysis", ha="center", va="center",
            fontsize=20, fontweight="bold", transform=ax.transAxes)
    ax.text(0.5, 0.65, f"{len(matches)} matches", ha="center", va="center",
            fontsize=14, transform=ax.transAxes)
    lines = [
        f"{m.match_id}  ·  {len(m.motors)} motors  ·  {float(m.timestamps[-1]):.1f}s"
        for m in matches
    ]
    ax.text(0.5, 0.45, "\n".join(lines), ha="center", va="center",
            fontsize=11, transform=ax.transAxes, linespacing=1.8)
    fig.tight_layout()
    return fig


def _stat_rows_motor(
    match: Match,
    motor_names: dict[str, str] | None = None,
) -> tuple[list[str], list[list[str]]]:
    headers = [
        "Motor",
        "Max Voltage (V)", "Avg Voltage (V)",
        "Max Stator (A)", "Avg Stator (A)",
        "Peak Power (W)", "Avg Power (W)", "σ Power (W)", "P95 Power (W)",
        "Total Energy (Wh)",
    ]
    rows: list[list[str]] = []
    for motor_id, data in list(match.motors.items()) + [("TOTAL", match.totals)]:
        display = motor_id if motor_id == "TOTAL" else (motor_names.get(motor_id, motor_id) if motor_names else motor_id)
        rows.append([
            display,
            f"{data.motor_voltage.max():.2f}",
            f"{data.motor_voltage.mean():.2f}",
            f"{data.stator_current.max():.2f}",
            f"{data.stator_current.mean():.2f}",
            f"{data.motor_power.max():.1f}",
            f"{data.motor_power.mean():.1f}",
            f"{data.motor_power.std():.1f}",
            f"{np.percentile(data.motor_power, 95):.1f}",
            f"{data.motor_energy[-1]:.3f}",
        ])
    return headers, rows


def _stat_rows_supply(
    match: Match,
    motor_names: dict[str, str] | None = None,
) -> tuple[list[str], list[list[str]]]:
    headers = [
        "Motor",
        "Max Voltage (V)", "Avg Voltage (V)",
        "Max Current (A)", "Avg Current (A)",
        "Peak Power (W)", "Avg Power (W)", "σ Power (W)", "P95 Power (W)",
        "Total Energy (Wh)",
    ]
    rows: list[list[str]] = []
    for motor_id, data in list(match.motors.items()) + [("TOTAL", match.totals)]:
        display = motor_id if motor_id == "TOTAL" else (motor_names.get(motor_id, motor_id) if motor_names else motor_id)
        if data.supply_power is None:
            rows.append([display] + ["N/A"] * (len(headers) - 1))
        else:
            rows.append([
                display,
                f"{data.supply_voltage.max():.2f}",
                f"{data.supply_voltage.mean():.2f}",
                f"{data.supply_current.max():.2f}",
                f"{data.supply_current.mean():.2f}",
                f"{data.supply_power.max():.1f}",
                f"{data.supply_power.mean():.1f}",
                f"{data.supply_power.std():.1f}",
                f"{np.percentile(data.supply_power, 95):.1f}",
                f"{data.supply_energy[-1]:.3f}",
            ])
    return headers, rows


def _stat_rows_multi(matches: list[Match]) -> tuple[list[str], list[list[str]]]:
    all_ids = sorted(
        {mid for m in matches for mid in m.motors},
        key=lambda x: int(x.split("-")[1]),
    )
    headers = ["Motor"] + [m.match_id for m in matches]
    rows: list[list[str]] = []
    for motor_id in all_ids + ["TOTAL"]:
        row: list[str] = [motor_id]
        for match in matches:
            data = match.totals if motor_id == "TOTAL" else match.motors.get(motor_id)
            row.append(f"{data.motor_energy[-1]:.3f} Wh" if data else "N/A")
        rows.append(row)
    return headers, rows


def _render_table(headers: list[str], rows: list[list[str]], title: str) -> Figure:
    fig, ax = plt.subplots(figsize=(12, max(4.0, len(rows) * 0.4 + 1.5)))
    ax.axis("off")
    tbl = ax.table(cellText=rows, colLabels=headers, loc="center", cellLoc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.auto_set_column_width(col=list(range(len(headers))))
    ax.set_title(title, fontsize=11, pad=12)
    fig.tight_layout()
    return fig


def build_report(
    matches: list[Match],
    output: Path,
    per_motor: bool = False,
    names_config: dict[str, dict[str, str]] | None = None,
) -> None:
    """Assemble a multi-page PDF report for one or more matches."""
    with PdfPages(output) as pdf:
        for match in matches:
            duration = float(match.timestamps[-1]) if len(match.timestamps) else 0.0

            has_supply = any(m.supply_current is not None for m in match.motors.values())

            summary_figs = [
                _cover_page(match.match_id, len(match.motors), duration),
                _render_table(*_stat_rows_motor(match),
                              f"{match.match_id} — Motor Output (Stator)"),
            ]
            if has_supply:
                summary_figs.append(
                    _render_table(*_stat_rows_supply(match),
                                  f"{match.match_id} — Supply (Battery Draw)")
                )

            for fig in summary_figs + [
                plotter.plot_heatmap(match, "motor_voltage"),
                plotter.plot_heatmap(match, "stator_current"),
                plotter.plot_heatmap(match, "motor_power"),
            ]:
                pdf.savefig(fig)
                plt.close(fig)

            if has_supply:
                for fig in [
                    plotter.plot_heatmap(match, "supply_current"),
                    plotter.plot_heatmap(match, "supply_power"),
                ]:
                    pdf.savefig(fig)
                    plt.close(fig)

            fig = plotter.plot_total_power(match)
            pdf.savefig(fig)
            plt.close(fig)

            fig = plotter.plot_cumulative_energy(match, "motor")
            pdf.savefig(fig)
            plt.close(fig)

            if has_supply:
                fig = plotter.plot_cumulative_energy(match, "supply")
                pdf.savefig(fig)
                plt.close(fig)

            if per_motor:
                for motor_id, data in match.motors.items():
                    fig = plotter.plot_per_motor(motor_id, data, match.timestamps)
                    pdf.savefig(fig)
                    plt.close(fig)

        if len(matches) > 1:
            for fig in [
                plotter.plot_comparison(matches, "motor_energy"),
                _render_table(*_stat_rows_multi(matches), "Match Comparison — Motor Energy (Wh)"),
            ]:
                pdf.savefig(fig)
                plt.close(fig)

            if any(m.totals.supply_energy is not None for m in matches):
                fig = plotter.plot_comparison(matches, "supply_energy")
                pdf.savefig(fig)
                plt.close(fig)
