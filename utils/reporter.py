from __future__ import annotations

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure
from pathlib import Path

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


def _stat_rows_single(match: Match) -> tuple[list[str], list[list[str]]]:
    headers = [
        "Motor", "Max V (V)", "Avg V (V)", "Max I (A)", "Avg I (A)",
        "Peak Pwr (W)", "Total Motor (Wh)", "Total Supply (Wh)",
    ]
    rows: list[list[str]] = []
    entries = list(match.motors.items()) + [("TOTAL", match.totals)]
    for motor_id, data in entries:
        supply_wh = (f"{data.supply_energy[-1]:.3f}"
                     if data.supply_energy is not None else "N/A")
        rows.append([
            motor_id,
            f"{data.motor_voltage.max():.2f}",
            f"{data.motor_voltage.mean():.2f}",
            f"{data.stator_current.max():.2f}",
            f"{data.stator_current.mean():.2f}",
            f"{data.motor_power.max():.1f}",
            f"{data.motor_energy[-1]:.3f}",
            supply_wh,
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
) -> None:
    """Assemble a multi-page PDF report for one or more matches."""
    with PdfPages(output) as pdf:
        for match in matches:
            duration = float(match.timestamps[-1]) if len(match.timestamps) else 0.0

            for fig in [
                _cover_page(match.match_id, len(match.motors), duration),
                _render_table(*_stat_rows_single(match), f"{match.match_id} — Summary"),
                plotter.plot_instantaneous(match, "motor_voltage"),
                plotter.plot_instantaneous(match, "stator_current"),
                plotter.plot_instantaneous(match, "motor_power"),
            ]:
                pdf.savefig(fig)
                plt.close(fig)

            has_supply = any(m.supply_current is not None for m in match.motors.values())
            if has_supply:
                for fig in [
                    plotter.plot_instantaneous(match, "supply_current"),
                    plotter.plot_instantaneous(match, "supply_power"),
                ]:
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
