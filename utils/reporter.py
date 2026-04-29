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


def _sorted_motor_items(
    match: Match,
    motor_names: dict[str, str] | None,
) -> list[tuple[str, object]]:
    items = list(match.motors.items())
    if motor_names is None:
        return items
    items = [(mid, data) for mid, data in items if mid in motor_names]
    return sorted(items, key=lambda x: _names.func_sort_key(motor_names[x[0]]))


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


def _stat_rows_combined(
    match: Match,
    motor_names: dict[str, str] | None = None,
) -> tuple[list[str], list[list[str]], dict[tuple[int, int], str]]:
    headers = [
        "Motor",
        "Max Motor V", "Avg Motor V",
        "Max Stator A", "Avg Stator A",
        "Peak Motor W", "P95 Motor W",
        "Max Supply V", "Avg Supply V",
        "Max Supply A", "Avg Supply A",
        "Peak Supply W", "P95 Supply W",
        "Avg Temp °C", "Max Temp °C",
    ]
    AVG_TEMP_IDX = 13
    MAX_TEMP_IDX = 14

    motor_items = _sorted_motor_items(match, motor_names)
    rows: list[list[str]] = []
    cell_colors: dict[tuple[int, int], str] = {}

    for motor_id, data in motor_items + [("TOTAL", match.totals)]:
        display = motor_id if motor_id == "TOTAL" else (motor_names.get(motor_id, motor_id) if motor_names else motor_id)
        row_idx = len(rows) + 1  # row 0 is the header

        if data.supply_power is None:
            supply_cols = ["—"] * 6
        else:
            supply_cols = [
                f"{data.supply_voltage.max():.2f}",
                f"{data.supply_voltage.mean():.2f}",
                f"{data.supply_current.max():.2f}",
                f"{data.supply_current.mean():.2f}",
                f"{data.supply_power.max():.1f}",
                f"{np.percentile(data.supply_power, 95):.1f}",
            ]

        is_total = motor_id == "TOTAL"
        if data.device_temp is None or is_total:
            temp_cols = ["—", "—"]
        else:
            avg_temp = float(data.device_temp.mean())
            max_temp = float(data.device_temp.max())
            temp_cols = [f"{avg_temp:.1f}", f"{max_temp:.1f}"]
            if avg_temp > 55.0:
                cell_colors[(row_idx, AVG_TEMP_IDX)] = "#FFCCCC"
            if max_temp > 65.0:
                cell_colors[(row_idx, MAX_TEMP_IDX)] = "#FFCCCC"

        rows.append([
            display,
            f"{data.motor_voltage.max():.2f}",
            f"{data.motor_voltage.mean():.2f}",
            f"{data.stator_current.max():.2f}",
            f"{data.stator_current.mean():.2f}",
            f"{data.motor_power.max():.1f}",
            f"{np.percentile(data.motor_power, 95):.1f}",
            *supply_cols,
            *temp_cols,
        ])

    return headers, rows, cell_colors


def _stat_rows_multi(
    matches: list[Match],
    names_config: dict[str, dict[str, str]] | None = None,
) -> tuple[list[str], list[list[str]]]:
    all_ids_raw = {mid for m in matches for mid in m.motors}
    if names_config is not None:
        canonical: dict[str, str] = {}
        for mid in all_ids_raw:
            for m in matches:
                mn = _resolve_names(m.match_id, names_config)
                if mn and mid in mn:
                    canonical[mid] = mn[mid]
                    break
        all_ids = [mid for mid in all_ids_raw if mid in canonical]
        all_ids = sorted(all_ids, key=lambda mid: _names.func_sort_key(canonical[mid]))
    else:
        canonical = {}
        all_ids = sorted(all_ids_raw, key=lambda x: int(x.split("-")[1]))
    headers = ["Motor"] + [m.match_id for m in matches]
    rows: list[list[str]] = []
    for motor_id in all_ids + ["TOTAL"]:
        display = "TOTAL" if motor_id == "TOTAL" else canonical.get(motor_id, motor_id)
        row: list[str] = [display]
        for match in matches:
            data = match.totals if motor_id == "TOTAL" else match.motors.get(motor_id)
            row.append(f"{data.motor_energy[-1]:.3f} Wh" if data else "N/A")
        rows.append(row)
    return headers, rows


def _render_table(
    headers: list[str],
    rows: list[list[str]],
    title: str,
    cell_colors: dict[tuple[int, int], str] | None = None,
) -> Figure:
    fig, ax = plt.subplots(figsize=(16, max(4.0, len(rows) * 0.4 + 1.5)))
    ax.axis("off")
    tbl = ax.table(cellText=rows, colLabels=headers, loc="center", cellLoc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.auto_set_column_width(col=list(range(len(headers))))
    if cell_colors:
        for (row_idx, col_idx), color in cell_colors.items():
            tbl[row_idx, col_idx].set_facecolor(color)
    ax.set_title(title, fontsize=11, pad=12)
    fig.tight_layout()
    return fig


def _build_single(
    pdf: "PdfPages",
    match: Match,
    per_motor: bool,
    motor_names: dict[str, str] | None,
) -> None:
    has_supply = any(m.supply_current is not None for m in match.motors.values())

    headers, rows, cell_colors = _stat_rows_combined(match, motor_names)
    for fig in [
        _cover_page(match.match_id, len(match.motors), float(match.timestamps[-1])),
        _render_table(headers, rows, f"{match.match_id} — Motor Stats", cell_colors),
    ]:
        pdf.savefig(fig)
        plt.close(fig)

    for fig in [
        plotter.plot_heatmap(match, "motor_voltage", motor_names),
        plotter.plot_heatmap(match, "stator_current", motor_names),
        plotter.plot_heatmap(match, "motor_power", motor_names),
    ]:
        pdf.savefig(fig)
        plt.close(fig)

    if has_supply:
        for fig in [
            plotter.plot_heatmap(match, "supply_current", motor_names),
            plotter.plot_heatmap(match, "supply_power", motor_names),
        ]:
            pdf.savefig(fig)
            plt.close(fig)

    fig = plotter.plot_total_power(match)
    pdf.savefig(fig)
    plt.close(fig)

    fig = plotter.plot_cumulative_energy(match, "motor", motor_names)
    pdf.savefig(fig)
    plt.close(fig)

    if has_supply:
        fig = plotter.plot_cumulative_energy(match, "supply", motor_names)
        pdf.savefig(fig)
        plt.close(fig)

    has_velocity = any(m.rotor_velocity is not None for m in match.motors.values())
    if has_supply and has_velocity:
        qualifying = [
            mid for mid, data in _sorted_motor_items(match, motor_names)
            if data.supply_power is not None and data.rotor_velocity is not None
        ]
        for chunk in [qualifying[i:i + 6] for i in range(0, len(qualifying), 6)]:
            fig = plotter.plot_watts_per_rps(match, motor_names, motor_subset=chunk)
            pdf.savefig(fig)
            plt.close(fig)

    if per_motor:
        for motor_id, data in match.motors.items():
            fig = plotter.plot_per_motor(motor_id, data, match.timestamps, motor_names)
            pdf.savefig(fig)
            plt.close(fig)


def _build_multi(
    pdf: "PdfPages",
    matches: list[Match],
    per_motor: bool,
    names_config: dict[str, dict[str, str]] | None,
) -> None:
    any_supply = any(
        any(m.supply_current is not None for m in match.motors.values())
        for match in matches
    )

    fig = _multi_cover_page(matches)
    pdf.savefig(fig)
    plt.close(fig)

    for match in matches:
        mn = _resolve_names(match.match_id, names_config)
        headers, rows, cell_colors = _stat_rows_combined(match, mn)
        fig = _render_table(headers, rows, f"{match.match_id} — Motor Stats", cell_colors)
        pdf.savefig(fig)
        plt.close(fig)

    for metric in ["motor_voltage", "stator_current", "motor_power"]:
        for match in matches:
            mn = _resolve_names(match.match_id, names_config)
            fig = plotter.plot_heatmap(match, metric, mn)
            pdf.savefig(fig)
            plt.close(fig)

    if any_supply:
        for metric in ["supply_current", "supply_power"]:
            for match in matches:
                if any(m.supply_current is not None for m in match.motors.values()):
                    mn = _resolve_names(match.match_id, names_config)
                    fig = plotter.plot_heatmap(match, metric, mn)
                    pdf.savefig(fig)
                    plt.close(fig)

    for match in matches:
        fig = plotter.plot_total_power(match)
        pdf.savefig(fig)
        plt.close(fig)

    for match in matches:
        mn = _resolve_names(match.match_id, names_config)
        fig = plotter.plot_cumulative_energy(match, "motor", mn)
        pdf.savefig(fig)
        plt.close(fig)

    if any_supply:
        for match in matches:
            if match.totals.supply_energy is not None:
                mn = _resolve_names(match.match_id, names_config)
                fig = plotter.plot_cumulative_energy(match, "supply", mn)
                pdf.savefig(fig)
                plt.close(fig)

    if per_motor:
        for match in matches:
            mn = _resolve_names(match.match_id, names_config)
            for motor_id, data in match.motors.items():
                fig = plotter.plot_per_motor(motor_id, data, match.timestamps, mn)
                pdf.savefig(fig)
                plt.close(fig)

    for fig in [
        plotter.plot_comparison(matches, "motor_energy"),
        _render_table(*_stat_rows_multi(matches, names_config), "Match Comparison — Motor Energy (Wh)"),
    ]:
        pdf.savefig(fig)
        plt.close(fig)

    if any(m.totals.supply_energy is not None for m in matches):
        fig = plotter.plot_comparison(matches, "supply_energy")
        pdf.savefig(fig)
        plt.close(fig)

    if any(m.totals.supply_current is not None for m in matches):
        fig = plotter.plot_comparison(matches, "supply_current", smooth=True, show_peaks=True)
        pdf.savefig(fig)
        plt.close(fig)

    for match in matches:
        mn = _resolve_names(match.match_id, names_config)
        has_velocity = any(m.rotor_velocity is not None for m in match.motors.values())
        has_supply = any(m.supply_current is not None for m in match.motors.values())
        if has_supply and has_velocity:
            qualifying = [
                mid for mid, data in _sorted_motor_items(match, mn)
                if data.supply_power is not None and data.rotor_velocity is not None
            ]
            for chunk in [qualifying[i:i + 6] for i in range(0, len(qualifying), 6)]:
                fig = plotter.plot_watts_per_rps(match, mn, motor_subset=chunk)
                pdf.savefig(fig)
                plt.close(fig)


def build_report(
    matches: list[Match],
    output: Path,
    per_motor: bool = False,
    names_config: dict[str, dict[str, str]] | None = None,
) -> None:
    """Assemble a multi-page PDF report for one or more matches."""
    with PdfPages(output) as pdf:
        if len(matches) == 1:
            _build_single(pdf, matches[0], per_motor, _resolve_names(matches[0].match_id, names_config))
        else:
            _build_multi(pdf, matches, per_motor, names_config)
