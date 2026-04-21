# Supply Current Comparison, Motor Naming & Interleaved Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add supply current comparison graphs, a TOML-based motor naming config, and interleaved multi-match page ordering to the PDF reporter.

**Architecture:** New `utils/names.py` handles TOML loading and per-match name resolution. Plotter functions gain an optional `motor_names` dict for display-time label substitution. Reporter's `build_report` takes a raw `names_config` dict, resolves names per match internally, and restructures the multi-match path to emit pages grouped by content type rather than by match.

**Tech Stack:** Python 3.11+, `tomllib` (stdlib), `matplotlib`, `pytest`

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `utils/names.py` | Create | TOML load + per-match name resolution |
| `tests/utils/test_names.py` | Create | Unit tests for names module |
| `utils/plotter.py` | Modify | `_label` helper; `motor_names` param on `plot_heatmap`, `plot_cumulative_energy`, `plot_per_motor` |
| `tests/utils/test_plotter.py` | Modify | Tests for motor_names display in plotter functions |
| `utils/reporter.py` | Modify | `_multi_cover_page`; motor_names in stat table helpers; `build_report` restructure; supply current comparison |
| `tests/utils/test_reporter.py` | Modify | Two-match smoke test with motor_names; supply current guard |
| `utils/cli.py` | Modify | `--motor-names PATH` flag |
| `tests/utils/test_cli.py` | Modify | Smoke test for `--motor-names` flag |

---

## Task 1: `utils/names.py` — Motor name loading and resolution

**Files:**
- Create: `utils/names.py`
- Create: `tests/utils/test_names.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/utils/test_names.py`:

```python
from __future__ import annotations

import pytest
from pathlib import Path

from utils import names


TOML_CONTENT = """\
[default]
1 = "FL Drive"
2 = "FR Drive"
5 = "Intake"

[GACMP]
5 = "Intake V2"

[GACOL]
5 = "Intake V3"
2 = "Arm Pivot"
"""


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    f = tmp_path / "motor-names.toml"
    f.write_text(TOML_CONTENT)
    return f


def test_load_returns_dict_with_sections(config_file: Path) -> None:
    config = names.load(config_file)
    assert "default" in config
    assert "GACMP" in config
    assert "GACOL" in config


def test_load_default_section_values(config_file: Path) -> None:
    config = names.load(config_file)
    assert config["default"]["1"] == "FL Drive"
    assert config["default"]["5"] == "Intake"


def test_resolve_default_only(config_file: Path) -> None:
    config = names.load(config_file)
    result = names.resolve("UNKNOWN_Q1", config)
    assert result == {
        "TalonFX-1": "FL Drive",
        "TalonFX-2": "FR Drive",
        "TalonFX-5": "Intake",
    }


def test_resolve_competition_overrides_merge_with_default(config_file: Path) -> None:
    config = names.load(config_file)
    result = names.resolve("GACMP_E5", config)
    assert result["TalonFX-1"] == "FL Drive"   # from default
    assert result["TalonFX-2"] == "FR Drive"   # from default
    assert result["TalonFX-5"] == "Intake V2"  # overridden by GACMP


def test_resolve_different_competition(config_file: Path) -> None:
    config = names.load(config_file)
    result = names.resolve("GACOL_E8", config)
    assert result["TalonFX-5"] == "Intake V3"
    assert result["TalonFX-2"] == "Arm Pivot"
    assert result["TalonFX-1"] == "FL Drive"   # from default


def test_resolve_unknown_motor_not_in_result(config_file: Path) -> None:
    config = names.load(config_file)
    result = names.resolve("GACMP_E5", config)
    assert "TalonFX-99" not in result


def test_resolve_empty_config() -> None:
    result = names.resolve("GACMP_Q1", {})
    assert result == {}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/utils/test_names.py -v
```

Expected: `ModuleNotFoundError: No module named 'utils.names'`

- [ ] **Step 3: Implement `utils/names.py`**

```python
from __future__ import annotations

import tomllib
from pathlib import Path


def load(path: Path) -> dict[str, dict[str, str]]:
    with open(path, "rb") as f:
        return tomllib.load(f)


def resolve(match_id: str, config: dict[str, dict[str, str]]) -> dict[str, str]:
    result = {
        f"TalonFX-{k}": v
        for k, v in config.get("default", {}).items()
    }
    for key, overrides in config.items():
        if key != "default" and match_id.startswith(key):
            result.update({f"TalonFX-{k}": v for k, v in overrides.items()})
            break
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/utils/test_names.py -v
```

Expected: 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add utils/names.py tests/utils/test_names.py
git commit -m "feat: add motor name loading and per-match resolution"
```

---

## Task 2: `_label` helper + `plot_heatmap` motor_names

**Files:**
- Modify: `utils/plotter.py`
- Modify: `tests/utils/test_plotter.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/utils/test_plotter.py`:

```python
def test_plot_heatmap_uses_motor_names_on_y_axis(simple_match) -> None:
    motor_names = {"TalonFX-1": "FL Drive", "TalonFX-2": "FR Drive"}
    fig = plotter.plot_heatmap(simple_match, "motor_power", motor_names=motor_names)
    ax = fig.axes[0]
    tick_labels = [t.get_text() for t in ax.get_yticklabels()]
    assert "FL Drive" in tick_labels
    assert "FR Drive" in tick_labels
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/utils/test_plotter.py::test_plot_heatmap_uses_motor_names_on_y_axis -v
```

Expected: FAIL — `plot_heatmap() got an unexpected keyword argument 'motor_names'`

- [ ] **Step 3: Add `_label` helper and update `plot_heatmap`**

Add `_label` after `_trend_line` in `utils/plotter.py`:

```python
def _label(motor_id: str, motor_names: dict[str, str] | None) -> str:
    return motor_names.get(motor_id, motor_id) if motor_names else motor_id
```

Update `plot_heatmap` signature and Y-tick labels:

```python
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
```

- [ ] **Step 4: Run all plotter tests**

```bash
python -m pytest tests/utils/test_plotter.py -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add utils/plotter.py tests/utils/test_plotter.py
git commit -m "feat: add _label helper and motor_names to plot_heatmap"
```

---

## Task 3: `plot_cumulative_energy` and `plot_per_motor` motor_names

**Files:**
- Modify: `utils/plotter.py`
- Modify: `tests/utils/test_plotter.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/utils/test_plotter.py`:

```python
def test_plot_cumulative_energy_uses_motor_names_in_legend(simple_match) -> None:
    motor_names = {"TalonFX-1": "FL Drive", "TalonFX-2": "FR Drive"}
    fig = plotter.plot_cumulative_energy(simple_match, "motor", motor_names=motor_names)
    ax = fig.axes[0]
    legend_texts = [t.get_text() for t in ax.get_legend().get_texts()]
    assert any("FL Drive" in t for t in legend_texts)


def test_plot_per_motor_uses_motor_name_in_title(simple_match) -> None:
    motor_names = {"TalonFX-1": "FL Drive"}
    motor_id = "TalonFX-1"
    data = simple_match.motors[motor_id]
    fig = plotter.plot_per_motor(motor_id, data, simple_match.timestamps, motor_names=motor_names)
    assert fig._suptitle.get_text() == "FL Drive"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/utils/test_plotter.py::test_plot_cumulative_energy_uses_motor_names_in_legend tests/utils/test_plotter.py::test_plot_per_motor_uses_motor_name_in_title -v
```

Expected: FAIL — `got an unexpected keyword argument 'motor_names'`

- [ ] **Step 3: Update `plot_cumulative_energy` and `plot_per_motor`**

Replace `plot_cumulative_energy` in `utils/plotter.py`:

```python
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
```

Replace `plot_per_motor` in `utils/plotter.py`:

```python
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
```

- [ ] **Step 4: Run all plotter tests**

```bash
python -m pytest tests/utils/test_plotter.py -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add utils/plotter.py tests/utils/test_plotter.py
git commit -m "feat: add motor_names to plot_cumulative_energy and plot_per_motor"
```

---

## Task 4: Reporter helpers — stat tables with motor_names, `_multi_cover_page`, `_resolve_names`

**Files:**
- Modify: `utils/reporter.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/utils/test_reporter.py`:

```python
def test_build_report_with_motor_names(simple_match, tmp_path: Path) -> None:
    names_config = {"default": {"1": "FL Drive", "2": "FR Drive"}}
    output = tmp_path / "report_named.pdf"
    build_report([simple_match], output, names_config=names_config)
    assert output.exists()
    assert output.stat().st_size > 0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/utils/test_reporter.py::test_build_report_with_motor_names -v
```

Expected: FAIL — `build_report() got an unexpected keyword argument 'names_config'`

- [ ] **Step 3: Add helpers to `utils/reporter.py`**

Add import at top of `utils/reporter.py`:

```python
from . import names as _names
```

Add `_resolve_names` and updated stat table helpers. Replace the existing `_stat_rows_motor`, `_stat_rows_supply`, and `_stat_rows_multi` functions, and add `_multi_cover_page` and `_resolve_names`:

```python
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
```

Also update `build_report` signature (implementation in Task 5):

```python
def build_report(
    matches: list[Match],
    output: Path,
    per_motor: bool = False,
    names_config: dict[str, dict[str, str]] | None = None,
) -> None:
```

Keep the existing body for now — just add the `names_config` param with `None` default so the test passes.

- [ ] **Step 4: Run reporter tests**

```bash
python -m pytest tests/utils/test_reporter.py -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add utils/reporter.py tests/utils/test_reporter.py
git commit -m "feat: add _multi_cover_page, _resolve_names, motor_names to stat table helpers"
```

---

## Task 5: `build_report` — interleaved multi-match layout + supply current comparison

**Files:**
- Modify: `utils/reporter.py`
- Modify: `tests/utils/test_reporter.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/utils/test_reporter.py`:

```python
def test_build_report_two_matches_interleaved(simple_match, tmp_path: Path) -> None:
    import copy
    m2 = copy.deepcopy(simple_match)
    m2.match_id = "TEST_Q2"
    output = tmp_path / "report_interleaved.pdf"
    build_report([simple_match, m2], output)
    assert output.exists()
    assert output.stat().st_size > 0


def test_build_report_supply_current_comparison_emitted(simple_match_with_supply, tmp_path: Path) -> None:
    import copy
    m2 = copy.deepcopy(simple_match_with_supply)
    m2.match_id = "TEST_Q2_SUPPLY"
    output = tmp_path / "report_supply_comparison.pdf"
    build_report([simple_match_with_supply, m2], output)
    assert output.exists()
    assert output.stat().st_size > 0
```

- [ ] **Step 2: Run tests**

```bash
python -m pytest tests/utils/test_reporter.py::test_build_report_two_matches_interleaved tests/utils/test_reporter.py::test_build_report_supply_current_comparison_emitted -v
```

These are smoke tests — they may already PASS with the current code since they only verify a PDF is produced. That's fine. The interleaved page order and supply current graph are verified in Task 7's integration step. Proceed to Step 3 regardless of result.

- [ ] **Step 3: Replace `build_report` body in `utils/reporter.py`**

```python
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


def _build_single(
    pdf: "PdfPages",
    match: Match,
    per_motor: bool,
    motor_names: dict[str, str] | None,
) -> None:
    has_supply = any(m.supply_current is not None for m in match.motors.values())

    for fig in [
        _cover_page(match.match_id, len(match.motors), float(match.timestamps[-1])),
        _render_table(*_stat_rows_motor(match, motor_names),
                      f"{match.match_id} — Motor Output (Stator)"),
    ]:
        pdf.savefig(fig)
        plt.close(fig)

    if has_supply:
        fig = _render_table(*_stat_rows_supply(match, motor_names),
                            f"{match.match_id} — Supply (Battery Draw)")
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

    # shared cover
    fig = _multi_cover_page(matches)
    pdf.savefig(fig)
    plt.close(fig)

    # stat tables — all matches, motor then supply
    for match in matches:
        mn = _resolve_names(match.match_id, names_config)
        fig = _render_table(*_stat_rows_motor(match, mn),
                            f"{match.match_id} — Motor Output (Stator)")
        pdf.savefig(fig)
        plt.close(fig)

    if any_supply:
        for match in matches:
            mn = _resolve_names(match.match_id, names_config)
            fig = _render_table(*_stat_rows_supply(match, mn),
                                f"{match.match_id} — Supply (Battery Draw)")
            pdf.savefig(fig)
            plt.close(fig)

    # motor heatmaps
    for metric in ["motor_voltage", "stator_current", "motor_power"]:
        for match in matches:
            mn = _resolve_names(match.match_id, names_config)
            fig = plotter.plot_heatmap(match, metric, mn)
            pdf.savefig(fig)
            plt.close(fig)

    # supply heatmaps
    if any_supply:
        for metric in ["supply_current", "supply_power"]:
            for match in matches:
                if any(m.supply_current is not None for m in match.motors.values()):
                    mn = _resolve_names(match.match_id, names_config)
                    fig = plotter.plot_heatmap(match, metric, mn)
                    pdf.savefig(fig)
                    plt.close(fig)

    # total power
    for match in matches:
        fig = plotter.plot_total_power(match)
        pdf.savefig(fig)
        plt.close(fig)

    # cumulative energy
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

    # per-motor pages
    if per_motor:
        for match in matches:
            mn = _resolve_names(match.match_id, names_config)
            for motor_id, data in match.motors.items():
                fig = plotter.plot_per_motor(motor_id, data, match.timestamps, mn)
                pdf.savefig(fig)
                plt.close(fig)

    # comparison section
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

    if any(m.totals.supply_current is not None for m in matches):
        fig = plotter.plot_comparison(matches, "supply_current")
        pdf.savefig(fig)
        plt.close(fig)
```

Note: remove the old `build_report` body (the `for match in matches` loop and the `if len(matches) > 1` block at the end). Keep `_cover_page` and `_render_table` — they're still used.

- [ ] **Step 4: Run all reporter tests**

```bash
python -m pytest tests/utils/test_reporter.py -v
```

Expected: all PASS

- [ ] **Step 5: Run full test suite**

```bash
python -m pytest -q
```

Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add utils/reporter.py tests/utils/test_reporter.py
git commit -m "feat: interleaved multi-match layout, shared cover, supply current comparison"
```

---

## Task 6: CLI `--motor-names` flag

**Files:**
- Modify: `utils/cli.py`
- Modify: `tests/utils/test_cli.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/utils/test_cli.py` (uses `subprocess.run` like the existing tests; `RIO_CSV`, `CARNIVORE_CSV`, `subprocess`, and `sys` are already imported at the top of that file):

```python
def test_cli_motor_names_flag_accepted(tmp_path: Path) -> None:
    if not RIO_CSV.exists():
        import pytest
        pytest.skip("real data files not present")
    config = tmp_path / "names.toml"
    config.write_text('[default]\n1 = "FL Drive"\n2 = "FR Drive"\n')
    output = tmp_path / "report_named.pdf"
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(RIO_CSV), str(CARNIVORE_CSV),
         "--output", str(output), "--motor-names", str(config)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.exists()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/utils/test_cli.py::test_cli_motor_names_flag_accepted -v
```

Expected: FAIL — `No such option: --motor-names`

- [ ] **Step 3: Update `utils/cli.py`**

Add import at top:

```python
from . import names as _names
```

Add argument after the existing `--threshold` arg in `main()`:

```python
parser.add_argument(
    "--motor-names", type=Path, default=None, metavar="PATH",
    help="TOML config mapping TalonFX IDs to display names (optional)",
)
```

Load config and pass to `build_report`:

```python
names_config = _names.load(args.motor_names) if args.motor_names else None

# ...existing match-loading loop...

reporter.build_report(matches, args.output, per_motor=args.per_motor_graphs, names_config=names_config)
```

- [ ] **Step 4: Run all tests**

```bash
python -m pytest -q
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add utils/cli.py tests/utils/test_cli.py
git commit -m "feat: add --motor-names CLI flag for TOML display name config"
```

---

## Task 7: Integration smoke test on real data

- [ ] **Step 1: Create a sample motor names config**

```bash
cat > data/motor-names.toml << 'EOF'
[default]
1 = "FL Drive"
2 = "FR Drive"
3 = "BL Drive"
4 = "BR Drive"

[GACMP]
5 = "Intake"

[GACOL]
5 = "Shooter"
EOF
```

- [ ] **Step 2: Run on both matches with naming**

```bash
python -m utils \
  data/GACMP_E5_rio_filtered.csv \
  data/GACMP_E5_6E9415C3394C485320202050101C18FF_filtered.csv \
  data/GACOL_E8_rio_filtered.csv \
  data/GACOL_E8_6E9415C3394C485320202050101C18FF_filtered.csv \
  --motor-names data/motor-names.toml \
  --output report.pdf
```

Expected output:
```
Loading GACMP_E5 (2 file(s))...
  18 motors · 165.1s match duration
Loading GACOL_E8 (2 file(s))...
  18 motors · 165.1s match duration
Building report → report.pdf
Done.
```

- [ ] **Step 3: Open and verify**

```bash
open report.pdf
```

Verify:
- Single shared cover page lists both matches
- Pages interleave by content type (both motor voltage heatmaps before stator current heatmaps, etc.)
- Named motors show display names on heatmap Y-axis and energy legend
- Supply current comparison graph appears in comparison section
- Comparison section is last

- [ ] **Step 4: Commit motor names sample config**

```bash
git add data/motor-names.toml
git commit -m "chore: add sample motor-names.toml for testing"
```
