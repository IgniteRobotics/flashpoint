# Supply Current Comparison & Motor Naming Design

**Date:** 2026-04-21
**Branch:** feature/power-tracking

## Features

Two independent additions:

1. Supply current comparison graph in multi-match reports
2. Motor ID → display name config with per-competition overrides

---

## Feature 1: Supply Current Comparison Graph

### Problem

Multi-match reports compare motor energy and supply energy across matches but not supply current, making it hard to see relative battery draw patterns between matches.

### Change

Add one call in `reporter.py`'s multi-match block:

```python
if any(m.totals.supply_current is not None for m in matches):
    fig = plotter.plot_comparison(matches, "supply_current")
    pdf.savefig(fig)
    plt.close(fig)
```

Placed after the existing `supply_energy` comparison. No changes to `plotter.py` — `plot_comparison` already supports `"supply_current"` via `METRIC_LABELS`.

---

## Feature 2: Motor Naming Config

### Problem

Motor IDs (`TalonFX-1`, `TalonFX-2`, …) are meaningless without knowing the robot's wiring. Motor assignments can change between competitions (e.g. a replacement motor gets a different CAN ID).

### Config File Format (TOML)

```toml
[default]
1 = "FL Drive"
2 = "FR Drive"
3 = "BL Drive"
4 = "BR Drive"
5 = "Intake"
6 = "Arm"

[GACMP]
5 = "Intake V2"   # overrides default for GACMP_* matches

[GACOL]
6 = "Arm Pivot"   # overrides default for GACOL_* matches
```

- Section keys are competition prefixes matched against `match_id` (e.g. `GACMP_E5` → `[GACMP]`)
- Motor number is the integer N from `TalonFX-N`
- Competition section merges with `[default]` — only listed motors are overridden; all others inherit default names
- Motors with no entry in config fall back to raw ID (`TalonFX-N`)
- Config file is optional; omitting `--motor-names` leaves all behavior unchanged

### New Module: `utils/names.py`

```python
def load(path: Path) -> dict[str, dict[str, str]]:
    """Parse TOML config. Returns {section: {motor_num_str: display_name}}."""

def resolve(match_id: str, config: dict[str, dict[str, str]]) -> dict[str, str]:
    """Merge default + competition section for match_id.
    Returns {"TalonFX-1": "FL Drive", ...}.
    Competition prefix determined by first section key that is a prefix of match_id.
    """
```

`load()` uses `tomllib` (stdlib in Python 3.11+). No new dependencies.

`resolve()` logic:
1. Start with `{f"TalonFX-{k}": v for k, v in config.get("default", {}).items()}`
2. Find first section key (excluding `"default"`) that `match_id.startswith(key)`
3. Merge competition overrides on top

### CLI Change

`utils/cli.py` gains:

```
--motor-names PATH    TOML config mapping TalonFX IDs to display names (optional)
```

When provided: `names_config = names.load(path)`, then per match: `motor_names = names.resolve(match.match_id, names_config)`. Passed into `reporter.build_report()`.

### Reporter Change

`reporter.build_report()` signature:

```python
def build_report(
    matches: list[Match],
    output: Path,
    per_motor: bool = False,
    motor_names: dict[str, str] | None = None,
) -> None:
```

`motor_names` is passed through to all plotter calls and table-building helpers that display motor IDs.

### Plotter Changes

Functions that display motor IDs gain `motor_names: dict[str, str] | None = None`:

- `plot_heatmap` — Y-axis tick labels
- `plot_cumulative_energy` — legend labels per motor
- `plot_per_motor` — figure suptitle
- `plot_comparison` — legend labels

Helper used at every display point:

```python
def _label(motor_id: str, motor_names: dict[str, str] | None) -> str:
    return motor_names.get(motor_id, motor_id) if motor_names else motor_id
```

`plot_total_power` displays totals only — no motor-level labels — no change needed.

### Reporter Table Changes

`_stat_rows_motor`, `_stat_rows_supply`, `_stat_rows_multi` each gain `motor_names` param and apply `_label()` to the `motor_id` column values. `"TOTAL"` row label is never remapped.

### Testing

- `utils/names.py`: unit tests for `load()` (valid TOML, missing default section, unknown section) and `resolve()` (default only, competition override merges correctly, no match falls back to default, unknown motor falls back to raw ID)
- `test_plotter.py`: existing tests unaffected (no `motor_names` passed → raw IDs used). Add one test per updated function verifying display name appears in figure when `motor_names` provided.
- `test_reporter.py`: verify `build_report` passes `motor_names` through without error.
