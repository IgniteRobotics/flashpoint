# Motor Power Analysis Utilities — Design Spec

**Date:** 2026-04-15  
**Branch:** feature/power-tracking  
**Status:** Approved

---

## Overview

A Python utils package that parses FRC robot motor telemetry CSV files, trims them to match time, computes power consumption metrics, and generates a PDF report with graphs and summary statistics. Supports comparing two or more matches.

---

## Input Data

### File Format

CSV files exported from Phoenix 6 / WPILib data logging. Two files per match (one per CAN bus), e.g.:

```
GACMP_E5-rio.csv
GACMP_E5_a1b2c3d4-e29b-41d4-a716-446655440000_20240315.csv
```

Filename convention: `COMPETITIONID_MATCH_CANBUSID_date` where `CANBUSID` is a UUID.

### Columns

- `Timestamp` — seconds since system boot (not match start), 20ms sample rate
- `Phoenix6/TalonFX-{id}/MotorVoltage` — per motor
- `Phoenix6/TalonFX-{id}/StatorCurrent` — per motor
- `Phoenix6/TalonFX-{id}/SupplyVoltage` — per motor (optional)
- `Phoenix6/TalonFX-{id}/SupplyCurrent` — per motor (optional)

Values may be `null` at startup. Voltage and current may be negative (motor direction). Sample rate is 20ms.

### Match Grouping

Files are grouped into matches by match ID — everything in the filename before the UUID pattern (`[0-9a-f]{8}-[0-9a-f]{4}-...`). Files without a UUID use everything before the last `-` or `_` delimiter. Files sharing the same match ID are merged (outer join on timestamp).

---

## Architecture

```
utils/
├── __init__.py
├── loader.py       # CSV parsing, filename grouping, DataFrame construction
├── trimmer.py      # Match window auto-detection
├── analyzer.py     # Normalization, power/energy calculations → MotorData
├── plotter.py      # All matplotlib figures
├── reporter.py     # PDF assembly via matplotlib PdfPages
└── cli.py          # CLI entry point
```

### Data Model

```python
@dataclass
class MotorData:
    # Motor power (MotorVoltage × StatorCurrent) — torque proxy
    motor_voltage: np.ndarray      # absolute values
    stator_current: np.ndarray     # absolute values
    motor_power: np.ndarray        # motor_voltage × stator_current
    motor_energy: np.ndarray       # cumulative energy (Wh), dt=0.02s

    # Supply power (SupplyVoltage × SupplyCurrent) — actual battery draw
    # None if columns not present in source data
    supply_voltage: np.ndarray | None
    supply_current: np.ndarray | None
    supply_power: np.ndarray | None
    supply_energy: np.ndarray | None  # cumulative energy (Wh), dt=0.02s

@dataclass
class Match:
    match_id: str
    timestamps: np.ndarray          # trimmed, re-zeroed to match start (seconds)
    motors: dict[str, MotorData]    # keyed by "TalonFX-{id}"
    totals: MotorData               # robot-wide sum across all motors
```

---

## Pipeline

```
CSV files
  → loader:   group by match ID, merge CAN bus files (outer join on timestamp)
  → trimmer:  detect match window, re-zero timestamps
  → analyzer: normalize to absolute values, compute power + cumulative energy
  → plotter:  generate matplotlib figures
  → reporter: assemble PDF
```

---

## Module Responsibilities

### `loader.py`

- `find_matches(paths: list[Path]) -> dict[str, list[Path]]`  
  Groups files by match ID using UUID detection in filename.

- `load_match(files: list[Path]) -> pd.DataFrame`  
  Loads and outer-joins all CAN bus CSVs for a match on timestamp. Detects motor columns by pattern `Phoenix6/TalonFX-{id}/{metric}`. Returns a single merged DataFrame.

### `trimmer.py`

- `trim_to_match(df: pd.DataFrame, voltage_threshold: float = 0.5) -> pd.DataFrame`  
  Finds first row where `any(abs(MotorVoltage) > threshold)` and last such row. Slices the DataFrame to that window and resets `Timestamp` to start at 0. `null` values are treated as 0 for threshold detection then forward-filled for analysis.

### `analyzer.py`

- `normalize(df: pd.DataFrame) -> pd.DataFrame`  
  Takes absolute values of all voltage and current columns.

- `compute_motor_data(df: pd.DataFrame) -> dict[str, MotorData]`  
  Builds a `MotorData` instance per detected motor. Cumulative energy in Wh = `cumsum(power × 0.02) / 3600`.

- `compute_totals(motors: dict[str, MotorData]) -> MotorData`  
  Sums per-timestep power across all motors for robot-wide `MotorData`.

### `plotter.py`

- `plot_instantaneous(match: Match, metric: str) -> Figure`  
  Time-series line plot. One line per motor. `metric` is one of: `motor_voltage`, `stator_current`, `motor_power`, `supply_current`, `supply_power`.

- `plot_cumulative_energy(match: Match, power_type: str) -> Figure`  
  Running energy curves per motor + robot total. `power_type`: `"motor"` or `"supply"`.

- `plot_per_motor(motor_id: str, data: MotorData, timestamps: np.ndarray) -> Figure`  
  All available metrics for a single motor on one figure (optional, `--per-motor-graphs`).

- `plot_comparison(matches: list[Match], metric: str) -> Figure`  
  Overlays the same metric across multiple matches. One line per match.

### `reporter.py`

- `build_report(matches: list[Match], output: Path, per_motor: bool = False)`  
  Assembles the PDF using `matplotlib.backends.backend_pdf.PdfPages`.

**Page order (single match):**

| Page | Content |
|------|---------|
| 1 | Cover: match ID, date, motor count, match duration |
| 2 | Summary stat table: per-motor max/avg voltage, current, peak power, total energy (motor + supply where available) + totals row |
| 3 | Graph: instantaneous motor voltage — all motors |
| 4 | Graph: instantaneous stator current — all motors |
| 5 | Graph: instantaneous motor power — all motors |
| 6 | Graph: instantaneous supply current — all motors *(if available)* |
| 7 | Graph: instantaneous supply power — all motors *(if available)* |
| 8 | Graph: cumulative motor energy — per motor + total |
| 9 | Graph: cumulative supply energy — per motor + total *(if available)* |
| 10+ | *(if `--per-motor-graphs`)* One page per motor, all metrics |

**Additional pages for multi-match comparison:**
- Overlaid cumulative energy curves (one line per match, robot total)
- Side-by-side summary stat table (columns = matches, rows = motors)

### `cli.py`

Entry point: `python -m utils [FILE...] [--output report.pdf] [--per-motor-graphs] [--threshold 0.5]`

- Accepts one or more CSV file paths
- Groups into matches automatically
- If multiple matches detected, appends comparison pages to the report

---

## Key Decisions

- **StatorCurrent vs SupplyCurrent**: Both are calculated and labeled distinctly. "Motor Power" (MotorVoltage × StatorCurrent) is a torque proxy. "Supply Power" (SupplyVoltage × SupplyCurrent) is actual battery draw. Only supply power represents real energy consumption.
- **Trim heuristic**: Voltage activity threshold, not fixed duration. Auto-detects start and end.
- **Motor labels**: Raw CAN IDs (e.g. `TalonFX-11`). No datamap lookup.
- **Energy units**: Watt-hours (Wh). `dt = 0.02s` (fixed 20ms sample rate, no interpolation needed).
- **PDF backend**: `matplotlib.backends.backend_pdf.PdfPages` — no LaTeX or headless browser dependency.
- **Null handling**: `null` CSV values treated as 0 during trim detection, forward-filled otherwise.

---

## Project & Environment Setup

**Runtime:** Python 3.11+, managed via `pyenv` + `pyenv virtualenv`.

```bash
pyenv install 3.11.x
pyenv virtualenv 3.11.x flashpoint
pyenv local flashpoint
```

**Packaging:** Poetry. The `utils/` package is added to the existing `pyproject.toml`.

```bash
poetry install
poetry run python -m utils [args]
```

**Dependencies:**

```toml
[tool.poetry.dependencies]
python = "^3.11"
pandas = "^2.0"
numpy = "^1.26"
matplotlib = "^3.8"
```

No additional PDF library required — matplotlib's PDF backend is sufficient.

---

## Testing

**Framework:** pytest with strict markers and quality gates.

```toml
[tool.pytest.ini_options]
addopts = "--strict-markers -q"
```

Test coverage targets:
- `loader.py` — filename grouping (UUID detection, fallback), CSV merge logic
- `trimmer.py` — threshold detection, edge cases (all-zero file, immediate activity)
- `analyzer.py` — normalization correctness, energy calculation (Wh math), totals summation
- `plotter.py` — smoke tests only (figures return without error; no pixel comparison)
- `reporter.py` — PDF written to disk, page count matches expected

Tests live in `tests/utils/`. Fixtures use the existing sample CSVs in `data/`.

---

## Code Conventions

- **Type hints:** Required on all function signatures and dataclass fields.
- **Naming:** `snake_case` for variables and functions, `PascalCase` for classes, `SCREAMING_SNAKE_CASE` for module-level constants.
- **Filenames:** Python modules use `snake_case` (required for importability). Non-Python files use `kebab-case`.
- **Import order:** external libraries → internal project modules → local, alphabetical within each group.
- **No docstrings on unchanged code.** New public functions get a one-line docstring only where the signature isn't self-explanatory.
