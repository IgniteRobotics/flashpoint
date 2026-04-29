# Motor Power Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `utils/` Python package that parses FRC TalonFX motor telemetry CSVs, trims to match time, computes dual power metrics, and generates a multi-page PDF report with graphs and optional match comparison.

**Architecture:** A pipeline of focused modules — loader groups CSV files by match ID and merges CAN buses, trimmer auto-detects match window by voltage activity, analyzer normalizes and computes power/energy, plotter generates matplotlib figures, reporter assembles the PDF. A CLI entry point orchestrates the pipeline.

**Tech Stack:** Python 3.11+, pyenv + pyenv virtualenv, Poetry, pandas 2.x, numpy 1.26+, matplotlib 3.8+ (PdfPages for PDF output), pytest with strict markers.

---

## File Map

**Create:**
- `pyproject.toml` — project definition + dependencies
- `utils/__init__.py` — package marker
- `utils/__main__.py` — enables `python -m utils`
- `utils/models.py` — `MotorData` and `Match` dataclasses
- `utils/loader.py` — filename grouping, CSV loading and merging
- `utils/trimmer.py` — voltage-threshold match window detection
- `utils/analyzer.py` — normalization, power/energy math, `Match` construction
- `utils/plotter.py` — all matplotlib figure generators
- `utils/reporter.py` — PDF assembly via `PdfPages`
- `utils/cli.py` — argparse entry point
- `tests/__init__.py`
- `tests/utils/__init__.py`
- `tests/utils/conftest.py` — shared CSV fixtures and `make_match` helper
- `tests/utils/test_loader.py`
- `tests/utils/test_trimmer.py`
- `tests/utils/test_analyzer.py`
- `tests/utils/test_plotter.py`
- `tests/utils/test_reporter.py`

---

## Task 1: Project Scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `utils/__init__.py`
- Create: `utils/__main__.py`
- Create: `tests/__init__.py`
- Create: `tests/utils/__init__.py`
- Create: `tests/utils/conftest.py`

- [ ] **Step 1: Set up pyenv virtualenv**

```bash
pyenv virtualenv 3.11 flashpoint
pyenv local flashpoint
python --version  # should show 3.11.x
```

- [ ] **Step 2: Initialize Poetry project**

```bash
poetry init --name flashpoint --python "^3.11" --no-interaction
```

- [ ] **Step 3: Add dependencies to pyproject.toml**

Replace the generated `pyproject.toml` with:

```toml
[tool.poetry]
name = "flashpoint"
version = "0.1.0"
description = "FRC robot telemetry analysis"
authors = []
packages = [{include = "utils"}]

[tool.poetry.dependencies]
python = "^3.11"
pandas = "^2.0"
numpy = "^1.26"
matplotlib = "^3.8"

[tool.poetry.group.dev.dependencies]
pytest = "^8.0"

[tool.pytest.ini_options]
addopts = "--strict-markers -q"
testpaths = ["tests"]

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

- [ ] **Step 4: Install dependencies**

```bash
poetry install
```

Expected: resolves and installs pandas, numpy, matplotlib, pytest.

- [ ] **Step 5: Create package stubs**

`utils/__init__.py`:
```python
"""FRC motor power analysis utilities."""
```

`utils/__main__.py`:
```python
from .cli import main

main()
```

`tests/__init__.py`: empty file.

`tests/utils/__init__.py`: empty file.

- [ ] **Step 6: Create conftest.py with shared fixtures**

`tests/utils/conftest.py`:
```python
from __future__ import annotations
from pathlib import Path

import numpy as np
import pytest

from utils.models import Match, MotorData

MINIMAL_RIO_CSV = """\
Timestamp,Phoenix6/TalonFX-1/MotorVoltage,Phoenix6/TalonFX-2/MotorVoltage,Phoenix6/TalonFX-1/StatorCurrent,Phoenix6/TalonFX-2/StatorCurrent
0.0,0.0,0.0,0.0,0.0
0.02,0.0,0.0,0.0,0.0
0.04,5.0,3.0,-2.0,1.5
0.06,6.0,4.0,-3.0,2.0
0.08,0.0,0.0,0.0,0.0
"""

MINIMAL_CARNIVORE_UUID = "a1b2c3d4-e29b-41d4-a716-446655440000"

MINIMAL_CARNIVORE_CSV = """\
Timestamp,Phoenix6/TalonFX-11/MotorVoltage,Phoenix6/TalonFX-11/StatorCurrent
0.0,0.0,0.0
0.02,0.0,0.0
0.04,4.0,-1.0
0.06,5.0,-2.0
0.08,0.0,0.0
"""


@pytest.fixture
def rio_csv(tmp_path: Path) -> Path:
    f = tmp_path / "GACMP_Q1-rio.csv"
    f.write_text(MINIMAL_RIO_CSV)
    return f


@pytest.fixture
def carnivore_csv(tmp_path: Path) -> Path:
    f = tmp_path / f"GACMP_Q1_{MINIMAL_CARNIVORE_UUID}.csv"
    f.write_text(MINIMAL_CARNIVORE_CSV)
    return f


def _make_motor_data(n: int = 10, with_supply: bool = False) -> MotorData:
    power = np.ones(n) * 100.0
    energy = np.cumsum(power * 0.02) / 3600
    kwargs: dict = dict(
        motor_voltage=np.ones(n) * 5.0,
        stator_current=np.ones(n) * 20.0,
        motor_power=power,
        motor_energy=energy,
    )
    if with_supply:
        kwargs.update(
            supply_voltage=np.ones(n) * 12.0,
            supply_current=np.ones(n) * 8.5,
            supply_power=np.ones(n) * 102.0,
            supply_energy=np.cumsum(np.ones(n) * 102.0 * 0.02) / 3600,
        )
    return MotorData(**kwargs)


@pytest.fixture
def simple_match() -> Match:
    n = 10
    t = np.arange(n) * 0.02
    data = _make_motor_data(n)
    motors = {"TalonFX-1": data, "TalonFX-2": data}
    totals = MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=data.motor_power * 2,
        motor_energy=data.motor_energy * 2,
    )
    return Match(match_id="TEST_Q1", timestamps=t, motors=motors, totals=totals)


@pytest.fixture
def simple_match_with_supply() -> Match:
    n = 10
    t = np.arange(n) * 0.02
    data = _make_motor_data(n, with_supply=True)
    motors = {"TalonFX-1": data}
    totals = MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=data.motor_power,
        motor_energy=data.motor_energy,
        supply_voltage=np.zeros(n),
        supply_current=np.zeros(n),
        supply_power=data.supply_power,
        supply_energy=data.supply_energy,
    )
    return Match(match_id="TEST_Q1_SUPPLY", timestamps=t, motors=motors, totals=totals)
```

- [ ] **Step 7: Verify pytest discovers tests**

```bash
poetry run pytest --collect-only
```

Expected: `no tests ran` (0 errors, just nothing collected yet).

- [ ] **Step 8: Commit scaffold**

```bash
git add pyproject.toml utils/__init__.py utils/__main__.py tests/
git commit -m "feat: scaffold utils package and test infrastructure"
```

---

## Task 2: Models

**Files:**
- Create: `utils/models.py`

- [ ] **Step 1: Write the failing test**

`tests/utils/test_models.py` — create this file:
```python
import numpy as np
from utils.models import MotorData, Match


def test_motor_data_requires_core_fields():
    n = 5
    data = MotorData(
        motor_voltage=np.ones(n),
        stator_current=np.ones(n),
        motor_power=np.ones(n),
        motor_energy=np.ones(n),
    )
    assert data.supply_power is None
    assert data.supply_energy is None


def test_motor_data_accepts_supply_fields():
    n = 5
    data = MotorData(
        motor_voltage=np.ones(n),
        stator_current=np.ones(n),
        motor_power=np.ones(n),
        motor_energy=np.ones(n),
        supply_voltage=np.ones(n),
        supply_current=np.ones(n),
        supply_power=np.ones(n),
        supply_energy=np.ones(n),
    )
    assert data.supply_power is not None


def test_match_holds_motors_and_totals():
    n = 5
    t = np.arange(n) * 0.02
    data = MotorData(
        motor_voltage=np.ones(n),
        stator_current=np.ones(n),
        motor_power=np.ones(n),
        motor_energy=np.ones(n),
    )
    match = Match(
        match_id="GACMP_E5",
        timestamps=t,
        motors={"TalonFX-1": data},
        totals=data,
    )
    assert match.match_id == "GACMP_E5"
    assert "TalonFX-1" in match.motors
```

- [ ] **Step 2: Run test to verify it fails**

```bash
poetry run pytest tests/utils/test_models.py -v
```

Expected: `ModuleNotFoundError: No module named 'utils.models'`

- [ ] **Step 3: Implement models.py**

`utils/models.py`:
```python
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np


@dataclass
class MotorData:
    """Per-motor telemetry arrays, normalized to absolute values."""

    # Motor power path: MotorVoltage × StatorCurrent (torque proxy)
    motor_voltage: np.ndarray
    stator_current: np.ndarray
    motor_power: np.ndarray
    motor_energy: np.ndarray  # cumulative Wh

    # Supply power path: SupplyVoltage × SupplyCurrent (actual battery draw)
    # None when source CSV does not contain these columns.
    supply_voltage: np.ndarray | None = None
    supply_current: np.ndarray | None = None
    supply_power: np.ndarray | None = None
    supply_energy: np.ndarray | None = None  # cumulative Wh


@dataclass
class Match:
    """A single FRC match worth of trimmed, normalized motor data."""

    match_id: str
    timestamps: np.ndarray       # seconds, re-zeroed to match start
    motors: dict[str, MotorData]  # keyed by "TalonFX-{id}"
    totals: MotorData             # robot-wide sum across all motors
```

- [ ] **Step 4: Run test to verify it passes**

```bash
poetry run pytest tests/utils/test_models.py -v
```

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add utils/models.py tests/utils/test_models.py
git commit -m "feat: add MotorData and Match dataclasses"
```

---

## Task 3: Loader

**Files:**
- Create: `utils/loader.py`
- Create: `tests/utils/test_loader.py`

- [ ] **Step 1: Write the failing tests**

`tests/utils/test_loader.py`:
```python
from __future__ import annotations
from pathlib import Path

import pytest

from utils.loader import extract_match_id, find_matches, load_match, get_motor_ids


def test_extract_match_id_with_uuid(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_a1b2c3d4-e29b-41d4-a716-446655440000_20240315.csv"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_fallback_dash(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5-rio.csv"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_fallback_underscore(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_CARNIVORE.csv"
    assert extract_match_id(p) == "GACMP_E5"


def test_find_matches_groups_uuid_and_dash_files(tmp_path: Path) -> None:
    rio = tmp_path / "GACMP_E5-rio.csv"
    carnivore = tmp_path / "GACMP_E5_a1b2c3d4-e29b-41d4-a716-446655440000.csv"
    rio.touch()
    carnivore.touch()
    groups = find_matches([rio, carnivore])
    assert list(groups.keys()) == ["GACMP_E5"]
    assert len(groups["GACMP_E5"]) == 2


def test_find_matches_separates_different_match_ids(tmp_path: Path) -> None:
    q1 = tmp_path / "GACMP_Q1-rio.csv"
    q2 = tmp_path / "GACMP_Q2-rio.csv"
    q1.touch()
    q2.touch()
    groups = find_matches([q1, q2])
    assert len(groups) == 2
    assert "GACMP_Q1" in groups
    assert "GACMP_Q2" in groups


def test_load_match_single_file_returns_dataframe(rio_csv: Path) -> None:
    df = load_match([rio_csv])
    assert "Timestamp" in df.columns
    assert "Phoenix6/TalonFX-1/MotorVoltage" in df.columns
    assert df["Timestamp"].is_monotonic_increasing


def test_load_match_merges_two_canbus_files(rio_csv: Path, carnivore_csv: Path) -> None:
    df = load_match([rio_csv, carnivore_csv])
    assert "Phoenix6/TalonFX-1/MotorVoltage" in df.columns
    assert "Phoenix6/TalonFX-11/MotorVoltage" in df.columns
    assert df["Timestamp"].is_monotonic_increasing


def test_load_match_parses_null_as_nan(rio_csv: Path) -> None:
    import math
    df = load_match([rio_csv])
    # row 0 in MINIMAL_RIO_CSV has no nulls, but SupplyVoltage columns absent → OK
    # just confirm no "null" string remains
    for col in df.columns:
        assert df[col].dtype != object or df[col].isna().all()


def test_get_motor_ids_returns_sorted_list(rio_csv: Path, carnivore_csv: Path) -> None:
    df = load_match([rio_csv, carnivore_csv])
    ids = get_motor_ids(df)
    nums = [int(mid.split("-")[1]) for mid in ids]
    assert nums == sorted(nums)
    assert "TalonFX-1" in ids
    assert "TalonFX-11" in ids
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/utils/test_loader.py -v
```

Expected: `ModuleNotFoundError: No module named 'utils.loader'`

- [ ] **Step 3: Implement loader.py**

`utils/loader.py`:
```python
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
MOTOR_COL_PATTERN = re.compile(
    r"Phoenix6/TalonFX-(\d+)/(MotorVoltage|StatorCurrent|SupplyVoltage|SupplyCurrent)"
)


def extract_match_id(path: Path) -> str:
    """Extract match ID from a CSV filename.

    Strips the UUID (or last dash/underscore-delimited token) from the stem.
    """
    stem = path.stem
    uuid_match = UUID_PATTERN.search(stem)
    if uuid_match:
        return stem[: uuid_match.start()].rstrip("-_")
    # Fallback: everything before the last separator
    parts = re.split(r"[-_]", stem)
    return "_".join(parts[:-1]) if len(parts) > 1 else stem


def find_matches(paths: list[Path]) -> dict[str, list[Path]]:
    """Group CSV files by match ID."""
    groups: dict[str, list[Path]] = {}
    for path in paths:
        match_id = extract_match_id(path)
        groups.setdefault(match_id, []).append(path)
    return groups


def load_match(files: list[Path]) -> pd.DataFrame:
    """Load and outer-join all CAN bus CSVs for a match into one DataFrame."""
    dfs = [pd.read_csv(f, na_values=["null"]) for f in files]
    if len(dfs) == 1:
        return dfs[0].sort_values("Timestamp").reset_index(drop=True)
    merged = dfs[0]
    for df in dfs[1:]:
        merged = pd.merge(merged, df, on="Timestamp", how="outer")
    return merged.sort_values("Timestamp").reset_index(drop=True)


def get_motor_ids(df: pd.DataFrame) -> list[str]:
    """Return sorted list of unique TalonFX motor IDs found in DataFrame columns."""
    ids: set[str] = set()
    for col in df.columns:
        m = MOTOR_COL_PATTERN.match(col)
        if m:
            ids.add(f"TalonFX-{m.group(1)}")
    return sorted(ids, key=lambda x: int(x.split("-")[1]))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
poetry run pytest tests/utils/test_loader.py -v
```

Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add utils/loader.py tests/utils/test_loader.py
git commit -m "feat: add loader — filename grouping and CSV merging"
```

---

## Task 4: Trimmer

**Files:**
- Create: `utils/trimmer.py`
- Create: `tests/utils/test_trimmer.py`

- [ ] **Step 1: Write the failing tests**

`tests/utils/test_trimmer.py`:
```python
from __future__ import annotations

import pandas as pd
import pytest

from utils.trimmer import trim_to_match


def _df(timestamps: list[float], voltages: list[float | None]) -> pd.DataFrame:
    return pd.DataFrame({
        "Timestamp": timestamps,
        "Phoenix6/TalonFX-1/MotorVoltage": voltages,
    })


def test_trim_removes_leading_zeros() -> None:
    df = _df([0.0, 0.02, 0.04, 0.06, 0.08], [0.0, 0.0, 5.0, 6.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert len(result) == 3  # rows at 0.04, 0.06, 0.08


def test_trim_removes_trailing_zeros() -> None:
    df = _df([0.0, 0.02, 0.04, 0.06, 0.08, 0.10], [0.0, 5.0, 6.0, 0.0, 0.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert len(result) == 2  # rows at 0.02, 0.04


def test_trim_resets_timestamp_to_zero() -> None:
    df = _df([10.0, 10.02, 10.04, 10.06], [0.0, 5.0, 6.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert result["Timestamp"].iloc[0] == pytest.approx(0.0)


def test_trim_preserves_relative_spacing() -> None:
    df = _df([10.0, 10.02, 10.04, 10.06], [0.0, 5.0, 6.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert result["Timestamp"].iloc[1] == pytest.approx(0.02)


def test_trim_returns_original_if_no_activity() -> None:
    df = _df([0.0, 0.02, 0.04], [0.0, 0.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert len(result) == 3


def test_trim_treats_null_as_inactive() -> None:
    df = _df([0.0, 0.02, 0.04, 0.06], [None, None, 5.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    # only rows 0.04 and 0.06 fall in the active window
    assert len(result) == 2


def test_trim_handles_negative_voltage() -> None:
    df = _df([0.0, 0.02, 0.04, 0.06], [0.0, -5.0, -6.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert len(result) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/utils/test_trimmer.py -v
```

Expected: `ModuleNotFoundError: No module named 'utils.trimmer'`

- [ ] **Step 3: Implement trimmer.py**

`utils/trimmer.py`:
```python
from __future__ import annotations

import numpy as np
import pandas as pd

from .loader import MOTOR_COL_PATTERN


def _voltage_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if MOTOR_COL_PATTERN.match(c) and "MotorVoltage" in c]


def trim_to_match(df: pd.DataFrame, voltage_threshold: float = 0.5) -> pd.DataFrame:
    """Trim DataFrame to the active match window.

    Finds the first and last row where any motor voltage exceeds the threshold.
    Resets Timestamp to start at 0. Nulls are treated as 0 for detection.
    Returns the original DataFrame unchanged if no activity is found.
    """
    vol_cols = _voltage_cols(df)
    if not vol_cols:
        return df

    voltages = df[vol_cols].fillna(0.0)
    active = (voltages.abs() > voltage_threshold).any(axis=1)
    active_indices = np.where(active)[0]

    if len(active_indices) == 0:
        return df

    start_idx = int(active_indices[0])
    end_idx = int(active_indices[-1])

    trimmed = df.iloc[start_idx : end_idx + 1].copy()
    trimmed["Timestamp"] = trimmed["Timestamp"] - trimmed["Timestamp"].iloc[0]
    return trimmed.reset_index(drop=True)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
poetry run pytest tests/utils/test_trimmer.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add utils/trimmer.py tests/utils/test_trimmer.py
git commit -m "feat: add trimmer — voltage-threshold match window detection"
```

---

## Task 5: Analyzer

**Files:**
- Create: `utils/analyzer.py`
- Create: `tests/utils/test_analyzer.py`

- [ ] **Step 1: Write the failing tests**

`tests/utils/test_analyzer.py`:
```python
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from utils.analyzer import (
    DT,
    SECONDS_PER_HOUR,
    build_match,
    compute_motor_data,
    compute_totals,
    normalize,
)


def _make_df(with_supply: bool = False) -> pd.DataFrame:
    data: dict = {
        "Timestamp": [0.0, 0.02, 0.04],
        "Phoenix6/TalonFX-1/MotorVoltage": [-5.0, 6.0, -3.0],
        "Phoenix6/TalonFX-1/StatorCurrent": [-2.0, 3.0, -1.0],
        "Phoenix6/TalonFX-2/MotorVoltage": [4.0, -5.0, 2.0],
        "Phoenix6/TalonFX-2/StatorCurrent": [1.5, -2.0, 0.5],
    }
    if with_supply:
        data["Phoenix6/TalonFX-1/SupplyVoltage"] = [12.0, 12.0, 12.0]
        data["Phoenix6/TalonFX-1/SupplyCurrent"] = [3.0, 4.0, 2.0]
        data["Phoenix6/TalonFX-2/SupplyVoltage"] = [12.0, 12.0, 12.0]
        data["Phoenix6/TalonFX-2/SupplyCurrent"] = [2.0, 3.0, 1.0]
    return pd.DataFrame(data)


def test_normalize_makes_all_motor_cols_non_negative() -> None:
    df = normalize(_make_df())
    for col in df.columns:
        if col != "Timestamp":
            assert (df[col] >= 0).all(), f"{col} has negative values"


def test_normalize_leaves_timestamp_unchanged() -> None:
    df = _make_df()
    result = normalize(df)
    pd.testing.assert_series_equal(df["Timestamp"], result["Timestamp"])


def test_compute_motor_data_creates_entry_per_motor() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    assert "TalonFX-1" in motors
    assert "TalonFX-2" in motors


def test_motor_power_equals_voltage_times_current() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    data = motors["TalonFX-1"]
    expected = data.motor_voltage * data.stator_current
    np.testing.assert_array_almost_equal(data.motor_power, expected)


def test_motor_energy_is_monotonically_non_decreasing() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    energy = motors["TalonFX-1"].motor_energy
    assert np.all(np.diff(energy) >= 0)


def test_motor_energy_units_are_watt_hours() -> None:
    # 1000 W for one 20ms sample → 1000 * 0.02 / 3600 Wh
    df = pd.DataFrame({
        "Timestamp": [0.0, 0.02],
        "Phoenix6/TalonFX-1/MotorVoltage": [100.0, 100.0],
        "Phoenix6/TalonFX-1/StatorCurrent": [10.0, 10.0],
    })
    motors = compute_motor_data(df)
    expected_first = 1000.0 * DT / SECONDS_PER_HOUR
    assert motors["TalonFX-1"].motor_energy[0] == pytest.approx(expected_first)


def test_supply_fields_none_when_columns_absent() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    assert motors["TalonFX-1"].supply_power is None
    assert motors["TalonFX-1"].supply_energy is None


def test_supply_power_computed_when_columns_present() -> None:
    motors = compute_motor_data(normalize(_make_df(with_supply=True)))
    data = motors["TalonFX-1"]
    assert data.supply_power is not None
    np.testing.assert_array_almost_equal(
        data.supply_power,
        data.supply_voltage * data.supply_current,  # type: ignore[operator]
    )


def test_compute_totals_sums_motor_power() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    totals = compute_totals(motors)
    expected = motors["TalonFX-1"].motor_power + motors["TalonFX-2"].motor_power
    np.testing.assert_array_almost_equal(totals.motor_power, expected)


def test_compute_totals_supply_none_when_any_motor_missing_supply() -> None:
    motors = compute_motor_data(normalize(_make_df()))  # no supply cols
    totals = compute_totals(motors)
    assert totals.supply_power is None


def test_compute_totals_supply_computed_when_all_motors_have_supply() -> None:
    motors = compute_motor_data(normalize(_make_df(with_supply=True)))
    totals = compute_totals(motors)
    assert totals.supply_power is not None


def test_build_match_sets_match_id() -> None:
    df = normalize(_make_df())
    match = build_match("GACMP_E5", df)
    assert match.match_id == "GACMP_E5"


def test_build_match_timestamps_start_at_zero() -> None:
    df = normalize(_make_df())
    match = build_match("GACMP_E5", df)
    assert match.timestamps[0] == pytest.approx(0.0)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/utils/test_analyzer.py -v
```

Expected: `ModuleNotFoundError: No module named 'utils.analyzer'`

- [ ] **Step 3: Implement analyzer.py**

`utils/analyzer.py`:
```python
from __future__ import annotations

import numpy as np
import pandas as pd

from .loader import MOTOR_COL_PATTERN, get_motor_ids
from .models import Match, MotorData

DT: float = 0.02           # seconds per sample
SECONDS_PER_HOUR: float = 3600.0


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Take absolute values of all motor voltage and current columns."""
    result = df.copy()
    for col in result.columns:
        if MOTOR_COL_PATTERN.match(col):
            result[col] = result[col].abs()
    return result


def _cumulative_energy(power: np.ndarray) -> np.ndarray:
    return np.cumsum(power * DT) / SECONDS_PER_HOUR


def _col(motor_id: str, metric: str) -> str:
    num = motor_id.split("-")[1]
    return f"Phoenix6/TalonFX-{num}/{metric}"


def compute_motor_data(df: pd.DataFrame) -> dict[str, MotorData]:
    """Build a MotorData instance for each motor found in the DataFrame."""
    n = len(df)
    zeros = np.zeros(n)
    motors: dict[str, MotorData] = {}

    for motor_id in get_motor_ids(df):
        mv_col = _col(motor_id, "MotorVoltage")
        sc_col = _col(motor_id, "StatorCurrent")
        sv_col = _col(motor_id, "SupplyVoltage")
        su_col = _col(motor_id, "SupplyCurrent")

        mv = df[mv_col].fillna(0.0).to_numpy() if mv_col in df.columns else zeros.copy()
        sc = df[sc_col].fillna(0.0).to_numpy() if sc_col in df.columns else zeros.copy()
        mp = mv * sc

        has_supply = sv_col in df.columns and su_col in df.columns
        if has_supply:
            sv = df[sv_col].fillna(0.0).to_numpy()
            su = df[su_col].fillna(0.0).to_numpy()
            sp = sv * su
            motors[motor_id] = MotorData(
                motor_voltage=mv,
                stator_current=sc,
                motor_power=mp,
                motor_energy=_cumulative_energy(mp),
                supply_voltage=sv,
                supply_current=su,
                supply_power=sp,
                supply_energy=_cumulative_energy(sp),
            )
        else:
            motors[motor_id] = MotorData(
                motor_voltage=mv,
                stator_current=sc,
                motor_power=mp,
                motor_energy=_cumulative_energy(mp),
            )

    return motors


def compute_totals(motors: dict[str, MotorData]) -> MotorData:
    """Sum per-timestep power across all motors to produce robot-wide MotorData."""
    motor_list = list(motors.values())
    if not motor_list:
        raise ValueError("No motors to aggregate")

    total_mp: np.ndarray = sum(m.motor_power for m in motor_list)  # type: ignore[assignment]
    has_supply = all(m.supply_power is not None for m in motor_list)

    if has_supply:
        total_sp: np.ndarray = sum(m.supply_power for m in motor_list)  # type: ignore[assignment]
        n = len(total_mp)
        return MotorData(
            motor_voltage=np.zeros(n),
            stator_current=np.zeros(n),
            motor_power=total_mp,
            motor_energy=_cumulative_energy(total_mp),
            supply_voltage=np.zeros(n),
            supply_current=np.zeros(n),
            supply_power=total_sp,
            supply_energy=_cumulative_energy(total_sp),
        )

    n = len(total_mp)
    return MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=total_mp,
        motor_energy=_cumulative_energy(total_mp),
    )


def build_match(match_id: str, df: pd.DataFrame) -> Match:
    """Construct a Match from a normalized, trimmed DataFrame."""
    timestamps = df["Timestamp"].to_numpy()
    motors = compute_motor_data(df)
    totals = compute_totals(motors)
    return Match(match_id=match_id, timestamps=timestamps, motors=motors, totals=totals)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
poetry run pytest tests/utils/test_analyzer.py -v
```

Expected: 13 passed.

- [ ] **Step 5: Commit**

```bash
git add utils/analyzer.py tests/utils/test_analyzer.py
git commit -m "feat: add analyzer — normalize, power/energy math, Match construction"
```

---

## Task 6: Plotter

**Files:**
- Create: `utils/plotter.py`
- Create: `tests/utils/test_plotter.py`

- [ ] **Step 1: Write the failing tests**

`tests/utils/test_plotter.py`:
```python
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
    import numpy as np
    m2 = copy.deepcopy(simple_match)
    m2.match_id = "TEST_Q2"
    fig = plotter.plot_comparison([simple_match, m2], "motor_energy")
    assert isinstance(fig, Figure)


def test_plot_instantaneous_supply_skips_none_gracefully(simple_match) -> None:
    # simple_match has no supply data — should not raise
    fig = plotter.plot_instantaneous(simple_match, "supply_power")
    assert isinstance(fig, Figure)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/utils/test_plotter.py -v
```

Expected: `ModuleNotFoundError: No module named 'utils.plotter'`

- [ ] **Step 3: Implement plotter.py**

`utils/plotter.py`:
```python
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


def plot_instantaneous(match: Match, metric: str) -> Figure:
    """Time-series line plot of a metric for all motors."""
    label, unit = METRIC_LABELS[metric]
    fig, ax = plt.subplots(figsize=(12, 5))
    for motor_id, data in match.motors.items():
        values = _get(data, metric)
        if values is not None:
            ax.plot(match.timestamps, values, label=motor_id, linewidth=0.8)
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
            ax.plot(match.timestamps, values, label=motor_id, linewidth=0.8)
    total = _get(match.totals, energy_attr)
    if total is not None:
        ax.plot(match.timestamps, total, label="TOTAL", linewidth=2,
                color="black", linestyle="--")
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
        ax.plot(timestamps, _get(data, attr), linewidth=0.8)
        ax.set_ylabel(f"{lbl} ({unit})")
    axes[-1].set_xlabel("Time (s)")
    fig.suptitle(motor_id)
    fig.tight_layout()
    return fig


def plot_comparison(matches: list[Match], metric: str) -> Figure:
    """Overlay robot-total metric across multiple matches."""
    label, unit = METRIC_LABELS[metric]
    fig, ax = plt.subplots(figsize=(12, 5))
    for match in matches:
        values = _get(match.totals, metric)
        if values is not None:
            ax.plot(match.timestamps, values, label=match.match_id, linewidth=1.2)
    ax.set_title(f"Match Comparison — {label} (Robot Total)")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel(f"{label} ({unit})")
    ax.legend()
    fig.tight_layout()
    return fig
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
poetry run pytest tests/utils/test_plotter.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add utils/plotter.py tests/utils/test_plotter.py
git commit -m "feat: add plotter — matplotlib figure generators"
```

---

## Task 7: Reporter

**Files:**
- Create: `utils/reporter.py`
- Create: `tests/utils/test_reporter.py`

- [ ] **Step 1: Write the failing tests**

`tests/utils/test_reporter.py`:
```python
from __future__ import annotations

import matplotlib
matplotlib.use("Agg")

from pathlib import Path

import pytest

from utils.reporter import build_report


def test_build_report_creates_pdf_file(simple_match, tmp_path: Path) -> None:
    output = tmp_path / "report.pdf"
    build_report([simple_match], output)
    assert output.exists()
    assert output.stat().st_size > 0


def test_build_report_with_per_motor_graphs(simple_match, tmp_path: Path) -> None:
    output = tmp_path / "report_per_motor.pdf"
    build_report([simple_match], output, per_motor=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_build_report_with_supply_data(simple_match_with_supply, tmp_path: Path) -> None:
    output = tmp_path / "report_supply.pdf"
    build_report([simple_match_with_supply], output)
    assert output.exists()
    assert output.stat().st_size > 0


def test_build_report_multi_match_includes_comparison(simple_match, tmp_path: Path) -> None:
    import copy
    m2 = copy.deepcopy(simple_match)
    m2.match_id = "TEST_Q2"
    output = tmp_path / "report_comparison.pdf"
    build_report([simple_match, m2], output)
    assert output.exists()
    assert output.stat().st_size > 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/utils/test_reporter.py -v
```

Expected: `ModuleNotFoundError: No module named 'utils.reporter'`

- [ ] **Step 3: Implement reporter.py**

`utils/reporter.py`:
```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
poetry run pytest tests/utils/test_reporter.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add utils/reporter.py tests/utils/test_reporter.py
git commit -m "feat: add reporter — PDF assembly via matplotlib PdfPages"
```

---

## Task 8: CLI and Integration

**Files:**
- Create: `utils/cli.py`
- Create: `tests/utils/test_cli.py`

- [ ] **Step 1: Write the failing tests**

`tests/utils/test_cli.py`:
```python
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


DATA_DIR = Path(__file__).parent.parent.parent / "data"
RIO_CSV = DATA_DIR / "GACMP_E5-rio.csv"
CARNIVORE_CSV = DATA_DIR / "GACMP_E5_CARNIVORE.csv"


def test_cli_missing_file_exits_nonzero(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(tmp_path / "nonexistent.csv")],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "not found" in result.stderr


def test_cli_produces_pdf_from_real_data(tmp_path: Path) -> None:
    if not RIO_CSV.exists():
        import pytest
        pytest.skip("real data files not present")
    output = tmp_path / "report.pdf"
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(RIO_CSV), str(CARNIVORE_CSV),
         "--output", str(output)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.exists()
    assert output.stat().st_size > 0


def test_cli_per_motor_flag_accepted(tmp_path: Path) -> None:
    if not RIO_CSV.exists():
        import pytest
        pytest.skip("real data files not present")
    output = tmp_path / "report_pm.pdf"
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(RIO_CSV), str(CARNIVORE_CSV),
         "--output", str(output), "--per-motor-graphs"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
poetry run pytest tests/utils/test_cli.py -v
```

Expected: `ModuleNotFoundError: No module named 'utils.cli'`

- [ ] **Step 3: Implement cli.py**

`utils/cli.py`:
```python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import loader, trimmer, analyzer, reporter


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m utils",
        description="Generate FRC motor power analysis PDF from telemetry CSVs.",
    )
    parser.add_argument(
        "files", nargs="+", type=Path, metavar="FILE",
        help="CSV telemetry files (one or more; multiple matches are supported)",
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=Path("report.pdf"),
        help="Output PDF path (default: report.pdf)",
    )
    parser.add_argument(
        "--per-motor-graphs", action="store_true",
        help="Include a per-motor graph page for each motor",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.5,
        help="Voltage threshold (V) for match start/end detection (default: 0.5)",
    )
    args = parser.parse_args()

    missing = [f for f in args.files if not f.exists()]
    if missing:
        for f in missing:
            print(f"error: file not found: {f}", file=sys.stderr)
        sys.exit(1)

    match_groups = loader.find_matches(args.files)
    matches = []

    for match_id, files in sorted(match_groups.items()):
        print(f"Loading {match_id} ({len(files)} file(s))...")
        df = loader.load_match(files)
        df = trimmer.trim_to_match(df, voltage_threshold=args.threshold)
        df = analyzer.normalize(df)
        match = analyzer.build_match(match_id, df)
        matches.append(match)
        print(f"  {len(match.motors)} motors · {match.timestamps[-1]:.1f}s match duration")

    print(f"Building report → {args.output}")
    reporter.build_report(matches, args.output, per_motor=args.per_motor_graphs)
    print("Done.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run all tests**

```bash
poetry run pytest -v
```

Expected: all tests pass (the real-data CLI tests will skip if `data/` files are absent from the environment).

- [ ] **Step 5: Smoke test against real data**

```bash
poetry run python -m utils data/GACMP_E5-rio.csv data/GACMP_E5_CARNIVORE.csv --output report.pdf
```

Expected output:
```
Loading GACMP_E5 (2 file(s))...
  N motors · 150.Xs match duration
Building report → report.pdf
Done.
```

Open `report.pdf` and verify: cover page, summary table, voltage/current/power graphs, cumulative energy curves.

- [ ] **Step 6: Commit**

```bash
git add utils/cli.py tests/utils/test_cli.py
git commit -m "feat: add CLI entry point and integration smoke test"
```

---

## Task 9: Full Test Suite Verification

- [ ] **Step 1: Run the complete test suite**

```bash
poetry run pytest -v
```

Expected: all unit tests pass. CLI integration tests may skip if real data is absent — that is acceptable.

- [ ] **Step 2: Verify `python -m utils --help` works**

```bash
poetry run python -m utils --help
```

Expected: argparse help text printed, exit 0.

- [ ] **Step 3: Final commit**

```bash
git add -A
git status  # confirm nothing unexpected is staged
git commit -m "feat: complete motor power analysis utils package"
```
