# Motor Table Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Combine the two per-match stat tables into one, drop noisy columns, add device temperature with threshold color highlighting, and remove CSV input support in favour of hoot-only ingestion.

**Architecture:** Add `DeviceTemp` to `MOTOR_COL_PATTERN` and `MotorData`; migrate `MOTOR_COL_PATTERN`, `get_motor_ids`, `extract_match_id`, `find_matches`, and `merge_dataframes` into `hoot_loader.py`; delete `loader.py`; rewrite `cli.py` to accept only `.hoot` files; replace `_stat_rows_motor` + `_stat_rows_supply` in `reporter.py` with a single `_stat_rows_combined` that also returns a color dict for temperature flagging.

**Tech Stack:** Python 3.11+, numpy, pandas, matplotlib, pytest

---

## File Map

| File | Change |
|------|--------|
| `utils/loader.py` | **Deleted** |
| `utils/hoot_loader.py` | Absorb `MOTOR_COL_PATTERN` (+ DeviceTemp), `get_motor_ids`, `extract_match_id`, `find_matches`, `merge_dataframes` |
| `utils/models.py` | Add `device_temp: np.ndarray \| None = None` to `MotorData` |
| `utils/analyzer.py` | Change import to `hoot_loader`; extract `DeviceTemp` in `compute_motor_data` |
| `utils/reporter.py` | Replace `_stat_rows_motor` + `_stat_rows_supply` with `_stat_rows_combined`; add `cell_colors` param to `_render_table`; update `_build_single` + `_build_multi` |
| `utils/cli.py` | Remove CSV branch; validate `.hoot`-only input; use `hoot_loader.find_matches` + `merge_dataframes` |
| `tests/utils/test_loader.py` | **Deleted** |
| `tests/utils/test_hoot_loader.py` | Add tests for moved functions + DeviceTemp pattern |
| `tests/utils/test_analyzer.py` | Add `device_temp` coverage |
| `tests/utils/test_reporter.py` | Add combined-table + color tests |
| `tests/utils/test_cli.py` | Remove CSV-based tests; add non-hoot rejection test |
| `tests/utils/conftest.py` | Remove CSV fixtures (`rio_csv`, `carnivore_csv`, CSV strings) |

---

## Task 1: Add DeviceTemp to MOTOR_COL_PATTERN

**Files:**
- Modify: `utils/loader.py:13-15`
- Modify: `tests/utils/test_loader.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/utils/test_loader.py`:

```python
def test_motor_col_pattern_matches_device_temp() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/DeviceTemp")


def test_motor_col_pattern_device_temp_captures_motor_id() -> None:
    m = MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-42/DeviceTemp")
    assert m is not None
    assert m.group(1) == "42"
```

- [ ] **Step 2: Run to confirm failure**

```
cd /Users/y0shi/workspace/ignite/flashpoint
python -m pytest tests/utils/test_loader.py::test_motor_col_pattern_matches_device_temp tests/utils/test_loader.py::test_motor_col_pattern_device_temp_captures_motor_id -v
```

Expected: FAIL — pattern does not match `DeviceTemp`

- [ ] **Step 3: Update MOTOR_COL_PATTERN in loader.py**

Replace lines 13–15 of `utils/loader.py`:

```python
MOTOR_COL_PATTERN = re.compile(
    r"Phoenix6/TalonFX-(\d+)/(MotorVoltage|StatorCurrent|SupplyVoltage|SupplyCurrent|Velocity|DeviceTemp)"
)
```

- [ ] **Step 4: Run tests to confirm pass**

```
python -m pytest tests/utils/test_loader.py -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add utils/loader.py tests/utils/test_loader.py
git commit -m "feat: add DeviceTemp to MOTOR_COL_PATTERN"
```

---

## Task 2: Add device_temp to MotorData and compute_motor_data

**Files:**
- Modify: `utils/models.py`
- Modify: `utils/analyzer.py`
- Modify: `tests/utils/test_analyzer.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/utils/test_analyzer.py`:

```python
def _make_df_with_temp() -> pd.DataFrame:
    return pd.DataFrame({
        "Timestamp": [0.0, 0.02, 0.04],
        "Phoenix6/TalonFX-1/MotorVoltage": [5.0, 6.0, 3.0],
        "Phoenix6/TalonFX-1/StatorCurrent": [2.0, 3.0, 1.0],
        "Phoenix6/TalonFX-1/DeviceTemp": [40.0, 42.0, 41.0],
        "Phoenix6/TalonFX-2/MotorVoltage": [4.0, 5.0, 2.0],
        "Phoenix6/TalonFX-2/StatorCurrent": [1.5, 2.0, 0.5],
    })


def test_device_temp_populated_when_column_present() -> None:
    motors = compute_motor_data(_make_df_with_temp())
    assert motors["TalonFX-1"].device_temp is not None
    np.testing.assert_array_almost_equal(
        motors["TalonFX-1"].device_temp,
        [40.0, 42.0, 41.0],
    )


def test_device_temp_none_when_column_absent() -> None:
    motors = compute_motor_data(normalize(_make_df()))
    assert motors["TalonFX-1"].device_temp is None


def test_compute_totals_leaves_device_temp_none() -> None:
    motors = compute_motor_data(_make_df_with_temp())
    timestamps = np.array([0.0, 0.02, 0.04])
    totals = compute_totals(motors, timestamps)
    assert totals.device_temp is None
```

- [ ] **Step 2: Run to confirm failure**

```
python -m pytest tests/utils/test_analyzer.py::test_device_temp_populated_when_column_present tests/utils/test_analyzer.py::test_device_temp_none_when_column_absent tests/utils/test_analyzer.py::test_compute_totals_leaves_device_temp_none -v
```

Expected: FAIL — `MotorData` has no `device_temp` field

- [ ] **Step 3: Add device_temp to MotorData**

Replace the full content of `utils/models.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

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
    # None when source does not contain these signals.
    supply_voltage: np.ndarray | None = None
    supply_current: np.ndarray | None = None
    supply_power: np.ndarray | None = None
    supply_energy: np.ndarray | None = None  # cumulative Wh

    rotor_velocity: np.ndarray | None = None  # RPS
    device_temp: np.ndarray | None = None     # °C


@dataclass
class Match:
    """A single FRC match worth of trimmed, normalized motor data."""

    match_id: str
    timestamps: np.ndarray        # seconds, re-zeroed to match start
    motors: dict[str, MotorData]  # keyed by "TalonFX-{id}"
    totals: MotorData              # robot-wide sum across all motors
```

- [ ] **Step 4: Extract DeviceTemp in compute_motor_data**

In `utils/analyzer.py`, update `compute_motor_data` to add temp extraction. Replace lines 33–71:

```python
def compute_motor_data(df: pd.DataFrame) -> dict[str, MotorData]:
    """Build a MotorData instance for each motor found in the DataFrame."""
    n = len(df)
    zeros = np.zeros(n)
    timestamps = df["Timestamp"].to_numpy()
    motors: dict[str, MotorData] = {}

    for motor_id in get_motor_ids(df):
        mv_col = _col(motor_id, "MotorVoltage")
        sc_col = _col(motor_id, "StatorCurrent")
        sv_col = _col(motor_id, "SupplyVoltage")
        su_col = _col(motor_id, "SupplyCurrent")
        vel_col = _col(motor_id, "Velocity")
        temp_col = _col(motor_id, "DeviceTemp")

        mv = df[mv_col].ffill().fillna(0.0).to_numpy() if mv_col in df.columns else zeros.copy()
        sc = df[sc_col].ffill().fillna(0.0).to_numpy() if sc_col in df.columns else zeros.copy()
        mp = mv * sc

        has_supply = sv_col in df.columns and su_col in df.columns
        has_velocity = vel_col in df.columns
        has_temp = temp_col in df.columns

        sv = df[sv_col].ffill().fillna(0.0).to_numpy() if has_supply else None
        su = df[su_col].ffill().fillna(0.0).to_numpy() if has_supply else None
        sp = sv * su if has_supply else None
        vel = df[vel_col].ffill().fillna(0.0).to_numpy() if has_velocity else None
        temp = df[temp_col].ffill().fillna(0.0).to_numpy() if has_temp else None

        motors[motor_id] = MotorData(
            motor_voltage=mv,
            stator_current=sc,
            motor_power=mp,
            motor_energy=_cumulative_energy(mp, timestamps),
            supply_voltage=sv,
            supply_current=su,
            supply_power=sp,
            supply_energy=_cumulative_energy(sp, timestamps) if has_supply else None,
            rotor_velocity=vel,
            device_temp=temp,
        )

    return motors
```

- [ ] **Step 5: Run tests**

```
python -m pytest tests/utils/test_analyzer.py -v
```

Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add utils/models.py utils/analyzer.py tests/utils/test_analyzer.py
git commit -m "feat: add device_temp field to MotorData, extract DeviceTemp in compute_motor_data"
```

---

## Task 3: Migrate to hoot-only — move helpers into hoot_loader, delete loader, update cli

**Files:**
- Modify: `utils/hoot_loader.py`
- Modify: `utils/analyzer.py`
- Modify: `utils/cli.py`
- Delete: `utils/loader.py`
- Delete: `tests/utils/test_loader.py`
- Modify: `tests/utils/conftest.py`
- Modify: `tests/utils/test_hoot_loader.py`
- Modify: `tests/utils/test_cli.py`

- [ ] **Step 1: Add moved helpers + tests to test_hoot_loader.py**

The functions `extract_match_id`, `find_matches`, `get_motor_ids`, `merge_dataframes` will move to `hoot_loader.py`. Write tests for them first in `tests/utils/test_hoot_loader.py`. Add these tests at the bottom of the file:

```python
from utils.hoot_loader import (
    MOTOR_COL_PATTERN,
    extract_match_id,
    find_matches,
    get_motor_ids,
    merge_dataframes,
)


# --- MOTOR_COL_PATTERN ---

def test_motor_col_pattern_matches_device_temp() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/DeviceTemp")


def test_motor_col_pattern_device_temp_captures_motor_id() -> None:
    m = MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-42/DeviceTemp")
    assert m is not None
    assert m.group(1) == "42"


def test_motor_col_pattern_matches_velocity() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/Velocity")


def test_motor_col_pattern_does_not_match_unknown_signal() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/Temperature") is None


# --- extract_match_id ---

def test_extract_match_id_strips_timestamp_suffix(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_Q1_rio_2026-04-23_14-30-00.hoot"
    assert extract_match_id(p) == "GACMP_Q1_rio"


def test_extract_match_id_strips_uuid(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_a1b2c3d4-e29b-41d4-a716-446655440000.hoot"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_strips_hex32(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_6E9415C3394C485320202050101C18FF_filtered.hoot"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_fallback_dash(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5-rio.hoot"
    assert extract_match_id(p) == "GACMP_E5"


# --- find_matches ---

def test_find_matches_groups_by_match_id(tmp_path: Path) -> None:
    f1 = tmp_path / "GACMP_E5-rio.hoot"
    f2 = tmp_path / "GACMP_E5_6E9415C3394C485320202050101C18FF.hoot"
    f1.touch()
    f2.touch()
    groups = find_matches([f1, f2])
    assert list(groups.keys()) == ["GACMP_E5"]
    assert len(groups["GACMP_E5"]) == 2


def test_find_matches_separates_different_matches(tmp_path: Path) -> None:
    f1 = tmp_path / "GACMP_Q1-rio.hoot"
    f2 = tmp_path / "GACMP_Q2-rio.hoot"
    f1.touch()
    f2.touch()
    groups = find_matches([f1, f2])
    assert len(groups) == 2


# --- get_motor_ids ---

def test_get_motor_ids_returns_sorted_list() -> None:
    df = pd.DataFrame(columns=[
        "Timestamp",
        "Phoenix6/TalonFX-11/MotorVoltage",
        "Phoenix6/TalonFX-1/MotorVoltage",
    ])
    ids = get_motor_ids(df)
    nums = [int(mid.split("-")[1]) for mid in ids]
    assert nums == sorted(nums)
    assert "TalonFX-1" in ids
    assert "TalonFX-11" in ids


# --- merge_dataframes ---

def test_merge_dataframes_single_df_sorted() -> None:
    df = pd.DataFrame({
        "Timestamp": [0.04, 0.02, 0.0],
        "Phoenix6/TalonFX-1/MotorVoltage": [3.0, 2.0, 1.0],
    })
    result = merge_dataframes([df])
    assert result["Timestamp"].is_monotonic_increasing


def test_merge_dataframes_outer_joins_two_dfs() -> None:
    df1 = pd.DataFrame({
        "Timestamp": [0.0, 0.02],
        "Phoenix6/TalonFX-1/MotorVoltage": [5.0, 6.0],
    })
    df2 = pd.DataFrame({
        "Timestamp": [0.0, 0.02],
        "Phoenix6/TalonFX-11/MotorVoltage": [3.0, 4.0],
    })
    result = merge_dataframes([df1, df2])
    assert "Phoenix6/TalonFX-1/MotorVoltage" in result.columns
    assert "Phoenix6/TalonFX-11/MotorVoltage" in result.columns
    assert result["Timestamp"].is_monotonic_increasing
```

- [ ] **Step 2: Run to confirm failure**

```
python -m pytest tests/utils/test_hoot_loader.py::test_motor_col_pattern_matches_device_temp tests/utils/test_hoot_loader.py::test_extract_match_id_strips_timestamp_suffix tests/utils/test_hoot_loader.py::test_get_motor_ids_returns_sorted_list tests/utils/test_hoot_loader.py::test_merge_dataframes_single_df_sorted -v
```

Expected: FAIL — these names are not exported from `hoot_loader`

- [ ] **Step 3: Update utils/hoot_loader.py**

Replace the full content of `utils/hoot_loader.py`:

```python
from __future__ import annotations

import hashlib
import platform
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd


MOTOR_COL_PATTERN = re.compile(
    r"Phoenix6/TalonFX-(\d+)/(MotorVoltage|StatorCurrent|SupplyVoltage|SupplyCurrent|Velocity|DeviceTemp)"
)

_UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
_HEX32_PATTERN = re.compile(r"[0-9A-Fa-f]{32}")

_REPO_ROOT = Path(__file__).parent.parent


def get_motor_ids(df: pd.DataFrame) -> list[str]:
    """Return sorted list of unique TalonFX motor IDs found in DataFrame columns."""
    ids: set[str] = set()
    for col in df.columns:
        m = MOTOR_COL_PATTERN.match(col)
        if m:
            ids.add(f"TalonFX-{m.group(1)}")
    return sorted(ids, key=lambda x: int(x.split("-")[1]))


def extract_match_id(path: Path) -> str:
    """Extract match ID from a hoot filename."""
    stem = path.stem
    stem = re.sub(r"[_-](filtered|raw)$", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"[_-]\d{4}-\d{2}-\d{2}.*$", "", stem)
    uuid_match = _UUID_PATTERN.search(stem)
    if uuid_match:
        return stem[: uuid_match.start()].rstrip("-_")
    hex_match = _HEX32_PATTERN.search(stem)
    if hex_match:
        return stem[: hex_match.start()].rstrip("-_")
    parts = re.split(r"[-_]", stem)
    return "_".join(parts[:-1]) if len(parts) > 1 else stem


def find_matches(paths: list[Path]) -> dict[str, list[Path]]:
    """Group hoot files by match ID."""
    groups: dict[str, list[Path]] = {}
    for path in paths:
        match_id = extract_match_id(path)
        groups.setdefault(match_id, []).append(path)
    return groups


def merge_dataframes(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Outer-join a list of DataFrames on Timestamp and sort."""
    if len(dfs) == 1:
        return dfs[0].sort_values("Timestamp").reset_index(drop=True)
    merged = dfs[0]
    for df in dfs[1:]:
        merged = pd.merge(merged, df, on="Timestamp", how="outer")
    return merged.sort_values("Timestamp").reset_index(drop=True)


def _file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def _owlet_path() -> Path:
    name = {
        "Darwin": "owlet-2026-mac",
        "Windows": "owlet-2026-win.exe",
    }.get(platform.system(), "owlet-2026-linux")
    return _REPO_ROOT / "executables" / name


def _pivot_records(records) -> pd.DataFrame:  # type: ignore[type-arg]
    """Walk a DataLogReader iterable, keep only TalonFX double entries, pivot to wide format."""
    entries: dict[int, str] = {}
    data: dict[float, dict[str, float]] = {}

    for record in records:
        if record.isStart():
            try:
                sd = record.getStartData()
                if MOTOR_COL_PATTERN.match(sd.name) and sd.type == "double":
                    entries[sd.entry] = sd.name
            except TypeError:
                pass
        elif record.isFinish() or record.isSetMetadata() or record.isControl():
            continue
        else:
            col = entries.get(record.entry)
            if col is None:
                continue
            try:
                ts_s = record.timestamp * 1e-6
                data.setdefault(ts_s, {})[col] = record.getDouble()
            except TypeError:
                pass

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(data, orient="index")
    df.index.name = "Timestamp"
    return df.sort_index().reset_index()


def _run_owlet(hoot_path: Path, wpilog_path: Path) -> None:
    exe = _owlet_path()
    if not exe.exists():
        print(f"error: owlet executable not found: {exe}", file=sys.stderr)
        sys.exit(1)
    result = subprocess.run(
        [str(exe), "-f", "wpilog", str(hoot_path), str(wpilog_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        if wpilog_path.exists() and wpilog_path.stat().st_size > 0:
            print(
                f"  warning: owlet reported an error on {hoot_path.name} (truncated log?), "
                f"proceeding with partial output",
                file=sys.stderr,
            )
        else:
            print(
                f"error: owlet failed on {hoot_path.name}:\n{result.stderr}",
                file=sys.stderr,
            )
            sys.exit(1)


def _wpilog_to_df(path: Path) -> pd.DataFrame:
    import mmap
    from datalog import DataLogReader  # repo-root module, importable from project root

    with open(path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            return _pivot_records(DataLogReader(mm))


def convert_hoot(path: Path, cache_dir: Path) -> Path | None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_csv = cache_dir / f"{_file_hash(path)}.csv"

    if cache_csv.exists():
        try:
            pd.read_csv(cache_csv, nrows=1)
            return cache_csv
        except Exception:
            pass

    with tempfile.NamedTemporaryFile(suffix=".wpilog", delete=False) as tmp:
        wpilog_path = Path(tmp.name)

    try:
        _run_owlet(path, wpilog_path)
        df = _wpilog_to_df(wpilog_path)
    finally:
        wpilog_path.unlink(missing_ok=True)

    if df.empty:
        print(
            f"warning: no TalonFX entries found in {path.name}, skipping",
            file=sys.stderr,
        )
        return None

    df.to_csv(cache_csv, index=False)
    return cache_csv
```

- [ ] **Step 4: Update analyzer.py import**

Replace line 6 of `utils/analyzer.py`:

```python
from .hoot_loader import MOTOR_COL_PATTERN, get_motor_ids
```

- [ ] **Step 5: Rewrite cli.py**

Replace the full content of `utils/cli.py`:

```python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from . import trimmer, analyzer, reporter, hoot_loader
from . import names as _names


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m utils",
        description="Generate FRC motor power analysis PDF from hoot telemetry logs.",
    )
    parser.add_argument(
        "files", nargs="+", type=Path, metavar="FILE",
        help="Telemetry files (.hoot; one or more; multiple matches are supported)",
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
    parser.add_argument(
        "--motor-names", type=Path, default=None, metavar="PATH",
        help="TOML config mapping TalonFX IDs to display names (optional)",
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=Path("converted_data/power_cache"),
        metavar="DIR",
        help="Directory for cached hoot conversions (default: converted_data/power_cache/)",
    )
    args = parser.parse_args()

    missing = [f for f in args.files if not f.exists()]
    if missing:
        for f in missing:
            print(f"error: file not found: {f}", file=sys.stderr)
        sys.exit(1)

    non_hoot = [f for f in args.files if f.suffix != ".hoot"]
    if non_hoot:
        for f in non_hoot:
            print(f"error: expected .hoot file, got: {f.name}", file=sys.stderr)
        sys.exit(1)

    match_groups = hoot_loader.find_matches(args.files)
    matches = []

    for match_id, files in sorted(match_groups.items()):
        print(f"Loading {match_id} ({len(files)} file(s))...")

        dfs: list[pd.DataFrame] = []
        for f in files:
            cached = hoot_loader.convert_hoot(f, args.cache_dir)
            if cached:
                dfs.append(pd.read_csv(cached, na_values=["null"]))
            else:
                print(f"  warning: skipping {f.name} (conversion produced no usable data)", file=sys.stderr)

        if not dfs:
            print(f"  No usable files for {match_id}, skipping.", file=sys.stderr)
            continue

        df = hoot_loader.merge_dataframes(dfs)
        df = trimmer.trim_to_match(df, voltage_threshold=args.threshold)
        df = analyzer.normalize(df)
        match = analyzer.build_match(match_id, df)
        matches.append(match)
        print(f"  {len(match.motors)} motors · {match.timestamps[-1]:.1f}s match duration")

    if not matches:
        print("error: no usable matches found in provided files", file=sys.stderr)
        sys.exit(1)

    names_config = _names.load(args.motor_names) if args.motor_names else None
    print(f"Building report → {args.output}")
    reporter.build_report(matches, args.output, per_motor=args.per_motor_graphs, names_config=names_config)
    print("Done.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Delete loader.py and test_loader.py**

```bash
rm utils/loader.py
rm tests/utils/test_loader.py
```

- [ ] **Step 7: Remove CSV fixtures from conftest.py**

Remove the following from `tests/utils/conftest.py`: the `MINIMAL_RIO_CSV` string, the `MINIMAL_CARNIVORE_UUID` constant, the `MINIMAL_CARNIVORE_CSV` string, the `rio_csv` fixture, and the `carnivore_csv` fixture. The `_make_motor_data`, `simple_match`, `simple_match_with_supply`, and `simple_match_with_velocity` fixtures stay untouched.

Also remove the unused `Path` import if it's no longer needed (it is still needed for the return type annotations, so leave it).

The resulting `conftest.py` should be:

```python
from __future__ import annotations
from pathlib import Path

import numpy as np
from typing import Any
import pytest

from utils.models import Match, MotorData


def _make_motor_data(n: int = 10, with_supply: bool = False, with_velocity: bool = False) -> MotorData:
    power = np.ones(n) * 100.0
    energy = np.cumsum(power * 0.02) / 3600
    kwargs: dict[str, Any] = dict(
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
    if with_velocity:
        kwargs["rotor_velocity"] = np.linspace(5.0, 15.0, n)
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


@pytest.fixture
def simple_match_with_velocity() -> Match:
    n = 150  # 3s of data at 50Hz — matches WATTS_PER_RPS_SMOOTH_S window
    t = np.arange(n) * 0.02
    data = _make_motor_data(n, with_supply=True, with_velocity=True)
    motors = {"TalonFX-1": data, "TalonFX-2": data}
    totals = MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=data.motor_power * 2,
        motor_energy=data.motor_energy * 2,
        supply_voltage=np.zeros(n),
        supply_current=np.zeros(n),
        supply_power=data.supply_power * 2,  # type: ignore[operator]
        supply_energy=data.supply_energy * 2,  # type: ignore[operator]
    )
    return Match(match_id="TEST_VEL", timestamps=t, motors=motors, totals=totals)
```

- [ ] **Step 8: Update test_cli.py**

Replace the full content of `tests/utils/test_cli.py`:

```python
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOOT_FILES = list(DATA_DIR.glob("*.hoot"))


def test_cli_missing_file_exits_nonzero(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(tmp_path / "nonexistent.hoot")],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "not found" in result.stderr


def test_cli_rejects_non_hoot_file(tmp_path: Path) -> None:
    csv_file = tmp_path / "data.csv"
    csv_file.touch()
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(csv_file)],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "expected .hoot" in result.stderr


def test_cli_produces_pdf_from_real_data(tmp_path: Path) -> None:
    if not HOOT_FILES:
        import pytest
        pytest.skip("no .hoot files present in data/")
    output = tmp_path / "report.pdf"
    result = subprocess.run(
        [sys.executable, "-m", "utils", *[str(f) for f in HOOT_FILES],
         "--output", str(output)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.exists()
    assert output.stat().st_size > 0


def test_cli_per_motor_flag_accepted(tmp_path: Path) -> None:
    if not HOOT_FILES:
        import pytest
        pytest.skip("no .hoot files present in data/")
    output = tmp_path / "report_pm.pdf"
    result = subprocess.run(
        [sys.executable, "-m", "utils", *[str(f) for f in HOOT_FILES],
         "--output", str(output), "--per-motor-graphs"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.exists()


def test_cli_cache_dir_arg_is_recognized(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable, "-m", "utils",
            str(tmp_path / "nonexistent.hoot"),
            "--cache-dir", str(tmp_path / "cache"),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "not found" in result.stderr
    assert "unrecognized" not in result.stderr
```

- [ ] **Step 9: Run full test suite**

```
python -m pytest tests/utils/ -v
```

Expected: all PASS (test_loader.py is deleted; CSV-based CLI tests are now skip or pass)

- [ ] **Step 10: Commit**

```bash
git add utils/hoot_loader.py utils/analyzer.py utils/cli.py
git add tests/utils/test_hoot_loader.py tests/utils/test_cli.py tests/utils/conftest.py
git rm utils/loader.py tests/utils/test_loader.py
git commit -m "feat: migrate to hoot-only input, move MOTOR_COL_PATTERN and helpers into hoot_loader"
```

---

## Task 4: Combined stat table with temperature color highlighting

**Files:**
- Modify: `utils/reporter.py`
- Modify: `tests/utils/test_reporter.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/utils/test_reporter.py`:

```python
import numpy as np
from utils.models import Match, MotorData
from utils.reporter import _render_table, _stat_rows_combined


def _make_hot_match(avg_temp: float, max_temp_spike: float | None = None) -> Match:
    n = 10
    t = np.arange(n) * 0.02
    temps = np.ones(n) * avg_temp
    if max_temp_spike is not None:
        temps[-1] = max_temp_spike
    data = MotorData(
        motor_voltage=np.ones(n) * 5.0,
        stator_current=np.ones(n) * 20.0,
        motor_power=np.ones(n) * 100.0,
        motor_energy=np.zeros(n),
        device_temp=temps,
    )
    totals = MotorData(
        motor_voltage=np.zeros(n),
        stator_current=np.zeros(n),
        motor_power=np.ones(n) * 100.0,
        motor_energy=np.zeros(n),
    )
    return Match(match_id="T", timestamps=t, motors={"TalonFX-1": data}, totals=totals)


def test_stat_rows_combined_returns_15_column_headers(simple_match: Match) -> None:
    headers, _, _ = _stat_rows_combined(simple_match)
    assert len(headers) == 15


def test_stat_rows_combined_supply_cols_dash_when_no_supply(simple_match: Match) -> None:
    _, rows, _ = _stat_rows_combined(simple_match)
    motor_row = rows[0]
    # Supply cols are indices 7–12
    assert all(v == "—" for v in motor_row[7:13])


def test_stat_rows_combined_temp_cols_dash_when_no_temp(simple_match: Match) -> None:
    _, rows, _ = _stat_rows_combined(simple_match)
    motor_row = rows[0]
    assert motor_row[13] == "—"
    assert motor_row[14] == "—"


def test_stat_rows_combined_flags_avg_temp_above_55() -> None:
    match = _make_hot_match(avg_temp=60.0)
    _, _, cell_colors = _stat_rows_combined(match)
    assert (1, 13) in cell_colors
    assert cell_colors[(1, 13)] == "#FFCCCC"


def test_stat_rows_combined_does_not_flag_avg_temp_at_55() -> None:
    match = _make_hot_match(avg_temp=55.0)
    _, _, cell_colors = _stat_rows_combined(match)
    assert (1, 13) not in cell_colors


def test_stat_rows_combined_flags_max_temp_above_65() -> None:
    # avg < 55, spike pushes max > 65
    match = _make_hot_match(avg_temp=40.0, max_temp_spike=70.0)
    _, _, cell_colors = _stat_rows_combined(match)
    assert (1, 14) in cell_colors
    assert (1, 13) not in cell_colors


def test_stat_rows_combined_total_row_temp_always_dash() -> None:
    match = _make_hot_match(avg_temp=60.0)
    _, rows, _ = _stat_rows_combined(match)
    total_row = rows[-1]
    assert total_row[13] == "—"
    assert total_row[14] == "—"


def test_render_table_applies_cell_color() -> None:
    fig = _render_table(["A", "B"], [["x", "y"]], "Test", {(1, 1): "#FFCCCC"})
    import matplotlib.pyplot as plt
    plt.close(fig)


def test_build_report_with_temp_data(tmp_path: Path) -> None:
    match = _make_hot_match(avg_temp=60.0)
    output = tmp_path / "report_temp.pdf"
    build_report([match], output)
    assert output.exists()
    assert output.stat().st_size > 0
```

- [ ] **Step 2: Run to confirm failure**

```
python -m pytest tests/utils/test_reporter.py::test_stat_rows_combined_returns_15_column_headers tests/utils/test_reporter.py::test_stat_rows_combined_flags_avg_temp_above_55 -v
```

Expected: FAIL — `_stat_rows_combined` does not exist

- [ ] **Step 3: Replace _stat_rows_motor and _stat_rows_supply with _stat_rows_combined**

In `utils/reporter.py`, replace the `_stat_rows_motor` function (lines 67–94) and the `_stat_rows_supply` function (lines 97–127) with:

```python
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
```

- [ ] **Step 4: Update _render_table to accept cell_colors**

Replace the `_render_table` function (lines 160–169 in the original, now shifted by removing the two old stat functions) with:

```python
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
```

- [ ] **Step 5: Update _build_single**

Replace the body of `_build_single` with:

```python
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
```

- [ ] **Step 6: Update _build_multi**

Replace the per-match table section in `_build_multi`. Remove the `any_supply` variable and the two separate loops that render motor and supply tables. Replace those loops with:

```python
    for match in matches:
        mn = _resolve_names(match.match_id, names_config)
        headers, rows, cell_colors = _stat_rows_combined(match, mn)
        fig = _render_table(headers, rows, f"{match.match_id} — Motor Stats", cell_colors)
        pdf.savefig(fig)
        plt.close(fig)
```

The `any_supply` variable is still needed for the heatmap and cumulative energy sections below that point, so keep it defined at the top of `_build_multi`.

- [ ] **Step 7: Run tests**

```
python -m pytest tests/utils/test_reporter.py -v
```

Expected: all PASS

- [ ] **Step 8: Run full suite**

```
python -m pytest tests/utils/ -v
```

Expected: all PASS

- [ ] **Step 9: Commit**

```bash
git add utils/reporter.py tests/utils/test_reporter.py
git commit -m "feat: combine output/supply tables, add device temp with color flagging"
```
