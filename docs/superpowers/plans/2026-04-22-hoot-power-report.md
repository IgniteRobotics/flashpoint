# Hoot Power Report Automation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable `python -m utils` to accept `.hoot` log files directly as inputs, converting them automatically via the owlet executable and caching the result, while preserving the existing `.csv` input path unchanged.

**Architecture:** A new `utils/hoot_loader.py` module handles hoot → wide-format DataFrame conversion (owlet subprocess → DataLogReader walk → pivot to Timestamp-indexed columns). `utils/cli.py` is updated to detect `.hoot` inputs and route them through `hoot_loader.convert_hoot()` before handing off to the existing trim/analyze/report pipeline.

**Tech Stack:** Python 3.11, pandas, subprocess (owlet executable), `datalog.DataLogReader` (repo root module), hashlib (SHA-256 cache keys), tempfile.

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `utils/hoot_loader.py` | Create | All hoot conversion logic |
| `utils/cli.py` | Modify | Add `--cache-dir` arg, route `.hoot` inputs |
| `tests/utils/test_hoot_loader.py` | Create | Unit tests for hoot_loader |
| `tests/utils/test_cli.py` | Modify | Test `--cache-dir` arg acceptance |

---

### Task 1: Create `utils/hoot_loader.py` — stub with cache-hit path

**Files:**
- Create: `utils/hoot_loader.py`
- Create: `tests/utils/test_hoot_loader.py`

- [ ] **Step 1: Write failing test for cache hit**

```python
# tests/utils/test_hoot_loader.py
from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from utils.hoot_loader import convert_hoot


def test_convert_hoot_returns_cache_path_on_hit(tmp_path: Path) -> None:
    hoot = tmp_path / "GACMP_Q1_rio_2025-01-01_00-00-00.hoot"
    hoot.write_bytes(b"fake hoot content")

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    h = hashlib.sha256(b"fake hoot content").hexdigest()
    cache_csv = cache_dir / f"{h}.csv"
    cache_csv.write_text(
        "Timestamp,Phoenix6/TalonFX-1/MotorVoltage\n0.0,5.0\n"
    )

    result = convert_hoot(hoot, cache_dir)

    assert result == cache_csv
```

- [ ] **Step 2: Run test to verify it fails**

```
pytest tests/utils/test_hoot_loader.py::test_convert_hoot_returns_cache_path_on_hit -v
```

Expected: `ModuleNotFoundError` or `ImportError` — `utils.hoot_loader` does not exist yet.

- [ ] **Step 3: Write the stub module**

```python
# utils/hoot_loader.py
from __future__ import annotations

import hashlib
import platform
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

from .loader import MOTOR_COL_PATTERN


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
    return Path("executables") / name


def convert_hoot(path: Path, cache_dir: Path) -> Path | None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_csv = cache_dir / f"{_file_hash(path)}.csv"

    if cache_csv.exists():
        try:
            pd.read_csv(cache_csv, nrows=1)
            return cache_csv
        except Exception:
            pass  # corrupt — fall through to re-convert

    return None  # conversion not yet implemented
```

- [ ] **Step 4: Run test to verify it passes**

```
pytest tests/utils/test_hoot_loader.py::test_convert_hoot_returns_cache_path_on_hit -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add utils/hoot_loader.py tests/utils/test_hoot_loader.py
git commit -m "feat: add hoot_loader stub with cache-hit path"
```

---

### Task 2: Implement `_pivot_records` — pure record-to-DataFrame logic

**Files:**
- Modify: `utils/hoot_loader.py`
- Modify: `tests/utils/test_hoot_loader.py`

- [ ] **Step 1: Add test helpers and failing tests**

Append to `tests/utils/test_hoot_loader.py`:

```python
from utils.hoot_loader import _pivot_records


class _FakeStartData:
    def __init__(self, entry_id: int, name: str, type_: str) -> None:
        self.entry = entry_id
        self.name = name
        self.type = type_


class _FakeRecord:
    def __init__(
        self,
        *,
        is_start: bool = False,
        is_finish: bool = False,
        entry: int = 0,
        timestamp: int = 0,
        value: float | None = None,
        start_data: _FakeStartData | None = None,
    ) -> None:
        self._is_start = is_start
        self._is_finish = is_finish
        self.entry = entry
        self.timestamp = timestamp
        self._value = value
        self._start_data = start_data

    def isStart(self) -> bool: return self._is_start
    def isFinish(self) -> bool: return self._is_finish
    def isSetMetadata(self) -> bool: return False
    def isControl(self) -> bool: return False
    def getStartData(self) -> _FakeStartData: return self._start_data  # type: ignore[return-value]
    def getDouble(self) -> float: return self._value  # type: ignore[return-value]


def _make_records(
    entries: list[tuple[int, str, str]],
    data: list[tuple[int, int, float]],
) -> list[_FakeRecord]:
    """entries=(entry_id, name, type_), data=(entry_id, timestamp_us, value)"""
    records: list[_FakeRecord] = [
        _FakeRecord(is_start=True, start_data=_FakeStartData(eid, name, t))
        for eid, name, t in entries
    ]
    records += [
        _FakeRecord(entry=eid, timestamp=ts, value=val)
        for eid, ts, val in data
    ]
    return records


def test_pivot_records_produces_wide_dataframe() -> None:
    records = _make_records(
        entries=[(1, "Phoenix6/TalonFX-11/MotorVoltage", "double")],
        data=[(1, 1_000_000, 5.0), (1, 2_000_000, 6.0)],
    )
    df = _pivot_records(records)
    assert "Timestamp" in df.columns
    assert "Phoenix6/TalonFX-11/MotorVoltage" in df.columns
    assert list(df["Phoenix6/TalonFX-11/MotorVoltage"]) == [5.0, 6.0]
    assert df["Timestamp"].iloc[0] == pytest.approx(1.0)
    assert df["Timestamp"].iloc[1] == pytest.approx(2.0)


def test_pivot_records_filters_non_talon_entries() -> None:
    records = _make_records(
        entries=[
            (1, "Phoenix6/TalonFX-11/MotorVoltage", "double"),
            (2, "DS:enabled", "boolean"),
            (3, "elevator/Leader Motor Voltage", "double"),
        ],
        data=[
            (1, 1_000_000, 5.0),
            (2, 1_000_000, 1.0),
            (3, 1_000_000, 12.0),
        ],
    )
    df = _pivot_records(records)
    assert "DS:enabled" not in df.columns
    assert "elevator/Leader Motor Voltage" not in df.columns
    assert "Phoenix6/TalonFX-11/MotorVoltage" in df.columns


def test_pivot_records_returns_empty_dataframe_when_no_talon_entries() -> None:
    records = _make_records(
        entries=[(1, "DS:enabled", "boolean")],
        data=[(1, 1_000_000, 1.0)],
    )
    df = _pivot_records(records)
    assert df.empty


def test_pivot_records_sorts_by_timestamp() -> None:
    records = _make_records(
        entries=[(1, "Phoenix6/TalonFX-11/MotorVoltage", "double")],
        data=[
            (1, 3_000_000, 3.0),
            (1, 1_000_000, 1.0),
            (1, 2_000_000, 2.0),
        ],
    )
    df = _pivot_records(records)
    assert df["Timestamp"].is_monotonic_increasing
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest tests/utils/test_hoot_loader.py -k "pivot" -v
```

Expected: `ImportError` — `_pivot_records` not defined.

- [ ] **Step 3: Implement `_pivot_records` in `utils/hoot_loader.py`**

Add after `_owlet_path()`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest tests/utils/test_hoot_loader.py -k "pivot" -v
```

Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add utils/hoot_loader.py tests/utils/test_hoot_loader.py
git commit -m "feat: implement _pivot_records for hoot → wide-format DataFrame"
```

---

### Task 3: Implement `_run_owlet`, `_wpilog_to_df`, and full conversion path

**Files:**
- Modify: `utils/hoot_loader.py`
- Modify: `tests/utils/test_hoot_loader.py`

- [ ] **Step 1: Write failing test for cache miss (full conversion)**

Append to `tests/utils/test_hoot_loader.py`:

```python
def test_convert_hoot_converts_on_cache_miss(tmp_path: Path) -> None:
    hoot = tmp_path / "GACMP_Q1_rio_2025-01-01_00-00-00.hoot"
    hoot.write_bytes(b"fake hoot content")
    cache_dir = tmp_path / "cache"

    sample_df = pd.DataFrame({
        "Timestamp": [0.0, 0.02],
        "Phoenix6/TalonFX-1/MotorVoltage": [5.0, 6.0],
    })

    with (
        patch("utils.hoot_loader._run_owlet"),
        patch("utils.hoot_loader._wpilog_to_df", return_value=sample_df),
    ):
        result = convert_hoot(hoot, cache_dir)

    assert result is not None
    assert result.exists()
    df = pd.read_csv(result)
    assert "Phoenix6/TalonFX-1/MotorVoltage" in df.columns
    assert list(df["Timestamp"]) == pytest.approx([0.0, 0.02])
```

- [ ] **Step 2: Run test to verify it fails**

```
pytest tests/utils/test_hoot_loader.py::test_convert_hoot_converts_on_cache_miss -v
```

Expected: FAIL — `_run_owlet` and `_wpilog_to_df` not defined.

- [ ] **Step 3: Implement `_run_owlet` and `_wpilog_to_df`, wire into `convert_hoot`**

Add to `utils/hoot_loader.py` after `_pivot_records`:

```python
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
        print(
            f"error: owlet failed on {hoot_path.name}:\n{result.stderr}",
            file=sys.stderr,
        )
        sys.exit(1)


def _wpilog_to_df(path: Path) -> pd.DataFrame:
    import mmap
    from datalog import DataLogReader  # repo-root module, importable from project root

    with open(path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        return _pivot_records(DataLogReader(mm))
```

Replace the stub `convert_hoot` body with the full version:

```python
def convert_hoot(path: Path, cache_dir: Path) -> Path | None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_csv = cache_dir / f"{_file_hash(path)}.csv"

    if cache_csv.exists():
        try:
            pd.read_csv(cache_csv, nrows=1)
            return cache_csv
        except Exception:
            pass  # corrupt — fall through to re-convert

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

- [ ] **Step 4: Run all hoot_loader tests to verify they pass**

```
pytest tests/utils/test_hoot_loader.py -v
```

Expected: all PASS (5 pivot tests + 2 convert tests)

- [ ] **Step 5: Commit**

```bash
git add utils/hoot_loader.py tests/utils/test_hoot_loader.py
git commit -m "feat: implement full hoot conversion path via owlet + DataLogReader"
```

---

### Task 4: Error handling — owlet failure, empty result, corrupt cache

**Files:**
- Modify: `utils/hoot_loader.py` (already implemented; tests verify existing behavior)
- Modify: `tests/utils/test_hoot_loader.py`

- [ ] **Step 1: Write failing tests for all error cases**

Append to `tests/utils/test_hoot_loader.py`:

```python
from utils.hoot_loader import _run_owlet


def test_run_owlet_exits_on_owlet_failure(tmp_path: Path) -> None:
    fake_exe = tmp_path / "fake_owlet"
    fake_exe.touch()

    mock_result = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="", stderr="owlet error message"
    )
    with (
        patch("utils.hoot_loader._owlet_path", return_value=fake_exe),
        patch("utils.hoot_loader.subprocess.run", return_value=mock_result),
    ):
        with pytest.raises(SystemExit):
            _run_owlet(tmp_path / "test.hoot", tmp_path / "test.wpilog")


def test_convert_hoot_returns_none_when_no_talon_entries(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    hoot = tmp_path / "GACMP_Q1_rio_2025-01-01_00-00-00.hoot"
    hoot.write_bytes(b"fake hoot content")
    cache_dir = tmp_path / "cache"

    with (
        patch("utils.hoot_loader._run_owlet"),
        patch("utils.hoot_loader._wpilog_to_df", return_value=pd.DataFrame()),
    ):
        result = convert_hoot(hoot, cache_dir)

    assert result is None
    captured = capsys.readouterr()
    assert "warning" in captured.err.lower()


def test_convert_hoot_reconverts_corrupt_cache(tmp_path: Path) -> None:
    hoot = tmp_path / "GACMP_Q1_rio_2025-01-01_00-00-00.hoot"
    hoot.write_bytes(b"fake hoot content")

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    h = hashlib.sha256(b"fake hoot content").hexdigest()
    cache_csv = cache_dir / f"{h}.csv"
    cache_csv.write_bytes(b"\x00\x01\x02NOT VALID CSV")  # corrupt

    sample_df = pd.DataFrame({
        "Timestamp": [0.0],
        "Phoenix6/TalonFX-1/MotorVoltage": [5.0],
    })

    with (
        patch("utils.hoot_loader._run_owlet"),
        patch("utils.hoot_loader._wpilog_to_df", return_value=sample_df),
    ):
        result = convert_hoot(hoot, cache_dir)

    assert result == cache_csv
    df = pd.read_csv(result)
    assert "Phoenix6/TalonFX-1/MotorVoltage" in df.columns
```

Also add `import subprocess` to the top of the test file imports block.

- [ ] **Step 2: Run tests to verify results**

```
pytest tests/utils/test_hoot_loader.py -k "owlet or none or corrupt" -v
```

Expected: all 3 PASS (the behavior is already implemented; tests verify it).

- [ ] **Step 3: Commit**

```bash
git add tests/utils/test_hoot_loader.py
git commit -m "test: add error handling tests for hoot_loader"
```

---

### Task 5: Update `utils/cli.py` — `--cache-dir` arg and hoot routing

**Files:**
- Modify: `utils/cli.py`
- Modify: `tests/utils/test_cli.py`

- [ ] **Step 1: Write failing test**

Append to `tests/utils/test_cli.py`:

```python
def test_cli_cache_dir_arg_is_recognized(tmp_path: Path) -> None:
    """--cache-dir is accepted; a missing .hoot file still produces the file-not-found error."""
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
    # Should be "file not found", NOT "unrecognized argument"
    assert "not found" in result.stderr
    assert "unrecognized" not in result.stderr
```

- [ ] **Step 2: Run test to verify it fails**

```
pytest tests/utils/test_cli.py::test_cli_cache_dir_arg_is_recognized -v
```

Expected: FAIL — `--cache-dir` is not a recognized argument yet, so stderr contains "unrecognized".

- [ ] **Step 3: Update `utils/cli.py`**

Add the import and argument, then update the processing loop.

Replace the import line at the top:

```python
from . import loader, trimmer, analyzer, reporter, hoot_loader
from . import names as _names
```

Add the `--cache-dir` argument after the existing `--motor-names` argument:

```python
    parser.add_argument(
        "--cache-dir", type=Path, default=Path("converted_data/power_cache"),
        metavar="DIR",
        help="Directory for cached hoot conversions (default: converted_data/power_cache/)",
    )
```

Replace the match processing block in `main()`. Before:

```python
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
```

After:

```python
    match_groups = loader.find_matches(args.files)
    matches = []

    for match_id, files in sorted(match_groups.items()):
        print(f"Loading {match_id} ({len(files)} file(s))...")

        resolved: list[Path] = []
        for f in files:
            if f.suffix == ".hoot":
                cached = hoot_loader.convert_hoot(f, args.cache_dir)
                if cached:
                    resolved.append(cached)
            else:
                resolved.append(f)

        if not resolved:
            print(f"  No usable files for {match_id}, skipping.", file=sys.stderr)
            continue

        df = loader.load_match(resolved)
        df = trimmer.trim_to_match(df, voltage_threshold=args.threshold)
        df = analyzer.normalize(df)
        match = analyzer.build_match(match_id, df)
        matches.append(match)
        print(f"  {len(match.motors)} motors · {match.timestamps[-1]:.1f}s match duration")

    if not matches:
        print("error: no usable matches found in provided files", file=sys.stderr)
        sys.exit(1)
```

- [ ] **Step 4: Run all tests to verify nothing is broken**

```
pytest tests/ -v
```

Expected: all existing tests PASS, new CLI test PASS.

- [ ] **Step 5: Commit**

```bash
git add utils/cli.py tests/utils/test_cli.py
git commit -m "feat: add --cache-dir arg and hoot routing to utils CLI"
```

---

## Self-Review

**Spec coverage:**
- ✅ `utils/hoot_loader.py` with `convert_hoot(path, cache_dir) -> Path | None`
- ✅ SHA-256 cache key, `{hash}.csv` filename
- ✅ owlet subprocess via `_run_owlet`, platform detection via `_owlet_path`
- ✅ `DataLogReader` walk in `_wpilog_to_df`, filter via `MOTOR_COL_PATTERN`
- ✅ Pivot to wide-format in `_pivot_records`
- ✅ Temp wpilog via `tempfile.NamedTemporaryFile`, deleted in `finally`
- ✅ Cache hit: returns path without calling owlet
- ✅ Corrupt cache: re-converts and overwrites
- ✅ No TalonFX entries: warn to stderr, return None
- ✅ Owlet not found: `sys.exit(1)` with clear error
- ✅ Owlet non-zero exit: `sys.exit(1)` with stderr output
- ✅ Cache dir auto-created
- ✅ `--cache-dir` CLI arg, default `converted_data/power_cache/`
- ✅ `.hoot` / `.csv` routing in `main()`
- ✅ Empty matches guard added
- ✅ All existing code paths untouched

**No placeholders found.**

**Type consistency:** `convert_hoot` returns `Path | None` throughout. `_pivot_records` takes an untyped iterable (matches DataLogReader's duck-typed interface). `_run_owlet` / `_wpilog_to_df` both use `Path` args consistently.
