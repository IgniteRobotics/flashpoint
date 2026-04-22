# Hoot Log Power Report Automation

**Date:** 2026-04-22  
**Branch:** feature/power-tracking  
**Status:** Approved

## Goal

Eliminate the manual CSV export step from the power tracking report workflow. The CLI (`python -m utils`) should accept `.hoot` files directly alongside existing `.csv` inputs.

## Architecture

One new module — `utils/hoot_loader.py` — sits between the raw hoot files and the existing pipeline. Everything downstream (trimmer, analyzer, reporter) is untouched.

```
.hoot file(s)
     │
     ▼
hoot_loader.convert_hoot(path, cache_dir) → DataFrame
     │
     ▼
loader.load_match(files)     ← existing, unchanged
     │
     ▼
trimmer / analyzer / reporter  ← existing, unchanged
```

## New Module: `utils/hoot_loader.py`

### `convert_hoot(path: Path, cache_dir: Path) -> pd.DataFrame`

Converts a single `.hoot` file to a wide-format DataFrame compatible with the existing `load_match` pipeline.

**Steps:**

1. Compute SHA-256 hash of the hoot file.
2. Check `cache_dir / f"{hash}.csv"` — return cached DataFrame if present (re-convert and overwrite on pandas parse error).
3. Run the platform-appropriate owlet executable via subprocess: `.hoot` → `.wpilog` (written to a `tempfile.NamedTemporaryFile` in the OS temp dir, deleted in a `finally` block).
4. Walk `DataLogReader` records from the `.wpilog`. Keep only `double`-typed entries whose name matches `MOTOR_COL_PATTERN` (imported from `loader.py`). Convert timestamps from microseconds to seconds.
5. Pivot to wide-format: rows indexed by timestamp (seconds), one column per matching entry name, plus a `Timestamp` column.
6. Sort by `Timestamp`, write to `cache_dir / f"{hash}.csv"`, delete the temp `.wpilog`.
7. Return the DataFrame.

### Owlet executable selection

Reuses the same platform-detection logic as `ingest_library.py`:

```python
{"Darwin": "owlet-2026-mac", "Windows": "owlet-2026-win.exe"}.get(platform.system(), "owlet-2026-linux")
```

Executables are expected at `./executables/` relative to the repo root (same as today).

## CLI Changes (`utils/cli.py`)

### New argument

```
--cache-dir PATH   Directory for cached converted hoot files (default: ./converted_data/power_cache/)
```

### Input routing

At the top of `main()`, split inputs by extension:

```python
hoot_files = [f for f in args.files if f.suffix == ".hoot"]
csv_files  = [f for f in args.files if f.suffix == ".csv"]
```

Hoot files are converted first (one at a time, with a progress print per file). Each produces a cached CSV on disk. Those CSV paths are appended to `csv_files`, then the full list is passed into the existing `loader.find_matches()`. Mixed inputs (hoot + csv for the same match) work naturally since match grouping is filename-based.

## Filtering

Only `Phoenix6/TalonFX-*` signals are kept during the pivot step, using the existing `MOTOR_COL_PATTERN` from `loader.py`. All other hoot entries (FMS, DS, vision, mechanism telemetry, etc.) are dropped before the DataFrame is built. The cache only stores this filtered wide-format data.

## Caching

- **Cache key:** SHA-256 hash of the hoot file, stored as `{hash}.csv` in the cache dir.
- **Cache hit:** File exists and parses cleanly → return immediately, owlet is not invoked.
- **Cache miss:** Convert, write, return.
- **Corrupt cache:** Pandas parse error on load → re-convert and overwrite.
- **Cache dir:** Created automatically if it doesn't exist.

## Error Handling

| Condition | Behavior |
|-----------|----------|
| Owlet executable not found | Print error naming the file, `sys.exit(1)` |
| Owlet non-zero exit | Print stderr from owlet, `sys.exit(1)` |
| No TalonFX entries found after filtering | Warn to stderr, skip that hoot file (same behavior as empty CSV) |
| Cache dir creation failure | Surface the OS error, `sys.exit(1)` |
| Corrupt/partial cache file | Re-convert and overwrite silently |

## What Is Not Changed

- `utils/loader.py` — no changes
- `utils/trimmer.py` — no changes
- `utils/analyzer.py` — no changes
- `utils/reporter.py` — no changes
- `utils/models.py` — no changes
- The existing CSV input path — fully preserved

## Files Touched

| File | Change |
|------|--------|
| `utils/hoot_loader.py` | New |
| `utils/cli.py` | Add `--cache-dir` arg, hoot routing logic |
