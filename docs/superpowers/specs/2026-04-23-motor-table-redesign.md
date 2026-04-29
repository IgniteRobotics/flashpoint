# Motor Table Redesign

**Date:** 2026-04-23
**Branch:** feature/power-tracking

## Summary

Consolidate the two per-match stat tables (Motor Output and Supply) into a single combined table, drop noisy columns, add device temperature with threshold-based color highlighting, and remove CSV input support in favour of hoot-only ingestion.

---

## Data Pipeline Changes

### Drop CSV support

`loader.py` is deleted. `cli.py` drops the CSV branch and only accepts `.hoot` files. The `--cache-dir` argument is retained. All CSV-specific functions (`extract_match_id`, `find_matches`, `load_match`) are removed.

### MOTOR_COL_PATTERN moves to hoot_loader.py

`MOTOR_COL_PATTERN` and `get_motor_ids` move from `loader.py` into `hoot_loader.py`. `DeviceTemp` is added to the pattern:

```
Phoenix6/TalonFX-(\d+)/(MotorVoltage|StatorCurrent|SupplyVoltage|SupplyCurrent|Velocity|DeviceTemp)
```

`analyzer.py` imports these from `hoot_loader` instead of `loader`.

### MotorData model

`device_temp: np.ndarray | None = None` is added to `MotorData`. In `compute_motor_data`, it is extracted with `ffill().fillna(0.0)` when present, otherwise `None`. The `totals` row always has `device_temp=None` — temperature is not additive.

---

## Combined Table

One table per match replaces the current two separate tables. The title is `"{match_id} — Motor Stats"`.

### Columns (15 total)

| Motor | Max Motor V | Avg Motor V | Max Stator A | Avg Stator A | Peak Motor W | P95 Motor W | Max Supply V | Avg Supply V | Max Supply A | Avg Supply A | Peak Supply W | P95 Supply W | Avg Temp °C | Max Temp °C |

### Removed columns

Dropped from both the former output and supply tables:
- Avg Power (W)
- σ Power (W)
- Total Energy (Wh)

### Null handling

- Supply columns show `—` when a motor has no supply data
- Temp columns show `—` when `device_temp` is `None`
- `TOTAL` row: temp columns always show `—`

### Layout

Figure width increases from 12 → 16 inches to accommodate the wider table. Font size stays at 8 or drops to 7 if needed.

---

## Temperature Color Highlighting

`_render_table` gains an optional `cell_colors: dict[tuple[int, int], str] | None = None` parameter. Row 0 is the header; data rows start at 1.

### Thresholds

| Column | Threshold | Color |
|--------|-----------|-------|
| Avg Temp °C | > 55 | `#FFCCCC` (light red) |
| Max Temp °C | > 65 | `#FFCCCC` (light red) |

### Implementation

`_stat_rows_combined` returns `tuple[list[str], list[list[str]], dict[tuple[int,int], str]]` — headers, rows, and the pre-computed color dict. The color logic is centralised there. Callers that don't need colors (e.g. the multi-match energy table) omit the argument.

---

## Files Changed

| File | Change |
|------|--------|
| `utils/loader.py` | **Deleted** |
| `utils/hoot_loader.py` | Absorb `MOTOR_COL_PATTERN` + `get_motor_ids`; add `DeviceTemp` to pattern |
| `utils/models.py` | Add `device_temp: np.ndarray | None = None` to `MotorData` |
| `utils/analyzer.py` | Import from `hoot_loader`; extract `device_temp` in `compute_motor_data` |
| `utils/reporter.py` | Replace `_stat_rows_motor` + `_stat_rows_supply` with `_stat_rows_combined`; update `_render_table` for cell colors; update `_build_single` and `_build_multi` |
| `utils/cli.py` | Remove CSV branch; remove `loader` import |
| `tests/utils/test_loader.py` | Deleted or gutted (CSV-only tests) |
| `tests/utils/test_analyzer.py` | Update fixtures for `device_temp` |
| `tests/utils/test_reporter.py` | Update for combined table signature |
