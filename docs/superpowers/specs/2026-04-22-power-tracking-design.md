# Design: Supply Watts per RPS

**Date:** 2026-04-22
**Branch:** feature/power-tracking
**Status:** Approved

## Summary

Add `RotorVelocity` (RPS) as a first-class field in the motor data pipeline, then expose a new per-motor plot of supply watts per RPS over time — a torque proxy useful for identifying stall conditions and load patterns during a match.

## Data Pipeline

### `loader.py` — Column Pattern

Expand `MOTOR_COL_PATTERN` to also match `Velocity`:

```
Phoenix6/TalonFX-{n}/(MotorVoltage|StatorCurrent|SupplyVoltage|SupplyCurrent|Velocity)
```

The hoot loader (`hoot_loader.py`) already uses this pattern to filter wpilog entries, so no changes are needed there — `Phoenix6/TalonFX-{n}/Velocity` will be captured automatically from existing hoot files.

### `models.py` — MotorData

Add one new optional field:

```python
rotor_velocity: np.ndarray | None = None  # rotations per second
```

Placed after `supply_energy` with the other optional supply fields. Defaults to `None` when the signal is absent.

### `analyzer.py` — compute_motor_data

Pick up `Velocity` per motor using the same pattern as supply columns: `ffill().fillna(0.0)`, stored on `MotorData.rotor_velocity`. No changes to `compute_totals` — summing RPS across motors is meaningless, so `totals.rotor_velocity` remains `None`.

## Computation

W/RPS is computed in the plotter at render time, not stored on the model. Steps in order:

1. **Smooth** both `supply_power` and `rotor_velocity` independently via `_smooth_seconds` with `WATTS_PER_RPS_SMOOTH_S = 3.0` seconds (3× the existing 1s default)
2. **Clamp** — set smoothed velocity samples where `|velocity| < VEL_MIN_RPS = 1.0` to `np.nan`
3. **Divide** — `ratio = smoothed_supply_power / smoothed_velocity`; NaN propagates naturally, rendering as gaps in the plot

Constants `WATTS_PER_RPS_SMOOTH_S` and `VEL_MIN_RPS` live at the top of `plotter.py` alongside the existing `SMOOTH_WINDOW_S`.

## Visualization

New function in `plotter.py`:

```python
def plot_watts_per_rps(match: Match, motor_names: dict[str, str] | None = None) -> Figure
```

- One subplot per motor, vertical stack, `sharex=True`
- Only includes motors with both `supply_power` and `rotor_velocity` present
- Each subplot: smoothed W/RPS ratio as a line; NaN gaps render as natural breaks
- No trend line — the ratio is already a derived/smoothed signal
- Title: `{match.match_id} — Supply Watts per RPS (per motor)`
- Y-axis: `W / RPS`, X-axis: `Time (s)`
- Motor labels from `motor_names` if provided, raw ID otherwise

## Files Changed

| File | Change |
|------|--------|
| `utils/loader.py` | Expand `MOTOR_COL_PATTERN` to include `Velocity` |
| `utils/models.py` | Add `rotor_velocity: np.ndarray | None = None` to `MotorData` |
| `utils/analyzer.py` | Capture `Velocity` in `compute_motor_data` |
| `utils/plotter.py` | Add constants + `plot_watts_per_rps` function |
| `tests/utils/test_models.py` | Cover new field |
| `tests/utils/test_analyzer.py` | Cover velocity capture + totals leaves it None |
| `tests/utils/test_plotter.py` | Cover new plot function |

## Out of Scope

- Velocity on `totals` (summing RPS is meaningless)
- Velocity in `plot_per_motor` (can be added later as a one-liner)
- Motor-power variant (W_motor / RPS) — supply watts is the battery draw, more useful for power budget analysis
- Comparison across matches for this metric
