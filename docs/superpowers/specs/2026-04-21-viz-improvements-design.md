# Viz Improvements Design

**Date:** 2026-04-21  
**Branch:** feature/power-tracking

## Problem

Raw 50 Hz motor telemetry plotted as line charts is unreadable. High-frequency noise buries the signal. Trendlines and summary tables are the only currently useful outputs.

## Goals

- Temporal pattern: how does power demand shift over a match?
- Cross-match comparison: how does this match differ from others?

## Out of Scope

- `plot_cumulative_energy` — already smooth by nature (integration), no change
- `plot_comparison` — unchanged
- Per-motor detail pages (`plot_per_motor`) — unchanged
- Summary stat tables — unchanged

---

## Changes

### 1. Replace `plot_instantaneous` with `plot_heatmap`

**Signature:** `plot_heatmap(match: Match, metric: str) -> Figure`

Replaces the noisy per-motor line chart for all instantaneous metrics (motor voltage, stator current, motor power, supply current, supply power).

**Layout:**
- X axis: time (seconds), binned into 1-second windows (~150 columns for a 150s match)
- Y axis: motor IDs (one row per motor, sorted by ID)
- Color: mean metric value per bin, using `matplotlib.pcolormesh`
- Color scale: **global** — `vmin=0`, `vmax=max value across all motors` for that metric
- Colormap: `plasma`
- Colorbar on right with unit label (V, A, or W)
- Figure size: `(12, max(3, n_motors * 0.6))`

**Binning:** `numpy` reshape/mean or `pandas` resample at 1s. Data at 50 Hz → mean of each 50-sample window per motor.

**Reporter changes:** All `plotter.plot_instantaneous(...)` calls in `reporter.py` replaced with `plotter.plot_heatmap(...)`. Function signature is identical.

---

### 2. Modify `plot_total_power`

**Signature unchanged:** `plot_total_power(match: Match) -> Figure`

**Changes:**
- Apply rolling mean before plotting. Window: `SMOOTH_WINDOW = 50` samples (1 second at 50 Hz), defined as a module-level constant in `plotter.py`
- No raw data plotted behind the smoothed line
- After smoothing, compute P95 threshold per series (`np.percentile`). Scatter any smoothed point ≥ P95 as small markers (size 12, color `#e67e22`, zorder 6, label `"{series} peaks"`)
- Trend line removed from this plot (rolling average already conveys the trend)

**Helper:** `_smooth(values: np.ndarray, window: int) -> np.ndarray` — `np.convolve(values, np.ones(window)/window, mode='same')`. Private to `plotter.py`.

---

## Constants Added to `plotter.py`

```python
SMOOTH_WINDOW: int = 50   # samples — 1s at 50 Hz
BIN_SECONDS: float = 1.0  # heatmap time bin width
```

---

## Testing

- `test_plotter.py`: add tests for `plot_heatmap` (returns Figure, correct shape) and updated `plot_total_power` (peak scatter present, no raw line)
- Existing `plot_instantaneous` tests removed or updated to `plot_heatmap`
