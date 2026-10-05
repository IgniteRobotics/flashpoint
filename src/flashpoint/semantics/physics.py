"""Motor physics over a period: time-weighted stats, power/energy, thermal, stall, residual.

Every statistic is weighted by time held: each sample holds until the next sample of the
same metric, capped so logging gaps don't inflate weights. Time before a metric's first
sample is excluded rather than zero-filled. These fix legacy pitfalls #23, #26, and #27.

The current residual uses the motor model the device reports about itself (Phoenix 6
MotorKV in rpm/V and MotorStallCurrent in A):
    expected stator current = (motor voltage - rotor speed * 60 / KV) * stall current / 12 V
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl

US_PER_S = 1_000_000


@dataclass(frozen=True)
class PhysicsConfig:
    hold_cap_us: int = 1_000_000
    asof_tolerance_us: int = 50_000
    stall_current_fraction: float = 0.2  # P4 corpus spike: limited stalls read 0.20-0.30
    stall_velocity_rps: float = 0.5
    nominal_voltage: float = 12.0
    current_p: float = 0.95
    thermal_rise_c: float = 10.0  # a thermal cycle arms this far above the low since the last
    thermal_fall_c: float = 3.0  # and counts once the reading falls this far from its peak


DEFAULT_CONFIG = PhysicsConfig()


def _held(
    frame: pl.DataFrame,
    metric: str,
    start_us: int,
    end_us: int,
    cap_us: int,
    breaks: Sequence[int] = (),
) -> pl.DataFrame:
    """Samples of `metric` within [start, end) with `w_s`, the seconds each value is held.

    A value never holds past the next of `breaks` (e.g. the start of the disabled gap between
    auto and teleop, whose samples the caller excluded).
    """
    rows = (
        frame.filter(
            (pl.col("metric") == metric) & (pl.col("t_us") >= start_us) & (pl.col("t_us") < end_us)
        )
        .select("t_us", "value")
        .sort("t_us")
    )
    t = rows.get_column("t_us").to_numpy()
    following = rows.get_column("t_us").shift(-1).fill_null(end_us).to_numpy()
    if len(breaks):
        bounds = np.array([*sorted(breaks), end_us], dtype=np.int64)
        following = np.minimum(following, bounds[np.searchsorted(bounds, t, side="right")])
    return rows.with_columns(
        pl.Series("w_s", np.minimum(following - t, cap_us) / US_PER_S, dtype=pl.Float64)
    )


def _mean(values: np.ndarray, weights: np.ndarray) -> float | None:
    total = weights.sum()
    return float((values * weights).sum() / total) if total > 0 else None


def _quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float | None:
    if weights.sum() <= 0:
        return None
    order = np.argsort(values)
    cumulative = np.cumsum(weights[order])
    index = int(np.searchsorted(cumulative, q * cumulative[-1]))
    return float(values[order][min(index, len(values) - 1)])


def _with(base: pl.DataFrame, other: pl.DataFrame, name: str, tolerance: int) -> pl.DataFrame:
    """As-of join (backward) of another metric's latest value onto base timestamps."""
    return base.join_asof(
        other.select("t_us", pl.col("value").alias(name)),
        on="t_us",
        strategy="backward",
        tolerance=tolerance,
    )


def _constant(frame: pl.DataFrame, metric: str) -> float | None:
    values = frame.filter(pl.col("metric") == metric).get_column("value")
    return float(values.median()) if len(values) and values.median() is not None else None  # type: ignore[arg-type]


def motor_features(
    frame: pl.DataFrame,
    start_us: int,
    end_us: int,
    config: PhysicsConfig = DEFAULT_CONFIG,
    breaks: Sequence[int] = (),
) -> dict[str, Any]:
    """Features for one motor over [start, end). `frame` has columns metric, t_us, value.

    `breaks` are times no sample holds past (see `_held`)."""
    cap, tol = config.hold_cap_us, config.asof_tolerance_us
    held = {
        m: _held(frame, m, start_us, end_us, cap, breaks)
        for m in frame.get_column("metric").unique()
    }
    empty = pl.DataFrame(schema={"t_us": pl.Int64, "value": pl.Float64, "w_s": pl.Float64})

    def get(metric: str) -> pl.DataFrame:
        return held.get(metric, empty)

    def stats(metric: str) -> tuple[float | None, float | None]:
        rows = get(metric)
        v, w = rows.get_column("value").to_numpy(), rows.get_column("w_s").to_numpy()
        return _mean(v, w), _quantile(np.abs(v), w, config.current_p)

    flags: list[str] = []
    out: dict[str, Any] = {}
    out["supply_current_mean"], out["supply_current_p95"] = stats("supply_current")
    out["stator_current_mean"], out["stator_current_p95"] = stats("stator_current")
    out["rotor_velocity_mean_rps"], _ = stats("rotor_velocity_rps")

    temp = get("temp_c")
    out["temp_mean_c"], _ = stats("temp_c")
    out["temp_max_c"] = float(temp.get_column("value").max()) if temp.height else None  # type: ignore[arg-type]
    out["temp_rise_c_per_min"] = (
        float(
            np.polyfit(
                temp.get_column("t_us").to_numpy() / (60 * US_PER_S),
                temp.get_column("value").to_numpy(),
                1,
            )[0]
        )
        if temp.height >= 2
        else None
    )

    for name, voltage, current in (
        ("supply", "supply_voltage", "supply_current"),
        ("motor", "motor_voltage", "stator_current"),
    ):
        base = get(current)
        if base.height and get(voltage).height:
            joined = _with(base, get(voltage), "volts", tol).drop_nulls("volts")
            out[f"{name}_energy_wh"] = float(
                (joined["value"] * joined["volts"] * joined["w_s"]).sum() / 3600
            )
        else:
            out[f"{name}_energy_wh"] = None

    kv, stall = _constant(frame, "motor_kv_rpm_per_v"), _constant(frame, "stall_current")
    stator, velocity, motor_v = (
        get("stator_current"),
        get("rotor_velocity_rps"),
        get("motor_voltage"),
    )
    out["stall_s"] = None
    out["residual_p95_a"] = None
    if stator.height and (kv is None or stall is None):
        flags.append("no-motor-constants")
    if stator.height and velocity.height and stall:
        joined = _with(stator, velocity, "rps", tol).drop_nulls("rps")
        stalled = (joined["value"].abs() >= config.stall_current_fraction * stall) & (
            joined["rps"].abs() < config.stall_velocity_rps
        )
        out["stall_s"] = float(joined.filter(stalled)["w_s"].sum())
    if stator.height and velocity.height and motor_v.height and kv and stall:
        joined = _with(_with(stator, velocity, "rps", tol), motor_v, "volts", tol).drop_nulls(
            ["rps", "volts"]
        )
        expected = (joined["volts"] - joined["rps"] * 60.0 / kv) * stall / config.nominal_voltage
        residual = (joined["value"] - expected).abs().to_numpy()
        out["residual_p95_a"] = _quantile(residual, joined["w_s"].to_numpy(), config.current_p)
    out["flags"] = ",".join(flags)
    return out
