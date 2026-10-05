"""Rule-based Replay markers: fixed, configurable thresholds; facts only, no cause or fix."""

from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import polars as pl

from flashpoint.report.envelope import Window
from flashpoint.report.settings import ReportConfig
from flashpoint.semantics.physics import DEFAULT_CONFIG, PhysicsConfig

US_PER_S = 1_000_000
MERGE_US = US_PER_S  # intervals closer than this are one event


def _source(path: Path) -> str:
    return "read_parquet('" + str(path / "*.parquet").replace("'", "''") + "')"


def _marker(
    level: str, source: str, kind: str, window: Window, start: int, end: int | None, message: str
) -> dict[str, Any]:
    return {
        "level": level,
        "source": source,
        "kind": kind,
        "t": round(window.seconds(start), 3),
        "end": round(window.seconds(end), 3) if end is not None else None,
        "message": message,
    }


def _intervals(
    starts: np.ndarray,
    ends: np.ndarray,
    values: np.ndarray,
    labels: list[str] | None = None,
    merge_us: int = MERGE_US,
) -> list[tuple[int, int, float, float, str]]:
    """Union of [start, end) intervals (merging gaps < merge_us):
    (start, end, min, max, label of the minimum)."""
    order = np.argsort(starts, kind="stable")
    merged: list[list[Any]] = []
    for i in order:
        s, e, v = int(starts[i]), int(ends[i]), float(values[i])
        label = labels[i] if labels else ""
        if merged and s <= merged[-1][1] + merge_us:
            last = merged[-1]
            if v < last[2]:
                last[2], last[4] = v, label
            last[1], last[3] = max(last[1], e), max(last[3], v)
        else:
            merged.append([s, e, v, v, label])
    return [(s, e, lo, hi, label) for s, e, lo, hi, label in merged]


def _held(frame: pl.DataFrame, end_us: int, cap_us: int) -> pl.DataFrame:
    """Adds `held_end`: each sample holds until the next one of its slot (capped)."""
    return frame.sort("slot_id", "t_us").with_columns(
        pl.min_horizontal(
            pl.col("t_us").shift(-1).over("slot_id").fill_null(end_us),
            pl.col("t_us") + cap_us,
        ).alias("held_end")
    )


def temperature_markers(
    temps: dict[str, list[list[float]]], config: ReportConfig
) -> list[dict[str, Any]]:
    out = []
    for slot, changes in temps.items():
        previous = float("-inf")
        for t, value in changes:
            for level, limit in (("WARN", config.temp_warn_c), ("FAULT", config.temp_fault_c)):
                if previous < limit <= value:
                    out.append(
                        {"level": level, "source": slot, "kind": "temperature", "t": t,
                         "end": None, "message": f"Temperature {value:.0f} °C"}
                    )  # fmt: skip
            previous = value
    return out


def power_markers(
    con: duckdb.DuckDBPyConnection,
    silver: Path,
    window: Window,
    config: ReportConfig,
    physics: PhysicsConfig = DEFAULT_CONFIG,
) -> list[dict[str, Any]]:
    """Brownout (FAULT, held >= brownout_hold_s) and sag (WARN) on the battery proxy: the
    lowest device supply voltage, i.e. any device below the threshold."""
    # Only samples below the sag threshold leave DuckDB, each with the time it holds until.
    held = con.execute(
        f"""
        SELECT slot_id, t_us, value, held_end FROM (
            SELECT slot_id, t_us, value, least(
                coalesce(lead(t_us) OVER (PARTITION BY slot_id ORDER BY t_us), ?), t_us + ?
            ) AS held_end
            FROM {_source(silver)}
            WHERE metric = 'supply_voltage' AND t_us >= ? AND t_us < ?
        ) WHERE value < ?
        """,  # noqa: S608 - internal path
        [window.end_us, physics.hold_cap_us, window.t0_us, window.end_us,
         max(config.sag_v, config.brownout_v)],
    ).pl()  # fmt: skip
    if held.is_empty():
        return []

    def below(threshold: float) -> list[tuple[int, int, float, float, str]]:
        low = held.filter(pl.col("value") < threshold)
        return _intervals(
            low["t_us"].to_numpy(),
            low["held_end"].to_numpy(),
            low["value"].to_numpy(),
            low["slot_id"].to_list(),
        )

    def message(lowest: float, slot: str, threshold: float, start: int, end: int) -> str:
        # Names the device: a stalled motor's own supply voltage can dip with wiring loss.
        return (
            f"Supply voltage {lowest:.2f} V at {slot} (below {threshold:.2f} V)"
            f" for {(end - start) / US_PER_S:.2f} s"
        )

    hold_us = config.brownout_hold_s * US_PER_S
    brownouts = [i for i in below(config.brownout_v) if i[1] - i[0] >= hold_us]
    out = [
        _marker(
            "FAULT", "power", "brownout", window, s, e, message(lo, slot, config.brownout_v, s, e)
        )
        for s, e, lo, _, slot in brownouts
    ]
    for s, e, lo, _, slot in below(config.sag_v):
        if any(b[0] < e and b[1] > s for b in brownouts):
            continue  # the brownout marker already covers this dip
        out.append(
            _marker("WARN", "power", "sag", window, s, e, message(lo, slot, config.sag_v, s, e))
        )
    return out


def stall_markers(
    con: duckdb.DuckDBPyConnection,
    silver: Path,
    window: Window,
    physics: PhysicsConfig = DEFAULT_CONFIG,
) -> list[dict[str, Any]]:
    """Stall intervals as motor physics defines them: stator current at or above a fraction of
    the motor's stall current while the rotor is (nearly) still."""
    source = _source(silver)
    stall = dict(
        con.execute(
            f"SELECT slot_id, median(value) FROM {source}"  # noqa: S608
            " WHERE metric = 'stall_current' GROUP BY slot_id"
        ).fetchall()
    )
    frame = con.execute(
        f"SELECT slot_id, metric, t_us, value FROM {source}"  # noqa: S608
        " WHERE metric IN ('stator_current', 'rotor_velocity_rps') AND t_us >= ? AND t_us < ?",
        [window.t0_us, window.end_us],
    ).pl()
    out = []
    for slot, stall_amps in sorted(stall.items()):
        if not stall_amps:
            continue
        mine = frame.filter(pl.col("slot_id") == slot)
        stator = _held(
            mine.filter(pl.col("metric") == "stator_current").drop("metric"),
            window.end_us,
            physics.hold_cap_us,
        )
        velocity = (
            mine.filter(pl.col("metric") == "rotor_velocity_rps")
            .select("t_us", pl.col("value").alias("rps"))
            .sort("t_us")
        )
        if stator.is_empty() or velocity.is_empty():
            continue
        joined = stator.join_asof(
            velocity, on="t_us", strategy="backward", tolerance=physics.asof_tolerance_us
        ).filter(
            (pl.col("value").abs() >= physics.stall_current_fraction * stall_amps)
            & (pl.col("rps").abs() < physics.stall_velocity_rps)
        )
        for s, e, _, hi, _ in _intervals(
            joined["t_us"].to_numpy(),
            joined["held_end"].to_numpy(),
            joined["value"].abs().to_numpy(),
        ):
            out.append(
                _marker("WARN", slot, "stall", window, s, e,
                        f"Stalled {(e - s) / US_PER_S:.1f} s, stator up to {hi:.0f} A")
            )  # fmt: skip
    return out


def gap_markers(
    con: duckdb.DuckDBPyConnection,
    silver: Path,
    window: Window,
    match_end_us: int,
    config: ReportConfig,
) -> list[dict[str, Any]]:
    """A slot silent (no sample of any metric) for longer than sample_gap_s during the match,
    including a slot that stops logging before the match ends."""
    gap_us = int(config.sample_gap_s * US_PER_S)
    start = window.match_start_us
    rows = con.execute(
        f"""
        WITH t AS (
            SELECT DISTINCT slot_id, t_us FROM {_source(silver)} WHERE t_us >= ? AND t_us <= ?
        ),
        edges AS (
            SELECT slot_id, t_us,
                coalesce(lead(t_us) OVER (PARTITION BY slot_id ORDER BY t_us), ?) AS next_us
            FROM t
        ),
        first AS (SELECT slot_id, min(t_us) AS t_us FROM t GROUP BY slot_id)
        SELECT slot_id, t_us, next_us FROM edges WHERE next_us - t_us > ?
        UNION ALL
        SELECT slot_id, ?, t_us FROM first WHERE t_us - ? > ?
        ORDER BY slot_id, t_us
        """,  # noqa: S608 - internal path
        [start, match_end_us, match_end_us, gap_us, start, start, gap_us],
    ).fetchall()
    return [
        _marker("WARN", slot, "gap", window, s, e, f"No samples for {(e - s) / US_PER_S:.1f} s")
        for slot, s, e in rows
    ]


def match_markers(
    con: duckdb.DuckDBPyConnection,
    silver: Path,
    window: Window,
    match_end_us: int | None,
    temps: dict[str, list[list[float]]],
    config: ReportConfig,
    physics: PhysicsConfig = DEFAULT_CONFIG,
) -> list[dict[str, Any]]:
    """Every rule's markers, in time order. Gaps need a match end (framing)."""
    markers = (
        temperature_markers(temps, config)
        + power_markers(con, silver, window, config, physics)
        + stall_markers(con, silver, window, physics)
        + (gap_markers(con, silver, window, match_end_us, config) if match_end_us else [])
    )
    return sorted(markers, key=lambda m: (m["t"], m["source"], m["kind"]))
