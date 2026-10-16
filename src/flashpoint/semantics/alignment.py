"""Put hoot samples on the wpilog clock.

Preferred: `payload-match`. A signal written to both logs (CTRE swerve telemetry logs
`DriveState/Pose` to the hoot and to NetworkTables) carries identical payloads, so equal
values pair samples exactly (Q7: 15,039 matches, IQR 0.076 ms). Fallback: robot-enable
transitions (30-500 ms error).
"""

from dataclasses import dataclass

import numpy as np
import polars as pl

MIN_PAYLOAD_MATCHES = 50
MAX_PAYLOAD_SPREAD_US = 5_000
EDGE_WINDOW_US = 2_000_000
MIN_EDGE_PAIRS = 2
MAX_EDGE_SPREAD_US = 1_000_000
MAX_EDGE_VS_WALL_CLOCK_US = 10_000_000
WPILOG_ENABLE = "DS:enabled"
HOOT_ENABLE = "RobotEnable"


@dataclass(frozen=True)
class Alignment:
    offset_us: int  # add to a hoot timestamp to get the wpilog timestamp
    method: str  # payload-match | enable-edges | wall-clock
    confidence: str  # high | low | very-low
    spread_us: int | None
    matches: int


def _strip_nt(name: str) -> str:
    return name.removeprefix("NT:").lstrip("/")


def _iqr(values: np.ndarray) -> int:
    q1, q3 = np.percentile(values, [25, 75])
    return int(round(q3 - q1))


def _unique_values(frame: pl.DataFrame, key: str) -> pl.DataFrame:
    return (
        frame.filter(pl.col(key).is_not_null())
        .group_by(key)
        .agg(pl.col("ts_us").first(), pl.len().alias("n"))
        .filter(pl.col("n") == 1)
        .drop("n")
    )


def align_by_payload(wpi: pl.DataFrame, hoot: pl.DataFrame) -> Alignment | None:
    """Pair identical payloads of signals present in both logs (non-device hoot signals only)."""
    diffs: list[np.ndarray] = []
    wpi_signals = wpi.select("signal", "type").unique().rows()
    for h_signal, h_type in hoot.select("signal", "type").unique().rows():
        if h_signal.startswith("Phoenix6/"):
            continue
        for w_signal, w_type in wpi_signals:
            if _strip_nt(w_signal) != h_signal or w_type != h_type:
                continue
            h_rows = hoot.filter(pl.col("signal") == h_signal)
            w_rows = wpi.filter(pl.col("signal") == w_signal)
            key = (
                "v_bytes" if h_rows.get_column("v_bytes").null_count() < h_rows.height else "v_f64"
            )
            joined = _unique_values(w_rows, key).join(
                _unique_values(h_rows, key), on=key, suffix="_h"
            )
            diffs.append((joined["ts_us"] - joined["ts_us_h"]).to_numpy())
    if not diffs:
        return None
    all_diffs = np.concatenate(diffs)
    if len(all_diffs) < MIN_PAYLOAD_MATCHES:
        return None
    spread = _iqr(all_diffs)
    if spread > MAX_PAYLOAD_SPREAD_US:
        return None
    return Alignment(int(np.median(all_diffs)), "payload-match", "high", spread, len(all_diffs))


def _edges(frame: pl.DataFrame, signal: str) -> list[tuple[int, bool]]:
    rows = (
        frame.filter((pl.col("signal") == signal) | pl.col("signal").str.ends_with("/" + signal))
        .filter(pl.col("v_bool").is_not_null())
        .sort("ts_us")
        .select("ts_us", "v_bool")
        .rows()
    )
    return [(t, v) for i, (t, v) in enumerate(rows) if i > 0 and v != rows[i - 1][1]]


def _paired_diffs(a: list[tuple[int, bool]], b: list[tuple[int, bool]], coarse: int) -> np.ndarray:
    """One-to-one: each edge in b pairs with the nearest unused same-polarity edge in a.

    Reusing an edge let a single wpilog edge "confirm" two unrelated hoot edges (corpus E10).
    """
    diffs = []
    used: set[int] = set()
    for tb, vb in b:
        candidates = [
            (abs(ta - tb - coarse), i, ta - tb)
            for i, (ta, va) in enumerate(a)
            if i not in used and va == vb and abs(ta - tb - coarse) < EDGE_WINDOW_US
        ]
        if candidates:
            _, index, diff = min(candidates)
            used.add(index)
            diffs.append(diff)
    return np.array(diffs, dtype=np.int64)


def align_by_enable_edges(
    wpi: pl.DataFrame, hoot: pl.DataFrame, wall_clock_us: int | None = None
) -> Alignment | None:
    """Pair enable transitions. Rejected unless at least 2 pairs agree (spread under 1 s) and,
    when a wall-clock estimate exists, land within 10 s of it. Too few edges mis-pair easily."""
    w_edges, h_edges = _edges(wpi, WPILOG_ENABLE), _edges(hoot, HOOT_ENABLE)
    w_rise = [t for t, v in w_edges if v]
    h_rise = [t for t, v in h_edges if v]
    if not w_rise or not h_rise:
        return None
    diffs = _paired_diffs(w_edges, h_edges, w_rise[0] - h_rise[0])
    if len(diffs) < MIN_EDGE_PAIRS or _iqr(diffs) > MAX_EDGE_SPREAD_US:
        return None
    offset = int(np.median(diffs))
    if wall_clock_us is not None and abs(offset - wall_clock_us) > MAX_EDGE_VS_WALL_CLOCK_US:
        return None
    return Alignment(offset, "enable-edges", "low", _iqr(diffs), len(diffs))


def hoot_bus_agreement_us(first: pl.DataFrame, second: pl.DataFrame) -> int | None:
    """How far apart two hoots' clocks are, from their shared RobotEnable transitions."""
    a, b = _edges(first, HOOT_ENABLE), _edges(second, HOOT_ENABLE)
    if not a or not b:
        return None
    diffs = _paired_diffs(b, a, 0)
    return int(abs(np.median(diffs))) if len(diffs) else None
