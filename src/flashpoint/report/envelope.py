"""Replay envelopes: about 1000 min/max/mean buckets per series over the match window.

Each bucket keeps the true minimum and maximum of its samples, so spikes survive the reduction
(legacy #25 decimated with `arr[::step]` and dropped them). Empty buckets are None, so charts
show gaps and never draw zero. Temperature changes rarely and is stored as change points.
"""

import math
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

US_PER_S = 1_000_000
BUCKETS = 1000
PAD_US = 2 * US_PER_S
WIDTH_STEP_US = 10_000
# Per-slot tracks. Constants (motor_kv, stall_current) are not tracks; energy is computed by
# the client from these means.
TRACK_METRICS = ("supply_current", "stator_current", "rotor_velocity_rps", "motor_voltage")

Envelope = dict[str, list[float | None]]  # min, max, mean


@dataclass(frozen=True)
class Window:
    t0_us: int
    width_us: int
    n: int
    match_start_us: int

    @property
    def end_us(self) -> int:
        return self.t0_us + self.n * self.width_us

    def time_s(self, bucket: int) -> float:
        """Start of a bucket, in seconds from match start."""
        return (self.t0_us + bucket * self.width_us - self.match_start_us) / US_PER_S

    def seconds(self, t_us: int) -> float:
        return (t_us - self.match_start_us) / US_PER_S


def window_between(
    start_us: int, end_us: int, match_start_us: int, buckets: int = BUCKETS
) -> Window:
    """`start_us` to `end_us` in about `buckets` buckets of a whole 10 ms, never narrower."""
    span = end_us - start_us
    width = max(WIDTH_STEP_US, math.ceil(span / buckets / WIDTH_STEP_US) * WIDTH_STEP_US)
    return Window(start_us, width, math.ceil(span / width), match_start_us)


def window_for(
    match_start_us: int, match_end_us: int, buckets: int = BUCKETS, pad_us: int = PAD_US
) -> Window:
    """Match start - pad to match end + pad, in `buckets` buckets of a whole 10 ms."""
    return window_between(match_start_us - pad_us, match_end_us + pad_us, match_start_us, buckets)


def round_sig(values: Iterable[float | None], digits: int = 3) -> list[float | None]:
    out: list[float | None] = []
    for value in values:
        if value is None or value == 0 or not math.isfinite(value):
            out.append(value if value is None or math.isfinite(value) else None)
        else:
            out.append(float(f"{value:.{digits}g}"))
    return out


def _source(path: Path) -> str:
    return "read_parquet('" + str(path / "*.parquet").replace("'", "''") + "')"


def _columns(n: int, rows: Iterable[tuple[int, float, float, float]]) -> Envelope:
    env: Envelope = {"min": [None] * n, "max": [None] * n, "mean": [None] * n}
    for bucket, lo, hi, mean in rows:
        env["min"][bucket], env["max"][bucket], env["mean"][bucket] = lo, hi, mean
    return env


def slot_envelopes(
    con: duckdb.DuckDBPyConnection,
    silver: Path,
    window: Window,
    metrics: tuple[str, ...] = TRACK_METRICS,
) -> dict[str, dict[str, Envelope]]:
    """{slot: {metric: {min, max, mean}}} over one session's silver partition."""
    names = ", ".join(f"'{m}'" for m in metrics)
    rows = con.execute(
        f"""
        SELECT slot_id, metric, (t_us - ?) // ? AS bucket, min(value), max(value), avg(value)
        FROM {_source(silver)}
        WHERE metric IN ({names}) AND t_us >= ? AND t_us < ?
        GROUP BY ALL ORDER BY slot_id, metric, bucket
        """,  # noqa: S608 - internal path and whitelisted metric names
        [window.t0_us, window.width_us, window.t0_us, window.end_us],
    ).fetchall()
    grouped: dict[tuple[str, str], list[tuple[int, float, float, float]]] = {}
    for slot, metric, bucket, lo, hi, mean in rows:
        grouped.setdefault((slot, metric), []).append((bucket, lo, hi, mean))
    out: dict[str, dict[str, Envelope]] = {}
    for (slot, metric), buckets in grouped.items():
        out.setdefault(slot, {})[metric] = _columns(window.n, buckets)
    return out


def battery_envelope(con: duckdb.DuckDBPyConnection, silver: Path, window: Window) -> Envelope:
    """The battery proxy (no battery voltage is logged): per bucket, the lowest device supply
    voltage as the minimum, the median across devices as the mean, the highest as the max."""
    rows = con.execute(
        f"""
        SELECT (t_us - ?) // ? AS bucket, min(value), max(value), median(value)
        FROM {_source(silver)}
        WHERE metric = 'supply_voltage' AND t_us >= ? AND t_us < ?
        GROUP BY ALL ORDER BY bucket
        """,  # noqa: S608 - internal path
        [window.t0_us, window.width_us, window.t0_us, window.end_us],
    ).fetchall()
    return _columns(window.n, rows)


def temperature_points(
    con: duckdb.DuckDBPyConnection, silver: Path, window: Window
) -> dict[str, list[list[float]]]:
    """{slot: [[seconds from match start, °C], ...]} at each change within the window; the last
    reading before the window is carried in at its start."""
    rows = con.execute(
        f"""
        WITH t AS (
            SELECT slot_id, t_us, value FROM {_source(silver)}
            WHERE metric = 'temp_c' AND t_us < ?
        ),
        before AS (
            SELECT slot_id, ? AS t_us, arg_max(value, t_us) AS value FROM t
            WHERE t_us < ? GROUP BY slot_id
        )
        SELECT slot_id, t_us, value FROM before
        UNION ALL SELECT slot_id, t_us, value FROM t WHERE t_us >= ?
        ORDER BY slot_id, t_us
        """,  # noqa: S608 - internal path
        [window.end_us, window.t0_us, window.t0_us, window.t0_us],
    ).fetchall()
    out: dict[str, list[list[float]]] = {}
    for slot, t_us, value in rows:
        changes = out.setdefault(slot, [])
        if not changes or changes[-1][1] != value:
            changes.append([round(window.seconds(t_us), 3), value])
    return out


def to_payload(env: Envelope) -> dict[str, Any]:
    return {k: round_sig(v) for k, v in env.items()}


def series_payload(
    con: duckdb.DuckDBPyConnection,
    silver: Path,
    window: Window,
    temps: dict[str, list[list[float]]],
    match_end: int,
) -> dict[str, Any]:
    """The time-series part of a match payload over `window`: series, battery, temps, maxima
    within the match, logging rates, and what each slot did not log."""
    envelopes = slot_envelopes(con, silver, window)
    battery = battery_envelope(con, silver, window)
    rates = dict(
        con.execute(
            "SELECT slot_id, count(*) / ? FROM read_parquet(?)"
            " WHERE metric = 'stator_current' AND t_us >= ? AND t_us < ?"
            " GROUP BY slot_id ORDER BY slot_id",
            [
                (window.end_us - window.t0_us) / US_PER_S,
                str(silver / "*.parquet"),
                window.t0_us,
                window.end_us,
            ],
        ).fetchall()
    )
    series: dict[str, dict[str, Any]] = {}
    maxima: dict[str, dict[str, Any]] = {}
    first = max(0, (window.match_start_us - window.t0_us) // window.width_us)
    last = min(window.n - 1, (match_end - window.t0_us) // window.width_us)
    for slot, metrics_ in envelopes.items():
        series[slot] = {m: to_payload(env) for m, env in metrics_.items()}
        for metric, env in metrics_.items():
            peaks = [v for v in env["max"][first : last + 1] if v is not None]
            if peaks and metric in ("supply_current", "stator_current"):
                maxima.setdefault(slot, {})[f"{metric}_max"] = round_sig([max(peaks)])[0]
    start_s, end_s = window.seconds(window.match_start_us), window.seconds(match_end)
    for slot, changes in temps.items():
        # the reading in effect at match start, then every change up to match end
        at_start = [v for t, v in changes if t <= start_s][-1:]
        during = [v for t, v in changes if start_s < t <= end_s]
        if at_start or during:
            maxima.setdefault(slot, {})["temp_max_c"] = max(at_start + during)
    has_temp = bool(temps)
    all_slots = sorted(set(envelopes) | set(temps))
    not_logged = {
        slot: [
            m
            for m in (*TRACK_METRICS, "temp_c")
            if (m == "temp_c" and slot not in temps)
            or (m != "temp_c" and m not in envelopes.get(slot, {}))
        ]
        for slot in all_slots
    }
    return {
        "window": {
            "t0": round(window.time_s(0), 3),
            "width": window.width_us / US_PER_S,
            "n": window.n,
        },
        "series": series,
        "battery": to_payload(battery),
        "temps": temps,
        "maxima": maxima,
        "rates": {k: round(v, 1) for k, v in rates.items()},
        "not_logged": not_logged,
        "temperature": {"available": has_temp, "reason": None},
    }
