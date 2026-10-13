"""Brownouts per match for detection, counted with Replay's own marker rule (battery proxy below
`brownout_v` for `brownout_hold_s`) over the same session, span, and proxy, so the two always
agree.

Counts are cached in `gold/anomalies/power_counts.parquet`; a match is recounted only when its
silver partition, its phases, the brownout thresholds, or its robot's proxy change, one match's
silver at a time.
"""

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from flashpoint.lake.paths import LakePaths
from flashpoint.report import envelope, markers, proxy
from flashpoint.report.build import LakeMeta, match_span, primary_session, silver_path
from flashpoint.report.settings import ReportConfig
from flashpoint.semantics.robot_config import RobotConfig

POWER_FILE = "power_counts.parquet"
SCHEMA: dict[str, pl.DataType] = {
    "match_key": pl.String(),
    "robot": pl.String(),
    "season": pl.String(),
    "session_id": pl.String(),
    "brownouts": pl.Int64(),
    "lowest_v": pl.Float64(),  # the battery proxy's lowest value in the span
    "proxy": pl.String(),  # which proxy: the declared slot or "median of N devices"
    "fingerprint": pl.String(),
    "thresholds": pl.String(),
}


@dataclass
class PowerCounts:
    counts: dict[str, dict[str, Any]] = field(default_factory=dict)  # match key -> row
    recounted: list[str] = field(default_factory=list)

    def digest(self) -> str:
        rows = sorted((k, r["fingerprint"], r["thresholds"]) for k, r in self.counts.items())
        return hashlib.sha256(json.dumps(rows).encode()).hexdigest()


def anomalies_dir(lake: LakePaths) -> Path:
    return lake.root / "gold" / "anomalies"


def thresholds_hash(report: ReportConfig, proxy_slot: str | None) -> str:
    rule = [report.brownout_v, report.brownout_hold_s, report.gap_limit_s, proxy_slot,
            proxy.FALLBACK_RULE]  # fmt: skip
    return hashlib.sha256(json.dumps(rule).encode()).hexdigest()[:16]


def _fingerprint(lake: LakePaths, meta: LakeMeta, session: dict[str, Any], silver: Path) -> str:
    files = sorted(
        (p.name, p.stat().st_size, p.stat().st_mtime_ns) for p in silver.glob("*.parquet")
    )
    phases = sorted(
        (p["phase"], p["start_us"], p["end_us"])
        for p in meta.rows("match_phases", session_id=session["session_id"])
    )
    payload = [session["session_id"], files, phases]
    return hashlib.sha256(json.dumps(payload, default=str).encode()).hexdigest()[:16]


def _previous(path: Path) -> dict[str, dict[str, Any]]:
    if not path.is_file():
        return {}
    try:
        return {r["match_key"]: r for r in pl.read_parquet(path).to_dicts()}
    except (OSError, pl.exceptions.PolarsError):
        return {}


def write_parquet_atomic(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    frame.write_parquet(tmp)
    tmp.replace(path)


def _count(
    con: duckdb.DuckDBPyConnection, lake: LakePaths, meta: LakeMeta,
    aligned: list[dict[str, Any]], report: ReportConfig, proxy_slot: str | None,
) -> tuple[int, float | None, str | None]:  # fmt: skip
    span = match_span(lake, meta, con, aligned)
    window = envelope.window_for(span.start_us, span.end_us)
    found = proxy.resolve(con, span.silver, window.t0_us, window.end_us, proxy_slot)
    if found is None:
        return 0, None, None
    brownouts = sum(
        1
        for m in markers.power_markers(con, span.silver, window, report, found)
        if m["kind"] == "brownout" and m["level"] == "FAULT"
    )
    query, params = proxy.series_sql(found, span.silver, window.t0_us, window.end_us)
    lowest = con.execute(f"SELECT min(value) FROM ({query})", params).fetchone()  # noqa: S608
    low = None if lowest is None or lowest[0] is None else float(lowest[0])
    return brownouts, low, found.label


def brownout_counts(
    lake: LakePaths, report: ReportConfig, robots: list[RobotConfig]
) -> PowerCounts:
    """Brownout count per match key with aligned silver (matches without are left out), on
    each match's robot's declared battery proxy (or the median fallback)."""
    if not (lake.meta / "sessions.parquet").is_file():
        return PowerCounts()
    path = anomalies_dir(lake) / POWER_FILE
    previous = _previous(path)
    declared = {r.robot: r.battery_proxy for r in robots}
    result = PowerCounts()
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit = '256MB'")
        con.execute("SET threads = 2")
        meta = LakeMeta(lake, con)
        keyed: dict[str, list[dict[str, Any]]] = {}
        for session in meta.tables["sessions"]:
            if session["match_key"]:
                keyed.setdefault(session["match_key"], []).append(session)
        for key in sorted(keyed):
            aligned = [s for s in keyed[key] if silver_path(lake, s) is not None]
            if not aligned:
                continue
            primary = primary_session(meta, aligned)
            silver = silver_path(lake, primary)
            assert silver is not None
            fingerprint = _fingerprint(lake, meta, primary, silver)
            proxy_slot = declared.get(primary["robot"])
            thresholds = thresholds_hash(report, proxy_slot)
            old = previous.get(key)
            if old and old["fingerprint"] == fingerprint and old["thresholds"] == thresholds:
                result.counts[key] = old
                continue
            brownouts, lowest, label = _count(con, lake, meta, aligned, report, proxy_slot)
            result.counts[key] = {
                "match_key": key, "robot": primary["robot"], "season": primary["season"],
                "session_id": primary["session_id"], "brownouts": brownouts, "lowest_v": lowest,
                "proxy": label, "fingerprint": fingerprint, "thresholds": thresholds,
            }  # fmt: skip
            result.recounted.append(key)
    finally:
        con.close()
    if result.recounted or set(previous) != set(result.counts):
        rows = [result.counts[k] for k in sorted(result.counts)]
        write_parquet_atomic(path, pl.DataFrame(rows, schema=SCHEMA))
    return result
