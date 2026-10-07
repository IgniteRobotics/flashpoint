"""Silver signals: declared NetworkTables entries from a session's wpilog, labeled and framed.

One DuckDB query per session over the wpilog's bronze partition: samples of the declared
entry names are joined to the declarations and the match phases, then written straight to
Parquet (`silver/signals/season=<s>/session_id=<id>/`). Slot-keyed `silver/samples` is separate.
"""

import logging
import shutil
from pathlib import Path

import duckdb
import polars as pl

from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.framing import Framing
from flashpoint.semantics.robot_config import Signal

NUMERIC_TYPES = ("double", "float", "int64", "boolean")
MISSING = "missing"
NON_NUMERIC = "non-numeric"

_log = logging.getLogger(__name__)


def signals_dir(lake: LakePaths) -> Path:
    return lake.root / "silver" / "signals"


def unusable(
    declared: dict[str, Signal], entry_types: dict[str, set[str]]
) -> list[tuple[str, str]]:
    """(signal id, reason) for declared entries absent from the log or never numeric there."""
    found = []
    for name, signal in declared.items():
        types = entry_types.get(name)
        if types is None:
            found.append((signal.id, MISSING))
        elif not types & set(NUMERIC_TYPES):
            found.append((signal.id, NON_NUMERIC))
    return found


def warn_unusable(session_id: str, problems: list[tuple[str, str]]) -> None:
    if problems:
        listed = ", ".join(f"{signal_id} ({reason})" for signal_id, reason in problems)
        _log.warning(
            "session %s: %d declared signal(s) not written: %s", session_id, len(problems), listed
        )


def remove_session(lake: LakePaths, session_id: str) -> None:
    for existing in signals_dir(lake).glob(f"season=*/session_id={session_id}"):
        shutil.rmtree(existing)


def write_session(
    con: duckdb.DuckDBPyConnection,
    lake: LakePaths,
    session_id: str,
    season: str,
    wpilog_id: str,
    declared: dict[str, Signal],
    framing: Framing | None,
    run_id: str,
) -> int:
    """Write one session's signal samples atomically; returns the row count.

    `framing` is None for a session without match framing: match time and phase stay empty.
    """
    stage = lake.root / "silver" / "_staging" / run_id / "signals" / f"session_id={session_id}"
    stage.mkdir(parents=True, exist_ok=True)
    con.register(
        "declared_signals",
        pl.DataFrame(
            {
                "name": list(declared),
                "signal_id": [s.id for s in declared.values()],
                "subsystem": [s.subsystem for s in declared.values()],
                "component": [s.component for s in declared.values()],
                "metric": [s.metric for s in declared.values()],
            },
            schema={
                "name": pl.String,
                "signal_id": pl.String,
                "subsystem": pl.String,
                "component": pl.String,
                "metric": pl.String,
            },
        ),
    )
    phases = framing.phases if framing is not None else []
    con.register(
        "signal_phases",
        pl.DataFrame(
            {
                "phase": [p.name for p in phases],
                "start_us": [p.start_us for p in phases],
                "end_us": [p.end_us for p in phases],
            },
            schema={"phase": pl.String, "start_us": pl.Int64, "end_us": pl.Int64},
        ),
    )
    match_start = (
        "NULL"
        if framing is None or framing.match_start_us is None
        else str(int(framing.match_start_us))
    )
    types = ", ".join(f"'{t}'" for t in NUMERIC_TYPES)
    target = stage / "part-0.parquet"
    query = f"""
        SELECT
            '{session_id}' AS session_id, d.signal_id, d.subsystem, d.component, d.metric,
            s.ts_us AS t_us, s.ts_us - {match_start} AS match_time_us, p.phase,
            COALESCE(s.v_f64, CAST(s.v_i64 AS DOUBLE), CAST(s.v_bool AS DOUBLE)) AS value
        FROM samples s
        JOIN declared_signals d ON d.name = s.signal::VARCHAR
        LEFT JOIN signal_phases p ON (p.start_us IS NULL OR s.ts_us >= p.start_us)
            AND (p.end_us IS NULL OR s.ts_us < p.end_us)
        WHERE s.log_id = '{wpilog_id}' AND s.type::VARCHAR IN ({types})
            AND s.signal::VARCHAR IN (SELECT name FROM declared_signals)
    """  # noqa: S608 - ids and offsets are internal hex/int values
    con.execute(f"COPY ({query}) TO '{target}' (FORMAT parquet, COMPRESSION zstd)")
    total: int = con.sql(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]  # type: ignore[index]
    final = signals_dir(lake) / f"season={season}" / f"session_id={session_id}"
    final.parent.mkdir(parents=True, exist_ok=True)
    remove_session(lake, session_id)
    stage.rename(final)
    return total
