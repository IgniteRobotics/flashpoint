"""Detection input: one time-ordered frame of whole-match unit rows, `match_features` joined
with `can_health`, ordered by session start (from `unit_usage`), then match key."""

from dataclasses import dataclass, field

import duckdb
import polars as pl

from flashpoint.anomaly.settings import BASELINE_METRICS
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics import can_health, usage
from flashpoint.semantics.gold import gold_dir

KEYS = ("match_key", "session_id", "season", "robot", "slot_id", "unit_id")
DESCRIPTIVE = ("subsystem", "role", "alignment")
FEATURE_COLUMNS = (
    "temp_max_c",
    "temp_mean_c",
    "temp_rise_c_per_min",
    "stator_current_p95",
    "residual_p95_a",
    "stall_s",
    "supply_resistance_mohm",
)
CAN_COLUMNS = ("reboots", "reboots_enabled", "isolated_gaps", "missed_per_min", "longest_gap_ms")
DUCKDB_THREADS = 2  # per-thread buffers cost ~75 MB


@dataclass
class Frame:
    rows: pl.DataFrame
    matches: dict[str, list[str]] = field(default_factory=dict)  # robot -> keys in order
    starts: dict[str, str] = field(default_factory=dict)  # match key -> session start
    skipped: dict[str, int] = field(default_factory=dict)  # reason -> unit-match rows
    missing: dict[str, int] = field(default_factory=dict)  # metric -> null values

    @property
    def scanned_through(self) -> str | None:
        """The latest match judged."""
        if not self.starts:
            return None
        return max(self.starts, key=lambda k: (self.starts[k], k))


def _schema() -> dict[str, pl.DataType]:
    columns: dict[str, pl.DataType] = {k: pl.String() for k in (*KEYS, *DESCRIPTIVE)}
    columns |= {m: pl.Float64() for m in FEATURE_COLUMNS}
    columns |= {m: can_health.SCHEMA[m] for m in CAN_COLUMNS}
    return {**columns, "session_start": pl.String()}


def _glob(lake: LakePaths, table: str) -> str | None:
    base = gold_dir(lake, table)
    if not any(base.glob("season=*/session_id=*/*.parquet")):
        return None
    return str(base / "season=*" / "session_id=*" / "*.parquet")


def _read(
    con: duckdb.DuckDBPyConnection, glob: str | None, columns: tuple[str, ...], where: str
) -> pl.DataFrame:
    schema = {c: t for c, t in _schema().items() if c in columns}
    if glob is None:
        return pl.DataFrame(schema=schema)
    present = {
        r[0]
        for r in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name = true)", [glob]
        ).fetchall()
    }
    select = ", ".join(
        f'"{c}"' if c in present else f'NULL::{_sql_type(t)} AS "{c}"' for c, t in schema.items()
    )
    frame = con.execute(
        f"SELECT {select} FROM read_parquet(?, union_by_name = true) WHERE {where}",  # noqa: S608
        [glob],
    ).pl()
    return frame.cast(schema)  # type: ignore[arg-type]


def _sql_type(dtype: pl.DataType) -> str:
    return {pl.Float64(): "DOUBLE", pl.Int64(): "BIGINT", pl.Boolean(): "BOOLEAN"}.get(
        dtype, "VARCHAR"
    )


def _count(reasons: dict[str, int], reason: str, n: int) -> None:
    if n:
        reasons[reason] = reasons.get(reason, 0) + n


def load_frame(lake: LakePaths) -> Frame:
    con = duckdb.connect()
    try:
        con.execute(f"SET threads = {DUCKDB_THREADS}")
        features = _read(
            con, _glob(lake, "match_features"), (*KEYS, *DESCRIPTIVE, *FEATURE_COLUMNS),
            "phase = 'match'",
        )  # fmt: skip
        cans = _read(
            con, _glob(lake, can_health.TABLE), (*KEYS, *DESCRIPTIVE, *CAN_COLUMNS),
            "phase = 'match'",
        )  # fmt: skip
        starts = _read(con, _glob(lake, usage.TABLE), ("session_id", "session_start"), "true")
    finally:
        con.close()
    joined = features.join(
        cans, on=["session_id", "slot_id", "unit_id"], how="full", coalesce=True, nulls_equal=True
    )
    for column in (*KEYS, *DESCRIPTIVE):
        right = f"{column}_right"
        if right in joined.columns:
            joined = joined.with_columns(pl.coalesce(column, right).alias(column)).drop(right)
    starts = starts.group_by("session_id").agg(pl.col("session_start").min())
    rows = joined.join(starts, on="session_id", how="left")

    skipped: dict[str, int] = {}
    _count(skipped, "no-match-key", rows.filter(pl.col("match_key").is_null()).height)
    rows = rows.filter(pl.col("match_key").is_not_null())
    # Two sessions for one match (a duplicate log): keep the fuller one.
    sizes = (
        rows.group_by("match_key", "session_id")
        .len()
        .sort(["match_key", "len", "session_id"], descending=[False, True, False])
    )
    keep = sizes.unique("match_key", keep="first", maintain_order=True)["session_id"].to_list()
    _count(skipped, "duplicate-session", rows.filter(~pl.col("session_id").is_in(keep)).height)
    rows = rows.filter(pl.col("session_id").is_in(keep))
    _count(skipped, "no-unit", rows.filter(pl.col("unit_id").is_null()).height)
    rows = rows.filter(pl.col("unit_id").is_not_null())
    _count(skipped, "no-start-time", rows.filter(pl.col("session_start").is_null()).height)
    rows = rows.filter(pl.col("session_start").is_not_null())

    rows = rows.sort(["session_start", "match_key", "slot_id", "unit_id"]).select(list(_schema()))
    missing = {
        m: n for m in BASELINE_METRICS if (n := rows.get_column(m).null_count()) and m in rows
    }
    order = rows.select("robot", "match_key", "session_start").unique(maintain_order=True)
    matches: dict[str, list[str]] = {}
    for robot, key, _ in order.iter_rows():
        matches.setdefault(robot, []).append(key)
    starts_by_key = dict(zip(order["match_key"], order["session_start"], strict=True))
    return Frame(rows, matches, starts_by_key, skipped, missing)
