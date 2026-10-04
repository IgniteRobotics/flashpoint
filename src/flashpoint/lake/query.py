"""DuckDB access to a lake: `samples` over bronze Parquet, plus `meta_*` views."""

from pathlib import Path

import duckdb

from flashpoint.lake.paths import LakePaths

_EMPTY_SAMPLES = (
    "CREATE VIEW samples AS SELECT NULL::VARCHAR AS signal, NULL::VARCHAR AS type,"
    " NULL::BIGINT AS ts_us, NULL::DOUBLE AS v_f64, NULL::BIGINT AS v_i64, NULL::BOOLEAN AS v_bool,"
    " NULL::VARCHAR AS v_str, NULL::BLOB AS v_bytes, NULL::VARCHAR AS season,"
    " NULL::VARCHAR AS log_id WHERE false"
)


def _quote(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def connect(lake: LakePaths) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if any(lake.bronze.glob("season=*/log_id=*/*.parquet")):
        pattern = _quote(lake.bronze / "season=*" / "log_id=*" / "*.parquet")
        con.execute(
            "CREATE VIEW samples AS SELECT * FROM read_parquet("
            f"{pattern}, hive_partitioning = true, hive_types_autocast = false)"
        )
    else:
        con.execute(_EMPTY_SAMPLES)
    for snapshot in sorted(lake.meta.glob("*.parquet")):
        con.execute(f"CREATE VIEW meta_{snapshot.stem} AS SELECT * FROM {_quote(snapshot)}")
    return con
