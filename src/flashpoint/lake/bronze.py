"""Bronze samples: one row per decoded sample, partitioned by season and log."""

import os
import shutil
from collections.abc import Callable, Iterable
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from flashpoint.lake.paths import LakePaths

ROW_GROUP_SIZE = 1_000_000
EMPTY_SCHEMA = pa.schema(
    [
        ("signal", pa.dictionary(pa.int32(), pa.string())),
        ("type", pa.dictionary(pa.int32(), pa.string())),
        ("ts_us", pa.int64()),
        ("v_f64", pa.float64()),
        ("v_i64", pa.int64()),
        ("v_bool", pa.bool_()),
        ("v_str", pa.large_string()),
        ("v_bytes", pa.large_binary()),
    ]
)


def _sorted(table: pa.Table) -> pa.Table:
    """Sort by (signal name, ts_us) without materializing the signal strings."""
    signal = table.column("signal").combine_chunks()
    names = signal.dictionary.to_pylist()
    rank_of = np.empty(len(names), np.int32)
    rank_of[np.argsort(np.array(names, dtype=object))] = np.arange(len(names), dtype=np.int32)
    ranks = rank_of[signal.indices.to_numpy(zero_copy_only=False)]
    keys = pa.table({"r": ranks, "t": table.column("ts_us")})
    return table.take(pc.sort_indices(keys, [("r", "ascending"), ("t", "ascending")]))


def write(
    lake: LakePaths,
    tables: pa.Table | Iterable[pa.Table],
    season: str | Callable[[], str],
    log_id: str,
    run_id: str,
    presorted: bool = False,
) -> Path:
    """Write a log's samples to staging, then move them into place in one rename.

    Accepts one table, or an iterable of tables streamed into a single file as row groups
    (bounded memory). Samples are stored sorted by (signal, ts_us); pass presorted=True when
    the caller already ordered them. `season` may be a callable, resolved after every
    table is written (it can depend on metadata gathered while streaming).
    """
    stage_dir = lake.staging / run_id / f"log_id={log_id}"
    stage_dir.mkdir(parents=True, exist_ok=True)
    part = stage_dir / "part-0.parquet"
    chunks = [tables] if isinstance(tables, pa.Table) else tables
    writer: pq.ParquetWriter | None = None
    try:
        for table in chunks:
            ordered = table if presorted or not table.num_rows else _sorted(table)
            if writer is None:
                writer = pq.ParquetWriter(
                    part, ordered.schema, compression="zstd", compression_level=3
                )
            writer.write_table(ordered, row_group_size=ROW_GROUP_SIZE)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        pq.write_table(EMPTY_SCHEMA.empty_table(), part)
    with part.open("rb") as f:
        os.fsync(f.fileno())

    resolved = season() if callable(season) else season
    target = lake.bronze / f"season={resolved}" / f"log_id={log_id}"
    target.parent.mkdir(parents=True, exist_ok=True)
    trash = lake.staging / run_id / f"replaced-{log_id}"
    for existing in lake.bronze.glob(f"season=*/log_id={log_id}"):
        existing.rename(trash if existing == target else trash.with_name(trash.name + "-other"))
    stage_dir.rename(target)
    for leftover in trash.parent.glob(f"replaced-{log_id}*"):
        shutil.rmtree(leftover, ignore_errors=True)
    return target


def remove(lake: LakePaths, log_id: str) -> None:
    for existing in lake.bronze.glob(f"season=*/log_id={log_id}"):
        shutil.rmtree(existing)


def clean_staging(lake: LakePaths) -> None:
    for child in lake.staging.iterdir():
        shutil.rmtree(child, ignore_errors=True)
