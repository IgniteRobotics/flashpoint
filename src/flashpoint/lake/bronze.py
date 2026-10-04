"""Bronze samples: one row per decoded sample, partitioned by season and log."""

import os
import shutil
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from flashpoint.lake.paths import LakePaths

ROW_GROUP_SIZE = 1_000_000


def _sorted(table: pa.Table) -> pa.Table:
    """Sort by (signal name, ts_us) without materializing the signal strings."""
    signal = table.column("signal").combine_chunks()
    names = signal.dictionary.to_pylist()
    rank_of = np.empty(len(names), np.int32)
    rank_of[np.argsort(np.array(names, dtype=object))] = np.arange(len(names), dtype=np.int32)
    ranks = rank_of[signal.indices.to_numpy(zero_copy_only=False)]
    keys = pa.table({"r": ranks, "t": table.column("ts_us")})
    return table.take(pc.sort_indices(keys, [("r", "ascending"), ("t", "ascending")]))


def write(lake: LakePaths, table: pa.Table, season: str, log_id: str, run_id: str) -> Path:
    """Write a log's samples to staging, then move them into place in one rename."""
    stage_dir = lake.staging / run_id / f"log_id={log_id}"
    stage_dir.mkdir(parents=True, exist_ok=True)
    part = stage_dir / "part-0.parquet"
    pq.write_table(
        _sorted(table) if table.num_rows else table,
        part,
        compression="zstd",
        compression_level=3,
        row_group_size=ROW_GROUP_SIZE,
    )
    with part.open("rb") as f:
        os.fsync(f.fileno())

    target = lake.bronze / f"season={season}" / f"log_id={log_id}"
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
