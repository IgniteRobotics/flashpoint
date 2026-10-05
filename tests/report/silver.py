"""Write a synthetic silver partition for report tests."""

from pathlib import Path
from typing import Any

import polars as pl

S = 1_000_000


def series(
    slot: str, metric: str, start_s: float, end_s: float, hz: float, value: float,
    unit: str | None = None,
) -> list[dict[str, Any]]:  # fmt: skip
    n = round((end_s - start_s) * hz)
    return [
        {"slot_id": slot, "unit_id": unit or f"ctre:{slot.upper()}", "metric": metric,
         "t_us": round((start_s + i / hz) * S), "value": value}
        for i in range(n)
    ]  # fmt: skip


def points(slot: str, metric: str, values: list[tuple[float, float]]) -> list[dict[str, Any]]:
    return [
        {"slot_id": slot, "unit_id": f"ctre:{slot.upper()}", "metric": metric,
         "t_us": round(t * S), "value": v}
        for t, v in values
    ]  # fmt: skip


def write_silver(path: Path, rows: list[dict[str, Any]], session_id: str = "s1") -> Path:
    path.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.lit(session_id).alias("session_id"),
        pl.lit(None, pl.Int64).alias("match_time_us"),
        pl.lit(None, pl.String).alias("phase"),
    ).write_parquet(path / "part-0.parquet")
    return path
