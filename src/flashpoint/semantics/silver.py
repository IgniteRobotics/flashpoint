"""Silver: mapped device samples on the wpilog clock, one row per (slot, unit, metric, time).

Built inside DuckDB from bronze (bounded memory): device signals are joined to small tables
for slots, metric names, unit time ranges, and match phases, then written straight to Parquet.
"""

import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl

from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.framing import Framing
from flashpoint.semantics.identity import Observation

DEVICE_SIGNAL = r"^Phoenix6/([A-Za-z0-9]+)-(\d+)/(\w+)$"  # model, CAN id, signal

# Phoenix 6 status signal -> silver metric name (units in the name where they aren't SI-obvious)
METRICS = {
    "SupplyCurrent": "supply_current",
    "StatorCurrent": "stator_current",
    "TorqueCurrent": "torque_current",
    "MotorVoltage": "motor_voltage",
    "SupplyVoltage": "supply_voltage",
    "RotorVelocity": "rotor_velocity_rps",
    "Velocity": "velocity",
    "Position": "position",
    "DeviceTemp": "temp_c",
    "ProcessorTemp": "processor_temp_c",
    "DutyCycle": "duty_cycle",
    "DeviceEnable": "device_enable",
    "MotorKT": "motor_kt",
    "MotorKV": "motor_kv_rpm_per_v",
    "MotorStallCurrent": "stall_current",
}


@dataclass(frozen=True)
class HootInSession:
    log_id: str
    offset_us: int
    slots: dict[tuple[str, int], str]  # (model, can_id) -> slot_id for this hoot's bus


def silver_dir(lake: LakePaths) -> Path:
    return lake.root / "silver" / "samples"


def write_session(
    con: duckdb.DuckDBPyConnection,
    lake: LakePaths,
    session_id: str,
    season: str,
    hoots: list[HootInSession],
    observations: list[Observation],
    framing: Framing,
    run_id: str,
) -> int:
    """Write one session's silver atomically; returns the row count."""
    stage = lake.root / "silver" / "_staging" / run_id / f"session_id={session_id}"
    stage.mkdir(parents=True, exist_ok=True)
    con.register(
        "metric_map", pl.DataFrame({"sig": list(METRICS), "metric": list(METRICS.values())})
    )
    con.register(
        "unit_ranges",
        pl.DataFrame(
            {
                "slot_id": [o.slot_id for o in observations],
                "unit_id": [o.unit_id for o in observations],
                "from_us": [o.from_ts_us for o in observations],
                "to_us": [o.to_ts_us for o in observations],
            },
            schema={
                "slot_id": pl.String,
                "unit_id": pl.String,
                "from_us": pl.Int64,
                "to_us": pl.Int64,
            },
        ),
    )
    con.register(
        "phase_ranges",
        pl.DataFrame(
            {
                "phase": [p.name for p in framing.phases],
                "start_us": [p.start_us for p in framing.phases],
                "end_us": [p.end_us for p in framing.phases],
            },
            schema={"phase": pl.String, "start_us": pl.Int64, "end_us": pl.Int64},
        ),
    )
    match_start = "NULL" if framing.match_start_us is None else str(int(framing.match_start_us))
    total = 0
    for part, hoot in enumerate(hoots):
        con.register(
            "slot_map",
            pl.DataFrame(
                {
                    "model": [m for m, _ in hoot.slots],
                    "can_id": [i for _, i in hoot.slots],
                    "slot_id": list(hoot.slots.values()),
                },
                schema={"model": pl.String, "can_id": pl.Int64, "slot_id": pl.String},
            ),
        )
        target = stage / f"part-{part}.parquet"
        query = f"""
            WITH dev AS (
                SELECT
                    regexp_extract(signal::VARCHAR, '{DEVICE_SIGNAL}', 1) AS model,
                    TRY_CAST(
                        regexp_extract(signal::VARCHAR, '{DEVICE_SIGNAL}', 2) AS BIGINT
                    ) AS can_id,
                    regexp_extract(signal::VARCHAR, '{DEVICE_SIGNAL}', 3) AS sig,
                    ts_us + {int(hoot.offset_us)} AS t_us,
                    COALESCE(v_f64, CAST(v_bool AS DOUBLE), CAST(v_i64 AS DOUBLE)) AS value
                FROM samples
                WHERE log_id = '{hoot.log_id}' AND signal::VARCHAR LIKE 'Phoenix6/%'
            )
            SELECT
                '{session_id}' AS session_id, s.slot_id, u.unit_id, m.metric, d.t_us,
                d.t_us - {match_start} AS match_time_us, p.phase, d.value
            FROM dev d
            JOIN slot_map s ON s.model = d.model AND s.can_id = d.can_id
            JOIN metric_map m ON m.sig = d.sig
            LEFT JOIN unit_ranges u ON u.slot_id = s.slot_id
                AND (u.from_us IS NULL OR d.t_us >= u.from_us)
                AND (u.to_us IS NULL OR d.t_us < u.to_us)
            LEFT JOIN phase_ranges p ON (p.start_us IS NULL OR d.t_us >= p.start_us)
                AND (p.end_us IS NULL OR d.t_us < p.end_us)
            WHERE d.value IS NOT NULL
        """  # noqa: S608 - ids and offsets are internal hex/int values
        con.execute(f"COPY ({query}) TO '{target}' (FORMAT parquet, COMPRESSION zstd)")
        total += con.sql(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]  # type: ignore[index]
    final = silver_dir(lake) / f"season={season}" / f"session_id={session_id}"
    final.parent.mkdir(parents=True, exist_ok=True)
    for existing in silver_dir(lake).glob(f"season=*/session_id={session_id}"):
        shutil.rmtree(existing)
    stage.rename(final)
    return total
