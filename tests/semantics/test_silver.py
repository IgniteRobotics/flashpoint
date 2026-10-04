from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from flashpoint.lake import bronze
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.query import connect
from flashpoint.semantics.framing import frame
from flashpoint.semantics.identity import Observation
from flashpoint.semantics.silver import HootInSession, silver_dir, write_session


def _bronze(
    lake: LakePaths, log_id: str, rows: list[tuple[str, int, float | None, bool | None]]
) -> None:
    n = len(rows)
    table = pa.table(
        {
            "signal": pa.array([r[0] for r in rows]).dictionary_encode(),
            "type": pa.array(["double"] * n).dictionary_encode(),
            "ts_us": pa.array([r[1] for r in rows], pa.int64()),
            "v_f64": pa.array([r[2] for r in rows], pa.float64()),
            "v_i64": pa.nulls(n, pa.int64()),
            "v_bool": pa.array([r[3] for r in rows], pa.bool_()),
            "v_str": pa.nulls(n, pa.large_string()),
            "v_bytes": pa.nulls(n, pa.large_binary()),
        }
    )
    bronze.write(lake, table, season="2026", log_id=log_id, run_id="t")


def test_silver_maps_shifts_frames_and_attributes(tmp_path: Path) -> None:
    lake = LakePaths(tmp_path / "lake")
    lake.ensure()
    _bronze(
        lake,
        "h1",
        [
            ("Phoenix6/TalonFX-8/SupplyCurrent", 1_000, 5.0, None),
            ("Phoenix6/TalonFX-8/SupplyCurrent", 3_000, 7.0, None),
            ("Phoenix6/TalonFX-8/DeviceEnable", 1_000, None, True),
            ("Phoenix6/TalonFX-8/PIDDutyCycle_Output", 1_000, 0.3, None),  # not a silver metric
            ("Phoenix6/TalonFX-99/SupplyCurrent", 1_000, 9.0, None),  # unmapped device
            ("RobotEnable", 1_000, None, True),  # not a device signal
        ],
    )
    framing = frame([(0, "Disabled"), (10_500, "Teleop"), (20_000, "Disabled")])
    observations = [
        Observation("s1", "hood", "ctre:A", "inventory", None, 12_500),
        Observation("s1", "hood", "ctre:B", "inventory", 12_500, None),
    ]
    con = connect(lake)
    rows = write_session(
        con, lake, "s1", "2026", [HootInSession("h1", 10_000, {("TalonFX", 8): "hood"})],
        observations, framing, run_id="r",
    )  # fmt: skip

    assert rows == 3
    out = pq.read_table(silver_dir(lake) / "season=2026" / "session_id=s1").to_pylist()
    got = sorted(
        (r["metric"], r["t_us"], r["match_time_us"], r["phase"], r["unit_id"], r["value"])
        for r in out
    )
    assert got == [
        ("device_enable", 11_000, 500, "teleop", "ctre:A", 1.0),
        ("supply_current", 11_000, 500, "teleop", "ctre:A", 5.0),
        ("supply_current", 13_000, 2_500, "teleop", "ctre:B", 7.0),
    ]
    assert not list((lake.root / "silver" / "_staging" / "r").glob("*"))
