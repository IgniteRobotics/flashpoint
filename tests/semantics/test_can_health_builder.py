"""Self-tests for the synthetic CAN-health fixtures: they must say what the specs claim."""

from pathlib import Path

import duckdb
import numpy as np
import pyarrow.parquet as pq
import pytest

from flashpoint.lake.bronze import EMPTY_SCHEMA
from flashpoint.lake.paths import LakePaths
from tests import can_health_builder as b

S = b.S


def test_fixed_period_frame_count_and_intervals() -> None:
    hoot = b.Hoot([b.Device("TalonFX-1", 8.0)], duration_s=1.0)
    frames = hoot.frame_times(hoot.devices[0])
    assert len(frames) == 126  # 0, 8, ..., 1000 ms
    assert set(np.diff(frames)) == {8_000}


def test_aligned_silence_leaves_a_gap_of_its_length() -> None:
    device = b.Device("TalonFX-1", 8.0, silences=[(1.0, 0.040)])
    frames = b.Hoot([device], duration_s=2.0).frame_times(device)
    assert int(np.diff(frames).max()) == 40_000


def test_jitter_pattern_is_cycled() -> None:
    device = b.Device("TalonFX-1", intervals_ms=[5.0, 19.0, 8.0])
    frames = b.Hoot([device], duration_s=1.0).frame_times(device)
    assert list(np.diff(frames)[:4]) == [5_000, 19_000, 8_000, 5_000]


def test_stop_ends_frames() -> None:
    device = b.Device("TalonFX-1", 8.0, stop_s=0.5)
    assert b.Hoot([device], duration_s=1.0).frame_times(device).max() <= 500_000


def test_written_partition_matches_bronze_schema(tmp_path: Path) -> None:
    lake = LakePaths(tmp_path / "lake")
    path = b.write_hoot(lake, "h1", b.whole_bus())
    assert path == lake.bronze / "season=2026" / "log_id=h1"
    assert pq.read_schema(path / "part-0.parquet").equals(EMPTY_SCHEMA)


def _signals(hoot: b.Hoot) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.register("t", hoot.table())
    return con


def test_enable_follows_windows() -> None:
    con = _signals(b.Hoot([], duration_s=10.0, enabled=[(2.0, 4.0)]))
    on = con.sql(
        "SELECT min(ts_us), max(ts_us) FROM t WHERE signal = 'RobotEnable' AND v_bool"
    ).fetchone()
    assert on is not None and 2.0 * S <= on[0] < 2.05 * S and 3.95 * S <= on[1] < 4.0 * S


def test_q105_like() -> None:
    hoot = b.q105_like()
    roller = hoot.devices[1]
    assert roller.name == "TalonFX-2"
    assert int(np.diff(hoot.frame_times(roller)).max()) == pytest.approx(820_000, abs=8_000)
    con = _signals(hoot)
    assert con.sql(
        "SELECT ts_us, v_i64 FROM t WHERE signal = 'Phoenix6/TalonFX-2/ResetCount' ORDER BY 1"
    ).fetchall() == [(420_000, 0), (344_960_000, 1)]
    first_on = con.sql(
        "SELECT min(ts_us) FROM t"
        " WHERE signal = 'Phoenix6/TalonFX-2/Fault_BootDuringEnable' AND v_f64 = 1"
    ).fetchone()
    assert first_on == (345_250_000,)  # first 4 Hz sample at or after 345.16 s
    others = [d for d in hoot.devices if d is not roller]
    assert all(int(np.diff(hoot.frame_times(d)).max()) == 8_000 for d in others)


def test_bus_wide() -> None:
    hoot = b.bus_wide()
    starts = []
    for device in hoot.devices[:6]:
        frames = hoot.frame_times(device)
        i = int(np.argmax(np.diff(frames)))
        starts.append(int(frames[i]))
        assert int(frames[i + 1] - frames[i]) >= 56_000
    assert max(starts) - min(starts) <= 13_000  # 5 ms apart, rounded to their 8 ms clocks
    assert all(int(np.diff(hoot.frame_times(d)).max()) == 8_000 for d in hoot.devices[6:])


def test_whole_bus() -> None:
    hoot = b.whole_bus()
    for device in hoot.devices:
        assert int(np.diff(hoot.frame_times(device)).max()) >= 1_400_000


def test_stops_early() -> None:
    hoot = b.stops_early()
    assert hoot.frame_times(hoot.devices[2]).max() <= 22 * S
    assert hoot.frame_times(hoot.devices[0]).max() >= 29.99 * S


def test_no_reset_count() -> None:
    con = _signals(b.no_reset_count())
    assert con.sql("SELECT count(*) FROM t WHERE signal LIKE '%/ResetCount'").fetchone() == (0,)
    assert con.sql(
        "SELECT count(*) FROM t WHERE signal LIKE '%/Fault_BootDuringEnable' AND v_f64 = 1"
    ).fetchone() == (1,)
