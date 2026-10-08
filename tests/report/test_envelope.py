import math
import random
from pathlib import Path

import duckdb
import pytest

from flashpoint.report.envelope import (
    Window,
    battery_envelope,
    round_sig,
    slot_envelopes,
    temperature_points,
    window_between,
    window_for,
)
from tests.report.silver import S, points, series, write_silver


def test_window_about_1000_buckets_rounded_to_10ms() -> None:
    window = window_for(100 * S, 264_100_000)  # a 164.1 s match, like Q7
    assert window.t0_us == 98 * S
    assert window.width_us == 170_000  # 168.1 s / 1000 = 168.1 ms, up to whole 10 ms
    assert window.n == math.ceil(168.1 / 0.17)
    assert window.n * window.width_us >= 168.1 * S
    assert window.time_s(0) == -2.0


def test_window_bucket_count_override() -> None:
    window = window_for(0, 10 * S, buckets=100)
    assert window.width_us == 140_000 and window.n == 100


def test_round_sig() -> None:
    assert round_sig([123.4, 0.012345, -9.876, 0.0, None, 150.0, 1e-9]) == [
        123.0, 0.0123, -9.88, 0.0, None, 150.0, 1e-9,
    ]  # fmt: skip


def _window() -> Window:
    return Window(t0_us=0, width_us=10_000, n=1000, match_start_us=2 * S)


def test_spike_survives(tmp_path: Path) -> None:
    rows = series("drive-fl", "supply_current", 0, 10, 1000, 10.0)
    for row in rows:
        if 5.0 * S <= row["t_us"] < 5.004 * S:  # 4 ms at 150 A
            row["value"] = 150.0
    path = write_silver(tmp_path / "silver", rows)
    env = slot_envelopes(duckdb.connect(), path, _window())["drive-fl"]["supply_current"]
    assert max(v for v in env["max"] if v is not None) == 150.0
    assert env["max"][500] == 150.0 and env["min"][500] == 10.0
    assert env["max"][499] == 10.0 and env["max"][501] == 10.0
    assert env["mean"][500] == pytest.approx(10 + 140 * 4 / 10, rel=1e-3)


def test_empty_buckets_are_null(tmp_path: Path) -> None:
    rows = series("intake-roller", "stator_current", 0, 10, 4, 20.0)  # 4 Hz, 10 ms buckets
    path = write_silver(tmp_path / "silver", rows)
    env = slot_envelopes(duckdb.connect(), path, _window())["intake-roller"]["stator_current"]
    assert len(env["min"]) == 1000
    assert env["mean"][0] == 20.0 and env["mean"][1] is None and env["mean"][25] == 20.0
    assert sum(v is not None for v in env["mean"]) == 40


def test_samples_outside_window_excluded(tmp_path: Path) -> None:
    rows = series("drive-fl", "supply_current", 0, 12, 100, 5.0)
    rows += points("drive-fl", "supply_current", [(11.0, 99.0)])
    path = write_silver(tmp_path / "silver", rows)
    env = slot_envelopes(duckdb.connect(), path, _window())["drive-fl"]["supply_current"]
    assert max(v for v in env["max"] if v is not None) == 5.0


def test_only_track_metrics(tmp_path: Path) -> None:
    rows = series("drive-fl", "motor_kv_rpm_per_v", 0, 10, 4, 500.0) + series(
        "drive-fl", "rotor_velocity_rps", 0, 10, 100, 12.0
    )
    path = write_silver(tmp_path / "silver", rows)
    envs = slot_envelopes(duckdb.connect(), path, _window())
    assert set(envs["drive-fl"]) == {"rotor_velocity_rps"}


def test_battery_proxy_is_lowest_device(tmp_path: Path) -> None:
    rows = series("drive-fl", "supply_voltage", 0, 10, 100, 12.0)
    rows += series("hood", "supply_voltage", 0, 10, 100, 11.0)
    rows += series("steer-fl", "supply_voltage", 0, 10, 100, 12.5)
    for row in rows:
        if row["slot_id"] == "hood" and 3 * S <= row["t_us"] < 3.01 * S:
            row["value"] = 6.4
    path = write_silver(tmp_path / "silver", rows)
    env = battery_envelope(duckdb.connect(), path, _window())
    assert env["min"][300] == 6.4
    assert env["mean"][300] == 12.0  # median across devices
    assert env["min"][100] == 11.0 and env["max"][100] == 12.5


def test_temperature_change_points(tmp_path: Path) -> None:
    rows = points(
        "drive-fl", "temp_c",
        [(-1.0, 21.0), (0.5, 21.0), (1.0, 22.0), (1.5, 22.0), (2.0, 22.0), (8.0, 30.0)],
    )  # fmt: skip
    rows += points("hood", "temp_c", [(20.0, 40.0)])  # after the window only
    path = write_silver(tmp_path / "silver", rows)
    temps = temperature_points(duckdb.connect(), path, _window())
    # seconds from match start (2 s); the reading before the window is carried in at t0
    assert temps["drive-fl"] == [[-2.0, 21.0], [-1.0, 22.0], [6.0, 30.0]]
    assert "hood" not in temps


def test_window_between_any_range() -> None:
    long = window_between(100 * S, 130 * S, match_start_us=90 * S)
    assert long.width_us == 30_000 and long.n == 1000 and long.end_us >= 130 * S
    assert long.time_s(0) == 10.0
    short = window_between(100 * S, 102 * S, match_start_us=90 * S)  # 2 ms would be too narrow
    assert short.width_us == 10_000 and short.n == 200
    odd = window_between(0, 12_345_678, match_start_us=0)
    assert odd.width_us % 10_000 == 0 and odd.width_us >= 10_000 and odd.n <= 1000
    assert odd.end_us >= 12_345_678


def test_window_for_is_padded_window_between() -> None:
    assert window_for(100 * S, 264_100_000) == window_between(
        98 * S, 266_100_000, match_start_us=100 * S
    )


def test_two_second_window_keeps_spike(tmp_path: Path) -> None:
    rows = series("drive-fl", "supply_current", 0, 10, 1000, 10.0)
    for row in rows:
        if 5.0 * S <= row["t_us"] < 5.004 * S:
            row["value"] = 150.0
    path = write_silver(tmp_path / "silver", rows)
    window = window_between(4 * S, 6 * S, match_start_us=2 * S)
    env = slot_envelopes(duckdb.connect(), path, window)["drive-fl"]["supply_current"]
    assert env["max"][100] == 150.0 and window.time_s(100) == 3.0  # T+3 s is lake 5 s
    assert all(v == 10.0 for i, v in enumerate(env["max"]) if i != 100)


def test_window_buckets_match_silver(tmp_path: Path) -> None:
    rng = random.Random(7)
    rows = series("hood", "stator_current", 0, 20, 250, 0.0)
    for row in rows:
        row["value"] = round(rng.uniform(-40, 80), 2)
    path = write_silver(tmp_path / "silver", rows)
    window = window_between(round(3.3 * S), round(9.7 * S), match_start_us=0)
    env = slot_envelopes(duckdb.connect(), path, window)["hood"]["stator_current"]
    for bucket in range(window.n):
        lo = window.t0_us + bucket * window.width_us
        inside = [r["value"] for r in rows if lo <= r["t_us"] < lo + window.width_us]
        assert (env["min"][bucket], env["max"][bucket]) == (
            (min(inside), max(inside)) if inside else (None, None)
        )


def test_window_temperature_carries_last_reading_in(tmp_path: Path) -> None:
    rows = points("drive-fl", "temp_c", [(1.0, 30.0), (4.0, 31.0), (7.0, 33.0), (12.0, 35.0)])
    path = write_silver(tmp_path / "silver", rows)
    window = window_between(5 * S, 10 * S, match_start_us=2 * S)
    temps = temperature_points(duckdb.connect(), path, window)
    assert temps["drive-fl"] == [[3.0, 31.0], [5.0, 33.0]]
