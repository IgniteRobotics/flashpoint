"""CAN health detection on synthetic hoots (spec: can-health)."""

from pathlib import Path

import duckdb
import pytest

from flashpoint.lake.paths import LakePaths
from flashpoint.semantics import can_health
from flashpoint.semantics.can_health import HootHealth
from tests import can_health_builder as b

S = b.S


def _analyse(tmp_path: Path, hoot: b.Hoot) -> HootHealth:
    lake = LakePaths(tmp_path / "lake")
    b.write_hoot(lake, "h1", hoot)
    return can_health.analyse_hoot(duckdb.connect(), lake, "h1")


def _gaps(health: HootHealth, device: str) -> list[can_health.Gap]:
    return [g for g in health.gaps if g.device == device]


# --- 3.1 enabled windows and start-up -----------------------------------------------------


def test_enabled_intervals_from_robot_enable(tmp_path: Path) -> None:
    hoot = b.Hoot([b.Device("TalonFX-1")], duration_s=30.0, enabled=[(6.0, 12.0), (14.0, 20.0)])
    health = _analyse(tmp_path, hoot)
    assert len(health.enabled) == 2
    (a0, a1), (c0, c1) = health.enabled
    assert a0 == pytest.approx(6 * S, abs=45_000) and a1 == pytest.approx(12 * S, abs=45_000)
    assert c0 == pytest.approx(14 * S, abs=45_000) and c1 == pytest.approx(20 * S, abs=45_000)


def test_never_enabled_has_no_windows_and_no_gaps(tmp_path: Path) -> None:
    device = b.Device("TalonFX-1", silences=[(10.0, 2.0)])
    health = _analyse(tmp_path, b.Hoot([device], duration_s=30.0))
    assert health.enabled == [] and health.gaps == []


def test_start_up_ignored(tmp_path: Path) -> None:
    """Frames in the first 5 s of a hoot never make a gap, even while enabled."""
    devices = [b.Device(f"TalonFX-{i}") for i in range(1, 4)]
    devices[0].silences = [(2.0, 0.5)]
    health = _analyse(tmp_path, b.Hoot(devices, duration_s=30.0, enabled=[(0.0, 29.0)]))
    assert health.gaps == []


# --- 3.2 nominal period and gaps -----------------------------------------------------------


def _ten(silent: b.Device) -> b.Hoot:
    others = [b.Device(f"TalonFX-{i}") for i in range(2, 11)]
    return b.Hoot([silent, *others], duration_s=30.0, enabled=[(6.0, 29.0)])


def test_gap_against_the_devices_own_period(tmp_path: Path) -> None:
    health = _analyse(tmp_path, _ten(b.Device("TalonFX-1", 8.0, silences=[(20.0, 0.040)])))
    assert health.nominal_us["TalonFX-1"] == 8_000
    (gap,) = _gaps(health, "TalonFX-1")
    assert (gap.start_us, gap.end_us, gap.missed) == (20 * S, 20 * S + 40_000, 4)


def test_slow_device_has_its_own_period(tmp_path: Path) -> None:
    """A 20 ms gap is a gap for an 8 ms device but jitter for a 10 ms CANcoder at 2.5x."""
    slow = b.Device("CANcoder-13", 10.0, silences=[(20.0, 0.020)])
    health = _analyse(tmp_path, _ten(slow))
    assert health.nominal_us["CANcoder-13"] == 10_000
    assert _gaps(health, "CANcoder-13") == []


def test_normal_jitter_is_not_a_gap(tmp_path: Path) -> None:
    jittery = b.Device("TalonFX-1", intervals_ms=[8, 8, 8, 8, 8, 8, 19, 5, 8, 8, 8, 8, 8, 8])
    health = _analyse(tmp_path, _ten(jittery))
    assert health.nominal_us["TalonFX-1"] == 8_000
    assert _gaps(health, "TalonFX-1") == []


def test_disabled_time_ignored(tmp_path: Path) -> None:
    silent = b.Device("TalonFX-1", silences=[(2.0 + 28.0, 2.0)])
    hoot = _ten(silent)
    hoot.duration_s, hoot.enabled = 40.0, [(6.0, 29.0)]
    assert _gaps(_analyse(tmp_path, hoot), "TalonFX-1") == []


def test_duplicate_frame_times_do_not_shrink_the_period(tmp_path: Path) -> None:
    """Two status groups arriving in one frame time share a timestamp (seen in Q105)."""
    doubled = b.Device("TalonFX-1", intervals_ms=[0.0, 8.0])
    assert _analyse(tmp_path, _ten(doubled)).nominal_us["TalonFX-1"] == 8_000


# --- 3.3 trailing silence and hoot end ----------------------------------------------------


def test_device_stops_before_the_window_ends(tmp_path: Path) -> None:
    health = _analyse(tmp_path, b.stops_early())
    (gap,) = _gaps(health, "TalonFX-3")
    assert gap.start_us == 22 * S
    assert gap.end_us == pytest.approx(25 * S, abs=45_000)  # the enabled window's end
    assert not gap.bus_wide


def test_hoot_end_is_not_a_gap(tmp_path: Path) -> None:
    """Every device stops together (an incomplete-read hoot): no gap past the last frame."""
    devices = [b.Device(f"TalonFX-{i}", stop_s=20.0) for i in range(1, 11)]
    health = _analyse(tmp_path, b.Hoot(devices, duration_s=20.0, enabled=[(6.0, 29.0)]))
    assert health.gaps == []
    assert health.last_frame_us == 20 * S


def test_trailing_gap_ends_at_last_frame_of_any_device(tmp_path: Path) -> None:
    """Window outlasts the hoot: the stopped device's gap ends at the hoot's last frame."""
    devices = [b.Device(f"TalonFX-{i}", stop_s=24.0) for i in range(1, 11)]
    devices[0].stop_s = 21.0
    health = _analyse(tmp_path, b.Hoot(devices, duration_s=24.0, enabled=[(6.0, 29.0)]))
    (gap,) = _gaps(health, "TalonFX-1")
    assert (gap.start_us, gap.end_us) == (21 * S, 24 * S)


# --- 3.4 isolated vs bus-wide --------------------------------------------------------------


def test_one_device_drops_out(tmp_path: Path) -> None:
    silent = b.Device("TalonFX-1", silences=[(20.0, 0.820)])
    health = _analyse(tmp_path, _ten(silent))
    (gap,) = _gaps(health, "TalonFX-1")
    assert not gap.bus_wide
    assert gap.end_us - gap.start_us == pytest.approx(820_000, abs=8_000)
    assert health.bus_gaps == []


def test_overlapping_not_equal(tmp_path: Path) -> None:
    health = _analyse(tmp_path, b.bus_wide())
    silent = [g for g in health.gaps if g.device in {f"TalonFX-{i}" for i in range(1, 7)}]
    assert len(silent) == 6 and all(g.bus_wide for g in silent)
    assert len(health.bus_gaps) == 1
    (start, end) = health.bus_gaps[0]
    assert start == min(g.start_us for g in silent) and end == max(g.end_us for g in silent)


def test_whole_bus_is_one_bus_gap(tmp_path: Path) -> None:
    health = _analyse(tmp_path, b.whole_bus())
    assert all(g.bus_wide for g in health.gaps) and len(health.gaps) == 10
    ((start, end),) = health.bus_gaps
    assert end - start >= 1_400_000


def test_minority_gapping_together_is_isolated(tmp_path: Path) -> None:
    """Four of ten (fewer than half the siblings) overlapping: each is charged."""
    hoot = b.bus_wide()
    for device in hoot.devices[4:6]:
        device.silences = []
    health = _analyse(tmp_path, hoot)
    assert len(health.gaps) == 4 and not any(g.bus_wide for g in health.gaps)
    assert health.bus_gaps == []


def test_unmapped_devices_are_siblings(tmp_path: Path) -> None:
    """Siblings are every device on the hoot; the analysis knows nothing of slots."""
    hoot = b.bus_wide()
    hoot.devices[6].name = "CANdle-60"
    health = _analyse(tmp_path, hoot)
    assert "CANdle-60" in health.nominal_us
    assert all(g.bus_wide for g in health.gaps)


# --- 3.5 reboots ---------------------------------------------------------------------------


def test_reboot_while_enabled(tmp_path: Path) -> None:
    health = _analyse(tmp_path, b.q105_like())
    (reboot,) = health.reboots
    assert reboot.device == "TalonFX-2"
    assert reboot.t_us == 344_960_000
    assert reboot.enabled
    assert (reboot.reset_before, reboot.reset_after) == (0, 1)
    assert health.reset_count_logged
    (gap,) = [g for g in _gaps(health, "TalonFX-2") if g.end_us - g.start_us > 100_000]
    assert gap.end_us - gap.start_us == pytest.approx(820_000, abs=8_000)
    assert not gap.bus_wide


def test_baseline_is_not_a_reboot(tmp_path: Path) -> None:
    device = b.Device("TalonFX-1", reset_counts=[(0.5, 3), (10.0, 3)])
    assert _analyse(tmp_path, b.Hoot([device], duration_s=20.0)).reboots == []


def test_reboot_while_disabled(tmp_path: Path) -> None:
    device = b.Device("TalonFX-1", reset_counts=[(0.5, 0), (3.0, 1)])
    health = _analyse(tmp_path, b.Hoot([device], duration_s=20.0, enabled=[(6.0, 19.0)]))
    (reboot,) = health.reboots
    assert not reboot.enabled and reboot.t_us == 3 * S


def test_two_reboots_more_than_two_seconds_apart(tmp_path: Path) -> None:
    device = b.Device(
        "TalonFX-1",
        reset_counts=[(0.5, 0), (8.0, 1), (15.0, 2)],
        boot_during_enable=[(7.0, 0.0), (8.5, 1.0), (9.0, 0.0)],
    )
    health = _analyse(tmp_path, b.Hoot([device], duration_s=20.0, enabled=[(6.0, 19.0)]))
    assert [(r.t_us, r.reset_before, r.reset_after) for r in health.reboots] == [
        (8 * S, 0, 1),
        (15 * S, 1, 2),
    ]


def test_boot_during_enable_only(tmp_path: Path) -> None:
    health = _analyse(tmp_path, b.no_reset_count())
    (reboot,) = health.reboots
    assert reboot.device == "TalonFX-2" and reboot.t_us == 20 * S and reboot.enabled
    assert (reboot.reset_before, reboot.reset_after) == (None, None)
    assert not health.reset_count_logged


def test_fault_already_on_at_log_start_is_not_a_reboot(tmp_path: Path) -> None:
    device = b.Device("TalonFX-1", boot_during_enable=[(1.0, 1.0), (1.25, 1.0), (1.5, 0.0)])
    assert _analyse(tmp_path, b.Hoot([device], duration_s=20.0)).reboots == []
