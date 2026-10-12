from pathlib import Path
from typing import Any

import duckdb
import pytest

from flashpoint.report.envelope import Window, temperature_points
from flashpoint.report.markers import can_markers, match_markers
from flashpoint.report.settings import ReportConfig
from tests.report.silver import S, points, series, write_silver

MATCH_START, MATCH_END = 10 * S, 170 * S
WINDOW = Window(t0_us=8 * S, width_us=170_000, n=1000, match_start_us=MATCH_START)
SLOTS = ("drive-fl", "hood", "intake-roller")


def _quiet() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for slot in SLOTS:
        rows += series(slot, "supply_voltage", 8, 175, 50, 12.0)
        rows += series(slot, "stator_current", 8, 175, 50, 10.0)
        rows += series(slot, "rotor_velocity_rps", 8, 175, 50, 40.0)
        rows += series(slot, "stall_current", 8, 9, 1, 300.0)
        rows += points(slot, "temp_c", [(8.0, 30.0), (100.0, 40.0)])
    return rows


def _markers(
    tmp_path: Path, rows: list[dict[str, Any]], config: ReportConfig | None = None
) -> list[dict[str, Any]]:
    path = write_silver(tmp_path / "silver", rows)
    con = duckdb.connect()
    temps = temperature_points(con, path, WINDOW)
    return match_markers(con, path, WINDOW, MATCH_END, temps, config or ReportConfig())


def test_quiet_match_has_no_markers(tmp_path: Path) -> None:
    assert _markers(tmp_path, _quiet()) == []


def test_temperature_warn_and_fault(tmp_path: Path) -> None:
    rows = [r for r in _quiet() if not (r["slot_id"] == "hood" and r["metric"] == "temp_c")]
    rows += points("hood", "temp_c", [(8.0, 40.0), (118.0, 66.0), (130.0, 70.0), (150.0, 75.0)])
    markers = _markers(tmp_path, rows)
    assert [(m["level"], m["source"], m["t"], m["message"]) for m in markers] == [
        ("WARN", "hood", 108.0, "Temperature 66 °C"),
        ("FAULT", "hood", 140.0, "Temperature 75 °C"),
    ]
    assert all(m["kind"] == "temperature" for m in markers)


def test_brownout_is_one_fault_not_also_a_sag(tmp_path: Path) -> None:
    rows = _quiet()
    for row in rows:  # every device at 6.5 V for 0.6 s at T+97 s
        if row["metric"] == "supply_voltage" and 107 * S <= row["t_us"] < 107.6 * S:
            row["value"] = 6.5
    markers = _markers(tmp_path, rows)
    assert len(markers) == 1
    marker = markers[0]
    assert (marker["level"], marker["source"], marker["kind"]) == ("FAULT", "power", "brownout")
    assert marker["t"] == pytest.approx(97.0)
    assert marker["end"] == pytest.approx(97.6, abs=0.03)
    assert "6.50 V" in marker["message"]


def test_brief_dip_below_brownout_is_only_a_sag(tmp_path: Path) -> None:
    rows = _quiet()
    for row in rows:  # one device, one sample at 6.5 V held 10 ms (< the 500 ms minimum)
        hood = row["slot_id"] == "hood" and row["metric"] == "supply_voltage"
        if hood and 50 * S <= row["t_us"] < 50.01 * S:
            row["value"] = 6.5
    rows += points("hood", "supply_voltage", [(50.01, 12.0)])
    markers = _markers(tmp_path, rows)
    assert [(m["level"], m["kind"], m["t"]) for m in markers] == [("WARN", "sag", 40.0)]


def test_sag_warn(tmp_path: Path) -> None:
    rows = _quiet()
    for row in rows:
        drive = row["slot_id"] == "drive-fl" and row["metric"] == "supply_voltage"
        if drive and 60 * S <= row["t_us"] < 61 * S:
            row["value"] = 7.6
    markers = _markers(tmp_path, rows)
    assert [(m["level"], m["source"], m["kind"]) for m in markers] == [("WARN", "power", "sag")]
    assert markers[0]["t"] == pytest.approx(50.0) and "7.60 V" in markers[0]["message"]


def test_stall_interval(tmp_path: Path) -> None:
    rows = _quiet()
    for row in rows:
        if row["slot_id"] == "intake-roller" and 40 * S <= row["t_us"] < 44 * S:
            if row["metric"] == "stator_current":
                row["value"] = 80.0  # 0.27 of stall: current-limited
            elif row["metric"] == "rotor_velocity_rps":
                row["value"] = 0.1
    markers = _markers(tmp_path, rows)
    assert [(m["level"], m["source"], m["kind"]) for m in markers] == [
        ("WARN", "intake-roller", "stall")
    ]
    assert markers[0]["t"] == pytest.approx(30.0)
    assert markers[0]["end"] == pytest.approx(34.0, abs=0.05)
    assert markers[0]["message"] == "Stalled 4.0 s, stator up to 80 A"


def test_silver_silence_alone_is_no_marker(tmp_path: Path) -> None:
    """Gaps come from CAN health (frame clocks and siblings), not from silver samples."""
    rows = [r for r in _quiet() if not (r["slot_id"] == "hood" and 70 * S <= r["t_us"] < 72.5 * S)]
    rows = [r for r in rows if not (r["slot_id"] == "hood" and r["t_us"] >= 165 * S)]
    assert _markers(tmp_path, rows) == []


def _event(kind: str, t_s: float, **extra: Any) -> dict[str, Any]:
    """A gold can_events row; t_s is match time."""
    return {
        "kind": kind, "bus": "rio", "device": "TalonFX-2", "slot_id": "intake-roller",
        "unit_id": "ctre:A", "t_us": MATCH_START + round(t_s * S), "duration_ms": None,
        "enabled": True, "reset_before": None, "reset_after": None, **extra,
    }  # fmt: skip


def _can(events: list[dict[str, Any]], **kw: Any) -> list[dict[str, Any]]:
    return can_markers(events, WINDOW, MATCH_END, ReportConfig(**kw), {"rio": "rio",
                       "6E9415C3394C485320202050101C18FF": "DriveTrain"})  # fmt: skip


def test_reboot_marker() -> None:
    (marker,) = _can([_event("reboot", 128.0, reset_before=0, reset_after=1)])
    assert (marker["level"], marker["source"], marker["kind"]) == (
        "FAULT",
        "intake-roller",
        "reboot",
    )
    assert marker["t"] == pytest.approx(128.0)
    assert marker["message"] == "Device rebooted (reset count 0 → 1)"


def test_reboot_without_reset_count() -> None:
    (marker,) = _can([_event("reboot", 30.0)])
    assert marker["message"] == "Device rebooted"


def test_long_dropout_marker() -> None:
    (marker,) = _can([_event("dropout", 127.0, duration_ms=820.0)])
    assert (marker["level"], marker["source"], marker["kind"]) == (
        "WARN",
        "intake-roller",
        "dropout",
    )
    assert marker["t"] == pytest.approx(127.0) and marker["end"] == pytest.approx(127.82)
    assert marker["message"] == "No CAN frames for 0.82 s"


def test_short_dropout_below_threshold() -> None:
    assert _can([_event("dropout", 127.0, duration_ms=200.0)]) == []
    assert len(_can([_event("dropout", 127.0, duration_ms=200.0)], dropout_warn_s=0.15)) == 1


def test_whole_bus_silent() -> None:
    gap = _event("bus_gap", 60.0, device=None, slot_id=None, unit_id=None, duration_ms=1400.0)
    (marker,) = _can([gap])
    assert (marker["level"], marker["source"], marker["kind"]) == ("WARN", "bus:rio", "bus_gap")
    assert marker["message"] == "Whole bus silent for 1.4 s"
    canivore = {**gap, "bus": "6E9415C3394C485320202050101C18FF"}
    assert _can([canivore])[0]["source"] == "bus:DriveTrain"
    unnamed = {**gap, "bus": "D4F5AE23394C4853202020500E3418FF"}
    assert _can([unnamed])[0]["source"] == "bus:D4F5AE23394C4853202020500E3418FF"


def test_short_bus_gap_is_no_marker() -> None:
    gap = _event("bus_gap", 60.0, device=None, slot_id=None, unit_id=None, duration_ms=600.0)
    assert _can([gap]) == []


def test_unmapped_device_gets_no_marker() -> None:
    events = [
        _event("reboot", 30.0, slot_id=None, unit_id=None, device="CANdle-60"),
        _event("dropout", 40.0, slot_id=None, unit_id=None, device="CANdle-60", duration_ms=900.0),
    ]
    assert _can(events) == []


def test_events_outside_the_match_or_unaligned_are_ignored() -> None:
    events = [
        _event("reboot", -5.0, reset_before=0, reset_after=1),
        _event("reboot", 165.0, reset_before=1, reset_after=2),
        {**_event("reboot", 30.0), "t_us": None},
    ]
    assert _can(events) == []


def test_can_markers_merge_with_other_rules(tmp_path: Path) -> None:
    path = write_silver(tmp_path / "silver", _quiet())
    con = duckdb.connect()
    temps = temperature_points(con, path, WINDOW)
    events = [_event("reboot", 128.0, reset_before=0, reset_after=1)]
    markers = match_markers(con, path, WINDOW, MATCH_END, temps, ReportConfig(), events)
    assert [(m["kind"], m["t"]) for m in markers] == [("reboot", 128.0)]


def test_thresholds_from_config(tmp_path: Path) -> None:
    rows = [r for r in _quiet() if not (r["slot_id"] == "hood" and r["metric"] == "temp_c")]
    rows += points("hood", "temp_c", [(8.0, 40.0), (118.0, 62.0)])
    assert _markers(tmp_path, rows) == []
    markers = _markers(tmp_path, rows, ReportConfig(temp_warn_c=60.0))
    assert [m["message"] for m in markers] == ["Temperature 62 °C"]


def test_markers_state_facts_only(tmp_path: Path) -> None:
    rows = _quiet()
    for row in rows:
        if row["metric"] == "supply_voltage" and 107 * S <= row["t_us"] < 107.6 * S:
            row["value"] = 6.5
    for marker in _markers(tmp_path, rows):
        assert set(marker) == {"level", "source", "kind", "t", "end", "message"}
        assert "cause" not in marker["message"].lower() and "fix" not in marker["message"].lower()
