from pathlib import Path
from typing import Any

import duckdb
import pytest

from flashpoint.report.envelope import Window, temperature_points
from flashpoint.report.markers import match_markers
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
    for row in rows:  # every device at 6.5 V for 0.2 s at T+97 s
        if row["metric"] == "supply_voltage" and 107 * S <= row["t_us"] < 107.2 * S:
            row["value"] = 6.5
    markers = _markers(tmp_path, rows)
    assert len(markers) == 1
    marker = markers[0]
    assert (marker["level"], marker["source"], marker["kind"]) == ("FAULT", "power", "brownout")
    assert marker["t"] == pytest.approx(97.0)
    assert marker["end"] == pytest.approx(97.2, abs=0.03)
    assert "6.50 V" in marker["message"]


def test_brief_dip_below_brownout_is_only_a_sag(tmp_path: Path) -> None:
    rows = _quiet()
    for row in rows:  # one device, one sample at 6.5 V held 10 ms (< the 20 ms minimum)
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


def test_sample_gap(tmp_path: Path) -> None:
    rows = [r for r in _quiet() if not (r["slot_id"] == "hood" and 70 * S <= r["t_us"] < 72.5 * S)]
    markers = _markers(tmp_path, rows)
    assert [(m["level"], m["source"], m["kind"]) for m in markers] == [("WARN", "hood", "gap")]
    assert markers[0]["t"] == pytest.approx(60.0 - 0.02, abs=0.01)
    assert markers[0]["message"] == "No samples for 2.5 s"


def test_slot_that_stops_logging(tmp_path: Path) -> None:
    rows = [r for r in _quiet() if not (r["slot_id"] == "hood" and r["t_us"] >= 165 * S)]
    markers = _markers(tmp_path, rows)
    assert [(m["source"], m["kind"]) for m in markers] == [("hood", "gap")]
    assert markers[0]["message"] == "No samples for 5.0 s"


def test_thresholds_from_config(tmp_path: Path) -> None:
    rows = [r for r in _quiet() if not (r["slot_id"] == "hood" and r["metric"] == "temp_c")]
    rows += points("hood", "temp_c", [(8.0, 40.0), (118.0, 62.0)])
    assert _markers(tmp_path, rows) == []
    markers = _markers(tmp_path, rows, ReportConfig(temp_warn_c=60.0))
    assert [m["message"] for m in markers] == ["Temperature 62 °C"]


def test_markers_state_facts_only(tmp_path: Path) -> None:
    rows = _quiet()
    for row in rows:
        if row["metric"] == "supply_voltage" and 107 * S <= row["t_us"] < 107.2 * S:
            row["value"] = 6.5
    for marker in _markers(tmp_path, rows):
        assert set(marker) == {"level", "source", "kind", "t", "end", "message"}
        assert "cause" not in marker["message"].lower() and "fix" not in marker["message"].lower()
