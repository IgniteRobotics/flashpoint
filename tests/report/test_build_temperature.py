"""Replay's temperature on non-Pro hoots, per bus (match-reports "Temperature on non-Pro hoots";
mvp-outside-team-logs 6.5)."""

from pathlib import Path
from typing import Any

from flashpoint.report.build import BUDGET_BYTES
from flashpoint.report.paths import data_dir
from tests.report.lake import MatchLake, quiet_rows
from tests.report.silver import series
from tests.report.test_build import _build, _load

CANIVORE = "6E9415C3394C485320202050101C18FF"
RIO_SLOTS = ("hood", "intake-roller")  # quiet_rows' rio slots; drive-fl is on the CANivore


def _gyro() -> list[dict[str, Any]]:
    return series("gyro", "supply_voltage", 0, 180, 10, 12.4)


def _match(tmp_path: Path, rows: list[dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm8", "q8", rows + _gyro(), **kwargs)
    _build(lake)
    data: dict[str, Any] = _load(data_dir(lake.lake) / "2026gacmp_qm8.js")
    return data


def _reasons(data: dict[str, Any]) -> dict[str, Any]:
    return {s["id"]: s["temp_reason"] for s in data["slots"]}


def test_whole_match_non_pro(tmp_path: Path) -> None:
    rows = [r for r in quiet_rows() if r["metric"] != "temp_c"]
    data = _match(tmp_path, rows, pro_licensed=0)
    temperature = data["temperature"]
    assert temperature["available"] is False
    assert {b["bus"] for b in temperature["missing_by_bus"]} == {"rio", CANIVORE}
    assert "not Pro-licensed" in temperature["reason"]
    assert "Phoenix Pro" in temperature["reason"]
    assert _reasons(data) == {
        "drive-fl": "needs-pro", "hood": "needs-pro", "intake-roller": "needs-pro",
        "gyro": "not-tracked",
    }  # fmt: skip
    assert not [m for m in data["markers"] if m["kind"] == "temperature"]


def test_mixed_pro_and_non_pro_buses(tmp_path: Path) -> None:
    rows = [r for r in quiet_rows() if not (r["metric"] == "temp_c" and r["slot_id"] in RIO_SLOTS)]
    data = _match(tmp_path, rows, pro_by_bus={"rio": 0, CANIVORE: 1})
    temperature = data["temperature"]
    assert temperature["available"] is True
    assert [b["bus"] for b in temperature["missing_by_bus"]] == ["rio"]
    assert temperature["missing_by_bus"][0]["name"] == "rio"
    assert _reasons(data) == {
        "drive-fl": None, "hood": "needs-pro", "intake-roller": "needs-pro", "gyro": "not-tracked",
    }  # fmt: skip
    hottest = max(
        (s for s, m in data["maxima"].items() if "temp_max_c" in m),
        key=lambda s: data["maxima"][s]["temp_max_c"],
    )
    assert hottest == "drive-fl"
    assert all("temp_max_c" not in data["maxima"].get(s, {}) for s in RIO_SLOTS)


def test_pro_match_names_no_bus(tmp_path: Path) -> None:
    data = _match(tmp_path, quiet_rows())
    assert data["temperature"]["missing_by_bus"] == []
    assert _reasons(data)["gyro"] == "not-tracked"
    assert _reasons(data)["hood"] is None


def test_non_pro_build_succeeds(tmp_path: Path) -> None:
    rows = [r for r in quiet_rows() if r["metric"] != "temp_c"]
    data = _match(tmp_path, rows, pro_licensed=0)
    assert data["status"] == "ok"
    assert data["bytes"] <= BUDGET_BYTES
