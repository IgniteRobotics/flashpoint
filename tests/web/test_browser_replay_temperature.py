"""Replay's temperature on non-Pro hoots and sensors in Chromium (match-reports, "Temperature on
non-Pro hoots"; mvp-outside-team-logs 6.6)."""

from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.report.build import ReportBuilder
from tests.report.lake import MatchLake, quiet_rows
from tests.report.silver import series
from tests.web.test_browser import _open, _readout_slots, _serve, _stop

pytestmark = pytest.mark.browser
CANIVORE = "6E9415C3394C485320202050101C18FF"
RIO_SLOTS = ("hood", "intake-roller")


def _site(root: Path, rows: list[dict[str, Any]], **kwargs: Any) -> tuple[Any, str]:
    lake = MatchLake(root)
    gyro = series("gyro", "supply_voltage", 0, 180, 10, 12.4)
    lake.add_match("2026gacmp_qm8", "q8", rows + gyro, **kwargs)
    builder = ReportBuilder(lake.write_meta(), config.config_root())
    try:
        builder.build()
    finally:
        builder.close()
    server = _serve(lake.lake)
    return server, f"http://127.0.0.1:{server.server_address[1]}/"


def _q8(page: Any, url: str) -> Any:
    watch = _open(page, url, "#view=replay&m=2026gacmp_qm8")
    page.wait_for_selector("#readout tr")
    return watch


def _temp_cell(page: Any, slot: str) -> str:
    return str(page.inner_text(f"#readout tr[data-slot='{slot}'] td:nth-child(3)"))


def test_whole_match_non_pro(page: Any, tmp_path: Path) -> None:
    rows = [r for r in quiet_rows() if r["metric"] != "temp_c"]
    server, url = _site(tmp_path / "lake", rows, pro_licensed=0)
    try:
        watch = _q8(page, url)
        for slot in ("drive-fl", "hood", "intake-roller"):
            assert _temp_cell(page, slot) == "not logged (needs Phoenix Pro)"
        assert _temp_cell(page, "gyro") == "temperature not tracked"
        note = page.inner_text("#temperature-note")
        # Buses go by their robot-file names: the 2026 robot calls its CANivore "DriveTrain".
        assert "rio" in note and "DriveTrain" in note and "Phoenix Pro-licensed" in note
        # The default temperature track (third) shows the reason and no line.
        assert "not logged (needs Phoenix Pro)" in page.inner_text("[data-range='2']")
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_mixed_pro_and_non_pro_buses(page: Any, tmp_path: Path) -> None:
    rows = [r for r in quiet_rows() if not (r["metric"] == "temp_c" and r["slot_id"] in RIO_SLOTS)]
    server, url = _site(tmp_path / "lake", rows, pro_by_bus={"rio": 0, CANIVORE: 1})
    try:
        watch = _q8(page, url)
        note = page.inner_text("#temperature-note")
        assert "rio" in note and "DriveTrain" not in note
        assert _temp_cell(page, "hood") == "not logged (needs Phoenix Pro)"
        assert _temp_cell(page, "drive-fl") not in ("not logged", "")
        # No cursor: slots with temperature first (only drive-fl), then the rest by current.
        assert _readout_slots(page) == ["drive-fl", "intake-roller", "hood", "gyro"]
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_pro_match_says_nothing_about_temperature(page: Any, tmp_path: Path) -> None:
    server, url = _site(tmp_path / "lake", quiet_rows())
    try:
        watch = _q8(page, url)
        assert page.locator("#temperature-note").count() == 0
        assert _temp_cell(page, "gyro") == "temperature not tracked"
        assert watch.clean(), watch
    finally:
        _stop(server)


@pytest.mark.corpus
def test_sensors_without_temperature_on_a_pro_match(page: Any, derived_corpus_lake: Any) -> None:
    builder = ReportBuilder(derived_corpus_lake, config.config_root())
    try:
        builder.build(match_keys=["2026gacmp_qm7"])
    finally:
        builder.close()
    server = _serve(derived_corpus_lake)
    try:
        watch = _open(page, f"http://127.0.0.1:{server.server_address[1]}/",
                      "#view=replay&m=2026gacmp_qm7")  # fmt: skip
        page.wait_for_selector("#readout tr")
        assert _temp_cell(page, "gyro") == "temperature not tracked"
        for slot in ("encoder-fl", "encoder-fr", "encoder-bl", "encoder-br"):
            assert _temp_cell(page, slot) == "temperature not tracked"
        assert page.locator("#temperature-note").count() == 0
        assert watch.clean(), watch
    finally:
        _stop(server)
