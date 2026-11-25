"""History's N/A and NEEDS PRO health in Chromium (lifetime-trends; mvp-outside-team-logs 6.4)."""

from pathlib import Path
from typing import Any

import pytest

from tests.views.test_temperature_health import _lake
from tests.web.test_browser import _open, _serve, _stop

pytestmark = pytest.mark.browser


def _row(page: Any, unit: str) -> Any:
    return page.locator(f"#rows tr:has(button[data-unit='{unit}'])")


def _site(lake: Any) -> tuple[Any, str]:
    server = _serve(lake)
    return server, f"http://127.0.0.1:{server.server_address[1]}/"


def test_non_pro_lake(page: Any, tmp_path: Path) -> None:
    server, url = _site(_lake(tmp_path / "lake", 3, lambda slot, i: (None, "needs-pro")))
    try:
        watch = _open(page, url, "#view=history&metric=peak_temp")
        page.wait_for_selector("#rows tr")
        hood = _row(page, "ctre:HOOD")
        badge = hood.locator(".status")
        assert badge.inner_text() == "NEEDS PRO"
        classes = str(badge.get_attribute("class"))
        assert "status--pro" in classes
        assert "status--fault" not in classes and "status--retired" not in classes
        assert hood.locator("[data-latest]").inner_text() == "not logged (needs Phoenix Pro)"
        assert (
            hood.locator(".spark-cell").get_attribute("title") == "not logged (needs Phoenix Pro)"
        )
        tiles = page.inner_text("#tiles").split("\n")
        assert tiles[tiles.index("UNITS WARN/HOT") + 1] == "0"

        page.goto(url + "#view=history&metric=isolated_gaps")
        page.wait_for_selector("#rows tr:has(button[data-unit='ctre:GYRO'])")
        gyro = _row(page, "ctre:GYRO").locator(".status")
        assert gyro.inner_text() == "N/A" and "status--na" in str(gyro.get_attribute("class"))
        # On a CAN metric the latest value is the metric's, not a temperature note.
        assert _row(page, "ctre:HOOD").locator("[data-latest]").inner_text() == "0.0"
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_temperature_not_tracked_on_a_temperature_metric(page: Any, tmp_path: Path) -> None:
    server, url = _site(_lake(tmp_path / "lake", 3, lambda slot, i: (None, "not-tracked")))
    try:
        watch = _open(page, url, "#view=history&metric=peak_temp")
        page.wait_for_selector("#rows tr")
        hood = _row(page, "ctre:HOOD")
        assert hood.locator(".status").inner_text() == "N/A"
        assert hood.locator("[data-latest]").inner_text() == "temperature not tracked"
        assert hood.locator(".spark-cell").get_attribute("title") == "temperature not tracked"
        assert watch.clean(), watch
    finally:
        _stop(server)
