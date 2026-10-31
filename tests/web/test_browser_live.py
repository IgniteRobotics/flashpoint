"""The Live CAN view in Chromium against the fake robot (spec: live-can-health)."""

import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flashpoint.lake.paths import LakePaths
from flashpoint.live.fake_robot import FakeRobot, SetDevice, SetEnabled, SetVersion
from flashpoint.live.service import LiveCanService
from flashpoint.live.slots import load_resolver
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.server import FlashpointServer
from tests.conftest import REPO_CONFIG
from tests.live.test_fake_robot import DEVICES, free_port
from tests.web.test_browser import Watch, _open, _stop

pytestmark = pytest.mark.browser
GUIDE = "docs/guide/live-can-health.md"


def _serve(tmp_path: Path, port: int) -> FlashpointServer:
    lake = LakePaths(tmp_path / "lake")
    live = LiveCanService(
        ["127.0.0.1"], port=port, resolver=load_resolver(REPO_CONFIG), linger_s=0.5
    )
    server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake), live=live)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


@pytest.fixture
def robot() -> Iterator[FakeRobot]:
    with FakeRobot(DEVICES, port=free_port()) as fake:
        yield fake


@pytest.fixture
def site(tmp_path: Path, robot: FakeRobot) -> Iterator[str]:
    server = _serve(tmp_path, robot.port)
    yield f"http://127.0.0.1:{server.server_address[1]}/"
    _stop(server)


def _live(page: Any, url: str) -> Watch:
    watch = _open(page, url, "#view=live")
    page.wait_for_selector("#live-connection[data-connection='connected'][data-status='ok']")
    page.wait_for_selector("[data-bus] .mech")
    return watch


def _devices(page: Any) -> list[str]:
    return [
        str(d) for d in page.eval_on_selector_all(
            ".mech:not([data-state='missing'])", "els => els.map(e => e.dataset.device)"
        )
    ]


def test_contract_accepted(page: Any, site: str) -> None:
    watch = _live(page, site)
    assert page.inner_text("#live-connection") == "CONNECTED"
    assert page.inner_text("#live-enabled") == "DISABLED"
    buses = page.eval_on_selector_all("[data-bus]", "els => els.map(e => e.dataset.bus)")
    assert buses == ["DriveTrain", "rio"]
    assert sorted(_devices(page)) == sorted(n for names in DEVICES.values() for n in names)
    card = page.locator(".mech[data-device='TalonFX-11']")
    assert card.get_attribute("data-slot") == "drive-fl"
    assert card.get_attribute("data-state") == "ok"
    assert "12.40 V" in card.inner_text()
    # Every other 2026 slot is configured but not reported.
    assert page.locator(".mech[data-slot='steer-fr'][data-state='missing']").count() == 1
    assert page.locator("[data-bus='rio'] [data-utilization]").inner_text() == "35%"
    assert watch.clean(), watch


def test_shake_test_dropout_shows_within_a_second(page: Any, site: str, robot: FakeRobot) -> None:
    watch = _live(page, site)
    sent = time.monotonic()
    robot.apply(SetDevice("DriveTrain", "TalonFX-11", Dropouts=2, MaxAgeMs=48.0))
    page.wait_for_selector("#live-log [data-event='dropout'][data-slot='drive-fl']", timeout=2000)
    assert time.monotonic() - sent < 1.0
    line = page.inner_text("#live-log [data-event='dropout']")
    assert "DISABLED" in line and "drive-fl (TalonFX-11)" in line and "×2 · 48 ms" in line
    page.wait_for_selector(".mech[data-device='TalonFX-11'][data-state='dropped']")
    assert page.inner_text(".mech[data-device='TalonFX-11'] [data-total='dropouts']") == "2"
    assert watch.clean(), watch


def test_marked_shake_test(page: Any, site: str, robot: FakeRobot) -> None:
    watch = _live(page, site)
    page.fill("#live-mark-input", "FL module")
    page.click("#live-mark-set")
    assert page.inner_text("#live-mark-current") == "marking: FL module"
    robot.apply(SetDevice("DriveTrain", "TalonFX-11", Dropouts=2))
    line = page.wait_for_selector("#live-log [data-event='dropout']")
    assert line.query_selector("[data-mark='FL module']") is not None
    # Clearing stops marking later events.
    page.click("#live-mark-clear")
    robot.apply(SetDevice("rio", "TalonFX-2", Dropouts=1))
    later = page.wait_for_selector("#live-log [data-event='dropout'][data-device='TalonFX-2']")
    assert later.query_selector("[data-mark]") is None
    assert watch.clean(), watch


def test_event_log_newest_first_with_enabled_tags(page: Any, site: str, robot: FakeRobot) -> None:
    watch = _live(page, site)
    robot.apply(SetDevice("rio", "TalonFX-8", Dropouts=1))
    page.wait_for_selector("#live-log [data-event='dropout']")
    robot.apply(SetEnabled(True))
    page.wait_for_selector("#live-enabled[data-enabled='true']")
    robot.apply(SetDevice("rio", "TalonFX-2", Resets=1, Faults=0b10))
    page.wait_for_selector("#live-log [data-event='fault']")
    kinds = page.eval_on_selector_all(
        "#live-log [data-event]", "els => els.map(e => e.dataset.event)"
    )
    assert kinds[-1] == "dropout" and set(kinds[:2]) == {"reboot", "fault"}
    tags = page.eval_on_selector_all(
        "#live-log [data-event] [data-enabled]", "els => els.map(e => e.textContent)"
    )
    assert tags == ["ENABLED", "ENABLED", "DISABLED"]
    assert watch.clean(), watch


def test_bus_wide_event_names_its_devices(page: Any, site: str, robot: FakeRobot) -> None:
    watch = _live(page, site)
    robot.run([
        SetDevice("DriveTrain", "TalonFX-11", Dropouts=1),
        SetDevice("DriveTrain", "TalonFX-12", Dropouts=1),
    ])
    line = page.wait_for_selector("#live-log [data-event='bus-dropout']")
    assert "DriveTrain: TalonFX-11, TalonFX-12" in line.inner_text()
    assert page.locator("#live-log [data-event='dropout']").count() == 0
    assert watch.clean(), watch


def test_reload_drops_marks(page: Any, site: str, robot: FakeRobot) -> None:
    watch = _live(page, site)
    page.fill("#live-mark-input", "FL module")
    page.press("#live-mark-input", "Enter")
    robot.apply(SetDevice("DriveTrain", "TalonFX-11", Dropouts=2))
    page.wait_for_selector("#live-log [data-event='dropout']")
    page.reload()
    page.wait_for_selector("[data-bus] .mech[data-device='TalonFX-11']")
    assert page.locator("#live-log [data-event]").count() == 0
    assert page.inner_text("#live-mark-current") == "no mark"
    assert page.input_value("#live-mark-input") == ""
    # Totals come from the server, which kept its robot connection.
    assert page.inner_text(".mech[data-device='TalonFX-11'] [data-total='dropouts']") == "2"
    assert watch.clean(), watch


def test_unknown_contract_version(page: Any, site: str, robot: FakeRobot) -> None:
    watch = _live(page, site)
    robot.apply(SetVersion(2))
    page.wait_for_selector("#live-unsupported")
    text = page.inner_text("#live-unsupported")
    assert "version 2" in text and "supports version 1" in text
    assert page.inner_text("#live-connection") == "UNSUPPORTED VERSION"
    assert page.locator(".mech").count() == 0
    robot.apply(SetDevice("DriveTrain", "TalonFX-11", Dropouts=5))
    time.sleep(0.6)
    assert page.locator("#live-log [data-event]").count() == 0
    assert watch.clean(), watch


def test_robot_code_without_the_publisher(page: Any, tmp_path: Path) -> None:
    with FakeRobot(DEVICES, port=free_port(), publish=False) as fake:
        server = _serve(tmp_path, fake.port)
        try:
            watch = _open(page, f"http://127.0.0.1:{server.server_address[1]}/", "#view=live")
            page.wait_for_selector("#live-not-publishing", timeout=5000)
            assert page.inner_text("#live-connection") == "NOT PUBLISHING"
            assert page.inner_text("#live-guide") == GUIDE
            assert page.locator(".mech").count() == 0
            assert watch.clean(), watch
        finally:
            _stop(server)


def test_no_robot(page: Any, tmp_path: Path) -> None:
    server = _serve(tmp_path, free_port())
    try:
        watch = _open(page, f"http://127.0.0.1:{server.server_address[1]}/", "#view=live")
        page.wait_for_selector("#live-connecting")
        assert page.inner_text("#live-connection") == "CONNECTING"
        page.click("nav.tabs a[data-view='history']")
        page.wait_for_selector("#view")
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_keyboard_reaches_mark_and_log(page: Any, site: str, robot: FakeRobot) -> None:
    watch = _live(page, site)
    robot.apply(SetDevice("rio", "TalonFX-8", Dropouts=1))
    page.wait_for_selector("#live-log [data-event='dropout']")
    page.focus("nav.tabs a[data-view='live']")
    seen: list[str] = []
    for _ in range(12):
        page.keyboard.press("Tab")
        seen.append(str(page.evaluate("document.activeElement.id")))
        if seen[-1] == "live-log":
            break
    assert {"live-mark-input", "live-mark-set", "live-mark-clear", "live-log"} <= set(seen)
    page.focus("#live-mark-input")
    page.keyboard.type("BR module")
    page.keyboard.press("Enter")
    assert page.inner_text("#live-mark-current") == "marking: BR module"
    assert watch.clean(), watch
