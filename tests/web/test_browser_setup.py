"""The Setup view in Chromium (specs: team-setup, config-editor)."""

import shutil
import threading
import tomllib
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.robot_config import load_config_seasons, load_robot
from flashpoint.setup.checks import CheckContext, run_checks
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.server import FlashpointServer
from tests.setup.test_fixes import fake_registry
from tests.web.test_browser import NetworkServer, Watch, _open, _stop

pytestmark = pytest.mark.browser
ROBOT = "robots/2026-comp.toml"


def _site(
    tmp_path: Path, lake: LakePaths, server_class: type[FlashpointServer] = FlashpointServer
) -> tuple[FlashpointServer, Path]:
    root = tmp_path / "cfg"
    shutil.copytree(config.config_root(), root)
    server = server_class(("127.0.0.1", 0), lake, HistoryQueries(lake))
    server.config_dir = root
    server.check_context = lambda: CheckContext(
        root, lake, tmp_path / "cache", system="Darwin", season=2026,
        registry=fake_registry(tmp_path),
        find_advantagescope=lambda r: (None, "not found"),
    )  # fmt: skip
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, root


@pytest.fixture
def site(tmp_path: Path) -> Iterator[dict[str, Any]]:
    server, root = _site(tmp_path, LakePaths(tmp_path / "lake"))
    yield {"url": f"http://127.0.0.1:{server.server_address[1]}/", "root": root}
    _stop(server)


@pytest.fixture
def shared(tmp_path: Path) -> Iterator[dict[str, Any]]:
    server, root = _site(tmp_path, LakePaths(tmp_path / "lake"), NetworkServer)
    yield {"url": f"http://127.0.0.1:{server.server_address[1]}/", "root": root}
    _stop(server)


def _setup(page: Any, url: str, tab: str = "checks") -> Watch:
    watch = _open(page, url, f"#view=setup&tab={tab}")
    ready = {"checks": "#checks .check", "config": "[data-robot='2026-comp']",
             "draft": "#draft-session, #draft-none"}  # fmt: skip
    page.wait_for_selector(ready[tab])
    return watch


def _slot_input(page: Any, slot: str, field: str) -> Any:
    return page.locator(f"#robot-2026-comp-slots tr[data-row='{slot}'] input[data-field='{field}']")


def test_missing_owlet_fix(page: Any, site: dict[str, Any]) -> None:
    watch = _setup(page, site["url"])
    row = page.locator("[data-check='owlet:C19']")
    assert row.get_attribute("data-status") == "warning"
    row.locator("[data-fix='owlet']").click()
    page.wait_for_selector("[data-check='owlet:C19'][data-status='ok']")
    assert "downloaded and verified" in page.inner_text("#msg-checks")
    assert page.locator("#writable").is_visible()
    assert watch.clean(), watch


def test_read_only_when_shared(page: Any, shared: dict[str, Any]) -> None:
    watch = _setup(page, shared["url"])
    assert page.locator("#read-only").is_visible()
    assert page.locator("[data-fix]").count() == 0
    assert page.locator("code[data-command='owlet:C19']").is_visible()
    page.click("#setup-tab-config")
    page.wait_for_selector("[data-robot='2026-comp']")
    assert page.locator("[id^='save-']").count() == 0
    assert page.locator("#robot-2026-comp-slots input").count() == 0  # values shown as text
    assert "drive-fl" in page.inner_text("#robot-2026-comp-slots")
    assert watch.clean(), watch


def test_valid_slot_edit(page: Any, site: dict[str, Any]) -> None:
    before = (site["root"] / ROBOT).read_text()
    watch = _setup(page, site["url"], "config")
    _slot_input(page, "drive-fl", "can_id").fill("19")
    page.click("#save-robot-2026-comp")
    page.wait_for_selector("#msg-robot-2026-comp[role='status']")
    message = page.inner_text("#msg-robot-2026-comp")
    assert "Saved robots/2026-comp.toml" in message and "flashpoint derive" in message
    after = (site["root"] / ROBOT).read_text()
    assert after == before.replace("can_id = 11\n", "can_id = 19\n", 1)
    assert len(list((site["root"] / ".backups" / ROBOT).iterdir())) == 1
    assert _slot_input(page, "drive-fl", "can_id").input_value() == "19"  # reloaded
    assert watch.clean(), watch


def test_duplicate_slot_refused(page: Any, site: dict[str, Any]) -> None:
    before = (site["root"] / ROBOT).read_bytes()
    watch = _setup(page, site["url"], "config")
    _slot_input(page, "steer-fl", "can_id").fill("11")
    page.click("#save-robot-2026-comp")
    page.wait_for_selector("#msg-robot-2026-comp[role='alert']")
    message = page.inner_text("#msg-robot-2026-comp")
    assert "drive-fl" in message and "steer-fl" in message and "Not saved" in message
    assert (site["root"] / ROBOT).read_bytes() == before
    assert _slot_input(page, "steer-fl", "can_id").input_value() == "11"  # the edit is kept
    # Chromium logs the refused request (400) itself; nothing else may appear.
    assert not watch.dialogs and not watch.remote
    assert all("status of 400" in e for e in watch.errors), watch


def test_keyboard_reaches_the_save(page: Any, site: dict[str, Any]) -> None:
    watch = _setup(page, site["url"], "config")
    page.focus("#setup-tab-config")
    for _ in range(600):  # past 2025-comp's tables, which come first
        page.keyboard.press("Tab")
        if page.evaluate("() => document.activeElement.id") == "save-robot-2026-comp":
            break
    else:
        pytest.fail("Tab never reached the robot's SAVE button")
    page.keyboard.press("Shift+Tab")
    page.keyboard.press("Tab")
    page.keyboard.press("Enter")  # saving unchanged content is allowed and kept as a backup
    page.wait_for_selector("#msg-robot-2026-comp[role='status']")
    assert watch.clean(), watch


def test_threshold_edit_names_its_command(page: Any, site: dict[str, Any]) -> None:
    watch = _setup(page, site["url"], "config")
    page.fill("[data-file='report.toml'] input[data-key='thresholds.temp_warn_c']", "60")
    page.click("#save-thr-report")
    page.wait_for_selector("#msg-thr-report[role='status']")
    assert "flashpoint report" in page.inner_text("#msg-thr-report")
    with (site["root"] / "report.toml").open("rb") as f:
        assert tomllib.load(f)["thresholds"]["temp_warn_c"] == 60
    assert watch.clean(), watch


@pytest.mark.corpus
def test_draft_from_q7_and_save(
    page: Any, derived_corpus_lake: LakePaths, tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:  # fmt: skip
    server, root = _site(tmp_path, derived_corpus_lake)
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    try:
        watch = _setup(page, url, "draft")
        q7 = page.locator("#draft-session option", has_text="2026gacmp_qm7").get_attribute("value")
        page.select_option("#draft-session", q7)
        page.click("#draft-go")
        page.wait_for_selector("#draft-slots tbody tr")
        assert "22 devices" in page.inner_text("#draft-summary")
        assert page.input_value("#draft-id") == "2026-practice"  # 2026-comp is taken
        page.click("#draft-slots tbody tr:first-child button[aria-label^='Remove']")
        assert page.locator("#draft-slots tbody tr").count() == 21
        page.click("#draft-save")
        page.wait_for_selector("#msg-draft[role='alert']")  # slots aren't named yet
        fill_all: Callable[[str], None] = lambda field: page.eval_on_selector_all(  # noqa: E731
            f"#draft-slots input[data-field='{field}']",
            "els => els.forEach(e => { e.value = 'x'; e.dispatchEvent(new Event('input')); })",
        )
        fill_all("subsystem")
        fill_all("role")
        page.click("#draft-save")
        page.wait_for_selector("#msg-draft[role='status']")
        assert "robots/2026-practice.toml" in page.inner_text("#msg-draft")
        assert watch.clean(), watch
    finally:
        _stop(server)
    robot = load_robot(root / "robots" / "2026-practice.toml", load_config_seasons(root))
    assert len(robot.slots) == 21 and robot.project == "Robot-2026"
    ctx = CheckContext(root, derived_corpus_lake, tmp_path / "cache", season=2026)
    check = next(r for r in run_checks(ctx) if r.id == "file:robots/2026-practice.toml")
    assert check.status == "ok"  # doctor loads it


def test_refused_threshold_keeps_the_edit(page: Any, site: dict[str, Any]) -> None:
    watch = _setup(page, site["url"], "config")
    field = "[data-file='report.toml'] input[data-key='thresholds.dropout_warn_s']"
    page.fill(field, "0.01")
    page.click("#save-thr-report")
    page.wait_for_selector("#msg-thr-report[role='alert']")
    assert "dropout_warn_s" in page.inner_text("#msg-thr-report")
    assert page.input_value(field) == "0.01"
    assert not (site["root"] / "report.toml").exists()  # still the bundled default
    assert not watch.dialogs and not watch.remote
    assert all("status of 400" in e for e in watch.errors), watch
