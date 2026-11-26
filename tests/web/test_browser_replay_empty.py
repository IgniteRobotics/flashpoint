"""Replay with no match to list says what to do (spec: match-reports, "Empty Replay explains
what to do")."""

from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.report.build import ReportBuilder
from flashpoint.report.export import export_static
from tests.report.lake import MatchLake, quiet_rows
from tests.web.test_browser import _open, _serve, _stop

pytestmark = pytest.mark.browser
PUBLISHED = "https://igniterobotics.github.io/flashpoint/"
GUIDE_TITLE = "User guide, “How matches are identified”"


def _quarantined(lake: MatchLake) -> None:
    lake.tables["files"].append(
        {"sha256": "e" * 64, "kind": "hoot", "size": 0, "stage": "quarantined"}
    )


def _practice(root: Path) -> MatchLake:
    lake = MatchLake(root)
    lake.add_match(None, "practice", quiet_rows())
    builder = ReportBuilder(lake.write_meta(), config.config_root())
    try:
        builder.build()
    finally:
        builder.close()
    return lake


@pytest.fixture
def no_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(tmp_path / "no-config"))


def _served(lake: LakePaths) -> tuple[Any, str]:
    server = _serve(lake)
    return server, f"http://127.0.0.1:{server.server_address[1]}/"


def _empty(page: Any, state: str) -> str:
    page.wait_for_selector(f"#replay-empty[data-state='{state}']")
    return str(page.inner_text("#replay-empty"))


def test_blank_lake(page: Any, tmp_path: Path, no_config: None) -> None:
    lake = LakePaths(tmp_path / "blank")
    server, url = _served(lake)
    try:
        watch = _open(page, url, "#view=replay")
        page.wait_for_selector("#replay-empty[data-state='no-sessions']")
        text = page.inner_text("#replay-empty")
        assert str(lake.root) in text and "flashpoint ingest" in text
        assert "flashpoint report" not in text
        page.wait_for_selector("#replay-empty-setup[data-problems]")
        setup = page.locator("#replay-empty-setup")
        problems = int(setup.get_attribute("data-problems") or "0")
        assert problems > 0 and f"{problems} problem" in setup.inner_text()
        assert setup.get_attribute("href") == "#view=setup"
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_practice_logs_only(page: Any, tmp_path: Path) -> None:
    lake = _practice(tmp_path / "lake")
    server, url = _served(lake.lake)
    try:
        watch = _open(page, url, "#view=replay")
        page.wait_for_selector("#replay-empty[data-state='no-match-key']")
        text = page.inner_text("#replay-empty")
        assert "1 session" in text and "competition match" in text
        assert "flashpoint report" not in text
        assert page.locator("#replay-empty-history").get_attribute("href") == "#view=history"
        page.wait_for_selector("#replay-empty-guide[href]")
        href = page.locator("#replay-empty-guide").get_attribute("href") or ""
        assert href.startswith(PUBLISHED) and href.endswith("#how-matches-are-identified")
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_keyed_but_not_built(page: Any, tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows())
    server, url = _served(lake.write_meta())  # no Replay data built
    try:
        watch = _open(page, url, "#view=replay")
        text = _empty(page, "not-built")
        assert "1 match is waiting" in text and "flashpoint report" in text
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_only_a_quarantined_file(page: Any, tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    _quarantined(lake)
    server, url = _served(lake.write_meta())
    try:
        watch = _open(page, url, "#view=replay")
        text = _empty(page, "no-sessions")
        assert "1 file was quarantined" in text and "flashpoint doctor" in text
        assert "flashpoint report" not in text
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_static_export_of_practice_logs(page: Any, tmp_path: Path) -> None:
    lake = _practice(tmp_path / "lake")
    export = tmp_path / "export"
    export_static(lake.lake, export)
    watch = _open(page, (export / "index.html").as_uri(), "#view=replay")
    page.wait_for_selector("#replay-empty[data-state='no-match-key']")
    guide = page.locator("#replay-empty-guide")
    assert guide.inner_text() == GUIDE_TITLE and guide.get_attribute("href") is None
    assert page.locator("#replay-empty-setup").count() == 0
    assert "flashpoint report" not in page.inner_text("#replay-empty")
    assert str(lake.root) not in page.inner_text("body")
    assert "STATIC EXPORT" in page.inner_text("header")
    assert watch.clean(), watch
    assert watch.remote == []


# --- a match with no robot links Draft robot (match-reports "Partial and unmatched") --------


def _no_robot_lake(root: Path) -> MatchLake:
    lake = MatchLake(root)
    lake.add_match("2026gacmp_qm7", "q7", None, robot="unknown")
    lake.tables["logs"][-3]["project"] = "Robot-2026"
    builder = ReportBuilder(lake.write_meta(), config.config_root())
    try:
        builder.build()
    finally:
        builder.close()
    return lake


def test_match_with_no_robot_links_draft_robot(page: Any, tmp_path: Path) -> None:
    lake = _no_robot_lake(tmp_path / "lake")
    server, url = _served(lake.lake)
    try:
        watch = _open(page, url, "#view=replay&m=2026gacmp_qm7")
        page.wait_for_selector("#replay-draft-robot")
        link = page.locator("#replay-draft-robot")
        assert link.get_attribute("href") == "#view=setup&tab=draft"
        assert "Robot-2026" in page.inner_text(".notice")
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_match_with_no_robot_in_a_static_export(page: Any, tmp_path: Path) -> None:
    lake = _no_robot_lake(tmp_path / "lake")
    export = tmp_path / "export"
    export_static(lake.lake, export)
    watch = _open(page, (export / "index.html").as_uri(), "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("#replay-draft-robot")
    assert page.locator("#replay-draft-robot").get_attribute("href") is None
    assert "Draft robot" in page.inner_text("#replay-draft-robot")
    assert watch.clean(), watch
