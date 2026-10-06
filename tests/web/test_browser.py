"""The app in Chromium, served and opened from a static export with the network blocked.

Specs: match-reports (shell, Replay, compare, links, escaping, offline) and lifetime-trends /
unit-odometry (History). Run with `pytest -m browser` after `playwright install chromium`.
"""

import shutil
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.raw import raw_path
from flashpoint.report.build import ReportBuilder
from flashpoint.report.export import export_static
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.advantagescope import Install, Launcher
from flashpoint.web.server import FlashpointServer
from tests.report.lake import MatchLake, quiet_rows
from tests.report.silver import points
from tests.views.season import HOSTILE_ROLE, build_season, match_key

pytestmark = pytest.mark.browser
playwright = pytest.importorskip("playwright.sync_api")

HOSTILE_LOG = "x');alert(1);//.wpilog"


@dataclass
class Watch:
    """Dialogs, console errors, page errors, and requests to anywhere but the app."""

    dialogs: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    remote: list[str] = field(default_factory=list)

    def clean(self) -> bool:
        return not (self.dialogs or self.errors or self.remote)


def _watch(page: Any) -> Watch:
    watch = Watch()

    def on_dialog(dialog: Any) -> None:
        watch.dialogs.append(dialog.message)
        dialog.dismiss()

    def route(r: Any) -> None:
        url = urlsplit(r.request.url)
        if url.scheme == "file" or url.hostname in ("127.0.0.1", "localhost"):
            r.continue_()
        else:
            watch.remote.append(r.request.url)
            r.abort()

    page.on("dialog", on_dialog)
    page.on("console", lambda m: watch.errors.append(m.text) if m.type == "error" else None)
    page.on("pageerror", lambda e: watch.errors.append(str(e)))
    page.route("**/*", route)
    return watch


def _serve(lake: LakePaths, robots: Any = None, launcher: Any = None) -> FlashpointServer:
    queries = HistoryQueries(lake, robots)
    server = FlashpointServer(("127.0.0.1", 0), lake, queries, launcher=launcher)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _stop(server: FlashpointServer) -> None:
    server.shutdown()
    server.server_close()
    server.queries.close()


# --- a Replay site: Q7 (hot hood, hostile names), E10 (no hood), Q8 (no temperature) ---------


@pytest.fixture(scope="module")
def replay_site(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("replay")
    config_dir = root / "config"
    shutil.copytree(config.config_root(), config_dir)
    robot = config_dir / "robots" / "2026-comp.toml"
    robot.write_text(robot.read_text().replace('role = "hood"', f'role = "{HOSTILE_ROLE}"'))
    lake = MatchLake(root / "lake")
    q7 = quiet_rows() + points("hood", "temp_c", [(118.0, 66.0)])
    lake.add_match("2026gacmp_qm7", "q7", q7, wpilog_name=HOSTILE_LOG)
    e10 = [r for r in quiet_rows() if r["slot_id"] != "hood"]
    lake.add_match("2026gacmp_e10", "e10", e10, start_utc="2026-04-11T19:25:48+00:00")
    no_temp = [r for r in quiet_rows() if r["metric"] != "temp_c"]
    lake.add_match("2026gacmp_qm8", "q8", no_temp, pro_licensed=0)
    builder = ReportBuilder(lake.write_meta(), config_dir)
    builder.build()
    builder.close()
    export = root / "export"
    export_static(lake.lake, export)
    server = _serve(lake.lake)
    port = server.server_address[1]
    yield {
        "served": f"http://127.0.0.1:{port}/",
        "static": (export / "index.html").as_uri(),
        "export": export,
    }
    _stop(server)


@pytest.fixture(params=["served", "static"])
def app(request: pytest.FixtureRequest, replay_site: dict[str, Any]) -> str:
    return str(replay_site[request.param])


def _open(page: Any, url: str, fragment: str = "") -> Watch:
    watch = _watch(page)
    page.goto(url + fragment)
    page.wait_for_selector(".app-header")
    return watch


def _readout_slots(page: Any) -> list[str]:
    slots = page.eval_on_selector_all("#readout tr", "rows => rows.map(r => r.dataset.slot)")
    return [str(s) for s in slots]


# --- shell --------------------------------------------------------------------------------

TEST_VIEW = """
document.addEventListener('DOMContentLoaded', () => {
  FP.view({ id: 'test', label: 'Test', needsApi: false, mount(el) {
    FP.fill(el, FP.h('section', { class: 'panel' },
      FP.h('h2', { class: 'h-section', id: 'probe' }, 'Probe'),
      FP.h('button', { type: 'button', class: 'btn', id: 'probe-btn' }, 'PRESS')));
  } });
});
"""


def test_registered_view_renders_in_the_shell(page: Any, app: str) -> None:
    page.add_init_script(TEST_VIEW)
    watch = _open(page, app, "#view=test")
    page.wait_for_selector("#probe")
    tabs = page.inner_text("nav.tabs").split()
    assert tabs[0] == "REPLAY" and tabs[-1] == "TEST"
    style = page.evaluate(
        """async () => { await document.fonts.ready;
        const s = getComputedStyle(document.getElementById('probe'));
        return { font: s.fontFamily, color: s.color,
                 plex: document.fonts.check('13px "IBM Plex Mono"'),
                 sheets: [...document.styleSheets].map(x => x.href.split('/').pop()) }; }"""
    )
    assert "IBM Plex Mono" in style["font"] and style["plex"] is True
    assert style["color"] == "rgb(255, 176, 0)"
    assert style["sheets"] == ["uPlot.min.css", "tokens.css", "shell.css"]
    page.focus("#probe-btn")
    page.keyboard.press("Shift+Tab")
    page.keyboard.press("Tab")
    outline = page.evaluate(
        "() => { const s = getComputedStyle(document.activeElement);"
        " return [document.activeElement.id, s.outlineStyle, s.outlineColor]; }"
    )
    assert outline == ["probe-btn", "solid", "rgb(255, 210, 122)"]
    assert watch.clean(), watch


def test_history_in_nav_only_when_served(page: Any, replay_site: dict[str, Any]) -> None:
    _open(page, replay_site["served"])
    assert page.inner_text("nav.tabs").split() == ["REPLAY", "HISTORY"]
    _open(page, replay_site["static"])
    assert page.inner_text("nav.tabs").split() == ["REPLAY"]


# --- Replay -------------------------------------------------------------------------------


def test_hottest_motor_in_two_clicks(page: Any, app: str) -> None:
    watch = _open(page, app)
    page.click("a.match-btn[data-match='2026gacmp_qm7']")
    page.wait_for_selector("#readout tr")
    assert page.inner_text("#readout-label") == "MATCH MAXIMA · HOTTEST FIRST"
    assert _readout_slots(page) == ["hood", "intake-roller", "drive-fl"]
    first = page.inner_text("#readout tr:first-child")
    assert "66" in first and "WARN" in first
    assert watch.clean(), watch


def test_scrub_and_keyboard(page: Any, app: str) -> None:
    watch = _open(page, app, "#view=replay&m=2026gacmp_qm7&t=90")
    page.wait_for_selector("#readout tr")
    assert page.inner_text("#scrub-label") == "T+90.00 s"
    assert page.inner_text("#readout-label").startswith("AT T+90.0 S")
    nows = page.eval_on_selector_all("[data-now]", "els => els.map(e => e.textContent)")
    assert len(nows) == 4 and all(n not in ("—", "") for n in nows)
    # T+90 is silver T=100 s: intake-roller draws 12 A and its temperature steps to 45 °C
    row = page.inner_text("#readout tr[data-slot='intake-roller']")
    assert "12.0" in row and "45" in row
    page.focus("#scrub")
    page.keyboard.press("ArrowRight")  # one bucket (0.17 s) on the slider's grid
    stepped = float(page.inner_text("#scrub-label").removeprefix("T+").removesuffix(" s"))
    assert 90 < stepped <= 90.17
    assert f"t={stepped:.2f}" in page.url
    plot = page.locator(".track__plot").first.bounding_box()
    page.mouse.click(plot["x"] + plot["width"] * 0.9, plot["y"] + plot["height"] / 2)
    assert float(page.inner_text("#scrub-label")[2:-2]) > 140  # clicked near the end
    page.click("#clear-cursor")
    assert page.inner_text("#readout-label") == "MATCH MAXIMA · HOTTEST FIRST"
    assert watch.clean(), watch


def test_marker_select(page: Any, app: str) -> None:
    watch = _open(page, app, "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("[data-marker]")
    marker = page.locator("[data-marker]").first
    assert marker.get_attribute("aria-label") == "WARN hood at T+108.0 s: Temperature 66 °C"
    marker.focus()
    page.keyboard.press("Enter")
    assert "Temperature 66 °C" in page.inner_text("#event")
    assert page.inner_text("#scrub-label") == "T+108.00 s"
    assert marker.get_attribute("aria-pressed") == "true"
    assert watch.clean(), watch


def test_compare_overlay_and_absent_slots(page: Any, app: str) -> None:
    watch = _open(page, app, "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("#compare")
    page.select_option("#compare", "2026gacmp_e10")
    page.wait_for_selector(".readout-table th:has-text('E10 A')")
    header = page.inner_text(".readout-table thead")
    assert "E10 A" in header and "E10 °C" in header
    assert page.inner_text("#readout tr[data-slot='hood']").count("absent") == 2
    assert "absent" not in page.inner_text("#readout tr[data-slot='drive-fl']")
    series = page.evaluate("() => [...document.querySelectorAll('.track__plot')].length")
    assert series == 4 and "o=2026gacmp_e10" in page.url
    assert watch.clean(), watch


def test_link_restores_the_view(page: Any, app: str, browser: Any) -> None:
    watch = _open(page, app, "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("#compare")
    page.select_option("#compare", "2026gacmp_e10")
    page.wait_for_selector(".readout-table th:has-text('E10 A')")
    page.focus("#scrub")
    page.keyboard.press("End")
    copied = page.url
    other = browser.new_page()
    watch2 = _open(other, copied)
    other.wait_for_selector("#readout tr")
    assert other.input_value("#compare") == "2026gacmp_e10"
    assert other.inner_text("#scrub-label") == page.inner_text("#scrub-label")
    assert other.inner_text("h1.title-d3") == "Q7"
    other.close()
    assert watch.clean() and watch2.clean(), (watch, watch2)


def test_temperature_not_logged(page: Any, app: str) -> None:
    watch = _open(page, app, "#view=replay&m=2026gacmp_qm8")
    page.wait_for_selector("#readout tr")
    assert "not Pro-licensed" in page.inner_text(".notes")
    for slot in ("drive-fl", "hood", "intake-roller"):
        assert "not logged" in page.inner_text(f"#readout tr[data-slot='{slot}']")
    assert "not logged" in page.inner_text(".track:nth-child(3) .track__now")
    assert watch.clean(), watch


def test_hostile_names_shown_literally(page: Any, app: str) -> None:
    watch = _open(page, app, "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("#readout tr")
    assert HOSTILE_ROLE in page.inner_text("#readout tr[data-slot='hood']")
    assert HOSTILE_LOG in page.inner_text(".match-meta")
    assert HOSTILE_LOG in page.inner_text(".downloads")
    assert page.locator("img").count() == 0
    assert watch.clean(), watch


def test_downloads(page: Any, replay_site: dict[str, Any]) -> None:
    _open(page, replay_site["served"], "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("a.download")
    hrefs = page.eval_on_selector_all(
        "a.download", "as => as.map(a => [a.getAttribute('href'), a.download])"
    )
    assert all(h.startswith("raw/") and d.startswith("2026gacmp_qm7__") for h, d in hrefs)
    with page.expect_download() as info:
        page.click("a.download >> nth=0")
    download = info.value
    assert download.suggested_filename == "2026gacmp_qm7__wpilog__x_alert_1_.wpilog"
    assert Path(download.path()).read_bytes() == b"wpilog q7"
    _open(page, replay_site["static"], "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("a.download")
    first = page.get_attribute("a.download >> nth=0", "href")
    assert first == "raw/2026gacmp_qm7__wpilog__x_alert_1_.wpilog"
    assert (replay_site["export"] / first).read_bytes() == b"wpilog q7"


def test_static_export_without_raw(page: Any, replay_site: dict[str, Any], tmp_path: Path) -> None:
    lake = LakePaths(Path(replay_site["export"]).parent / "lake")
    out = tmp_path / "noraw"
    export_static(lake, out, include_raw=False)
    watch = _open(page, (out / "index.html").as_uri(), "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector(".downloads")
    text = page.inner_text(".downloads")
    assert "not included" in text and "sha256" in text
    page.wait_for_selector("#readout tr")
    assert watch.clean(), watch


def test_csp_holds_on_file(page: Any, replay_site: dict[str, Any]) -> None:
    """Recorded: Chromium accepts script-src 'self' on file:// (the app's scripts load) and
    still enforces the policy (an injected inline script is refused), so no fallback needed."""
    watch = _open(page, replay_site["static"], "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("#readout tr")
    ran = page.evaluate(
        "() => { const s = document.createElement('script'); s.textContent = 'window.__ran = 1';"
        " document.head.appendChild(s); return Boolean(window.__ran); }"
    )
    assert ran is False
    refused = [e for e in watch.errors if "Content Security Policy" in e]
    assert len(refused) == 1 and not watch.dialogs and not watch.remote


# --- History ------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def history_site(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    season = build_season(tmp_path_factory.mktemp("history") / "lake", hostile=True)
    server = _serve(season.lake, season.robots)
    yield f"http://127.0.0.1:{server.server_address[1]}/"
    _stop(server)


def test_history_filters_and_tiles(page: Any, history_site: str) -> None:
    watch = _open(page, history_site, "#view=history")
    page.wait_for_selector("#rows tr")
    assert page.inner_text("#tiles").split()[:4] == ["UNITS", "TRACKED", "26", "MATCHES"]
    page.select_option("#f-event", "2026gadal")
    page.wait_for_function("() => document.querySelector('#tiles').innerText.includes('30')")
    assert "event=2026gadal" in page.url
    assert page.evaluate("() => FP.historyChart().matches.length") == 30
    assert watch.clean(), watch


def test_unit_line_has_a_gap(page: Any, history_site: str) -> None:
    watch = _open(page, history_site, "#view=history&unit=ctre%3AHOOD-G")
    page.wait_for_selector("#device .lifeline")
    chart = page.evaluate("() => FP.historyChart()")
    values = chart["series"]["ctre:HOOD-G"]
    assert [i for i, v in enumerate(values) if v is None] == list(range(10, 21))
    assert chart["series"]["ctre:HOOD-T"][15] is not None
    assert watch.clean(), watch


def test_device_lifeline_order(page: Any, history_site: str) -> None:
    watch = _open(page, history_site, "#view=history")
    page.wait_for_selector("#rows tr")
    page.click("button.dev-btn[data-unit='ctre:MOVER']")
    page.wait_for_selector("#device .lifeline__item")
    events = page.eval_on_selector_all(
        "#device .lifeline__text", "els => els.map(e => e.textContent.split(' · ')[0])"
    )
    assert events == ["Last seen", "Moved", "First seen"]
    assert "2026-practice" in page.inner_text("#device .lifeline__item:last-child")
    assert "unit=ctre%3AMOVER" in page.url
    assert watch.clean(), watch


def test_unknown_unit_suggestions(page: Any, history_site: str) -> None:
    _open(page, history_site, "#view=history&unit=ctre%3AROLLER-Z")
    page.wait_for_selector(".suggestions")
    assert "ctre:ROLLER-A" in page.inner_text(".suggestions")


def test_drill_through_to_replay_not_built(page: Any, history_site: str) -> None:
    watch = _open(page, history_site, "#view=history&unit=ctre%3ADRIVE-FL")
    page.wait_for_selector("#unit-matches a")
    page.click("#unit-matches a >> nth=0")
    page.wait_for_selector("text=not built")
    assert f"m={match_key(59)}" in page.url and "track=drive-fl" in page.url
    assert f"flashpoint report --match {match_key(59)}" in page.inner_text("main")
    page.wait_for_selector("a.download")
    assert page.locator("a.download").count() == 1  # the season's synthetic wpilog only
    assert watch.clean(), watch


def test_hostile_role_in_history(page: Any, history_site: str) -> None:
    watch = _open(page, history_site, "#view=history&slot=hood")
    page.wait_for_selector("#rows tr")
    assert HOSTILE_ROLE in page.inner_text("#rows")
    assert page.locator("img").count() == 0
    assert watch.clean(), watch


def test_history_empty_lake(page: Any, tmp_path: Path) -> None:
    lake = LakePaths(tmp_path / "empty")
    server = _serve(lake)
    try:
        _open(page, f"http://127.0.0.1:{server.server_address[1]}/", "#view=history")
        page.wait_for_selector(".notice__title")
        assert page.inner_text(".notice__title") == "No matches ingested"
        assert str(lake.root) in page.inner_text(".notice")
    finally:
        _stop(server)


@pytest.mark.corpus
def test_corpus_lake_served(page: Any, derived_corpus_lake: Any) -> None:
    builder = ReportBuilder(derived_corpus_lake, config.config_root())
    builder.build()
    builder.close()
    from flashpoint.semantics.robot_config import load_robots

    server = _serve(derived_corpus_lake, load_robots(config.config_root() / "robots"))
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/"
        watch = _open(page, url)
        page.click("a.match-btn[data-match='2026gacmp_qm7']")
        page.wait_for_selector("#readout tr")
        assert _readout_slots(page)[0] == "intake-extension"  # 46 °C, the hottest in Q7
        _open(page, url, "#view=history&unit=legacy%3A2026-comp%3Aintake-extension%3A0")
        page.wait_for_selector("#device .lifeline__item")
        assert "2026gacmp_qm7" in page.inner_text("#device")
        assert watch.clean(), watch
    finally:
        _stop(server)


# --- Open in AdvantageScope (spec: advantagescope-launch) ---------------------------------


class Recorder:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> None:
        self.calls.append(argv)


class NetworkServer(FlashpointServer):
    """Reports itself as shared on the pit network (no all-interface bind in tests)."""

    @property
    def bound_local(self) -> bool:
        return False


@pytest.fixture(scope="module")
def launch_site(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("launch")
    lake = MatchLake(root / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows(), wpilog_name=HOSTILE_LOG)
    builder = ReportBuilder(lake.write_meta(), config.config_root())
    builder.build()
    builder.close()
    app_path = root / "AdvantageScope stand-in"
    app_path.write_text("")
    recorder = Recorder()
    found = _serve(lake.lake, launcher=Launcher(Install(app_path, "config"), root=root / "stage",
                                                spawn=recorder))  # fmt: skip
    missing = _serve(lake.lake)
    network = NetworkServer(("127.0.0.1", 0), lake.lake, HistoryQueries(lake.lake),
                            launcher=Launcher(Install(app_path, "config"), root=root / "stage",
                                              spawn=recorder))  # fmt: skip
    threading.Thread(target=network.serve_forever, daemon=True).start()
    export = root / "export"
    export_static(lake.lake, export)
    yield {
        "found": f"http://127.0.0.1:{found.server_address[1]}/",
        "missing": f"http://127.0.0.1:{missing.server_address[1]}/",
        "network": f"http://127.0.0.1:{network.server_address[1]}/",
        "static": (export / "index.html").as_uri(),
        "recorder": recorder,
        "stage": root / "stage",
    }
    for server in (found, missing, network):
        _stop(server)


def test_launch_from_replay(page: Any, launch_site: dict[str, Any]) -> None:
    recorder = launch_site["recorder"]
    recorder.calls.clear()
    watch = _open(page, launch_site["found"], "#view=replay&m=2026gacmp_qm7")
    page.click("#as-launch")
    page.wait_for_selector("#as-hoots li")
    folder = launch_site["stage"] / "2026gacmp_qm7"
    assert page.inner_text("#as-folder") == str(folder)
    assert "Insert log" in page.inner_text("#as-result")
    assert page.locator("#as-hoots li").count() == 2
    assert len(recorder.calls) == 1
    assert recorder.calls[0][-1] == str(folder / "2026gacmp_qm7__wpilog__x_alert_1_.wpilog")
    assert page.locator("a.download").count() == 3  # the downloads stay
    assert watch.clean(), watch


def test_launch_unavailable_on_the_network(page: Any, launch_site: dict[str, Any]) -> None:
    recorder = launch_site["recorder"]
    recorder.calls.clear()
    watch = _open(page, launch_site["network"], "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("#as-unavailable")
    assert "only available on the machine running Flashpoint" in page.inner_text("#as-unavailable")
    assert page.locator("#as-launch").count() == 0
    assert page.locator("a.download").count() == 3
    assert recorder.calls == [] and watch.clean(), watch


def test_launch_unavailable_without_advantagescope(page: Any, launch_site: dict[str, Any]) -> None:
    watch = _open(page, launch_site["missing"], "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("#as-unavailable")
    assert "AdvantageScope not found" in page.inner_text("#as-unavailable")
    assert page.locator("#as-launch").count() == 0
    assert page.locator("a.download").count() == 3
    assert watch.clean(), watch


def test_no_launch_in_a_static_export(page: Any, launch_site: dict[str, Any]) -> None:
    watch = _open(page, launch_site["static"], "#view=replay&m=2026gacmp_qm7")
    page.wait_for_selector("a.download")
    assert page.locator("#as-launch, #as-unavailable").count() == 0
    assert watch.clean(), watch


def test_launch_from_history(page: Any, tmp_path: Path) -> None:
    season = build_season(tmp_path / "lake")
    wpilog = raw_path(season.lake, "ws059", ".wpilog")  # the season's ids, no raw files by default
    wpilog.parent.mkdir(parents=True)
    wpilog.write_bytes(b"wpilog 59")
    recorder = Recorder()
    app_path = tmp_path / "AdvantageScope stand-in"
    app_path.write_text("")
    launcher = Launcher(Install(app_path, "config"), root=tmp_path / "stage", spawn=recorder)
    server = _serve(season.lake, season.robots, launcher)
    try:
        watch = _open(page, f"http://127.0.0.1:{server.server_address[1]}/",
                      "#view=history&unit=ctre%3ADRIVE-FL")  # fmt: skip
        page.wait_for_selector("#unit-matches button")
        page.click("#unit-matches button >> nth=0")
        page.click("#unit-logs #as-launch")
        page.wait_for_selector("#as-result >> text=/Opened|did not open/")
        assert len(recorder.calls) == 1, page.inner_text("#as-result")
        assert recorder.calls[0][-1].endswith(".wpilog")
        assert page.locator("#unit-logs a.download").count() >= 1
        assert watch.clean(), watch
    finally:
        _stop(server)
