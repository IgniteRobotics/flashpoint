"""The app in Chromium, served and opened from a static export with the network blocked.

Specs: match-reports (shell, Replay, compare, links, escaping, offline) and lifetime-trends /
unit-odometry (History). Run with `pytest -m browser` after `playwright install chromium`.
"""

import json
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
from tests.views.season import HOSTILE_ROLE, build_replay_season, build_season, match_key

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


# --- FP.track: x range, shift-drag select, ctrl+wheel zoom (spec: Timeline zoom) ------------

TRACK_VIEW = """
document.addEventListener('DOMContentLoaded', () => {
  FP.view({ id: 'tracktest', label: 'Track', needsApi: false, mount(el) {
    const x = []; const v = [];
    for (let i = 0; i <= 1000; i += 1) {
      x.push(i / 10); v.push(i < 500 ? 1 + (i % 2) : 10 + (i % 2));
    }
    const plot = FP.h('div', { id: 'plot', style: { width: '800px' } });
    FP.fill(el, plot, FP.h('div', { style: { height: '3000px' } }));
    window.__ev = { pick: [], select: [], zoom: [] };
    window.__track = FP.track(plot, { x, series: [{ label: 'v', values: v }],
      onPick: (t) => window.__ev.pick.push(t),
      onSelect: (a, b) => window.__ev.select.push([a, b]),
      onZoom: (f, t) => window.__ev.zoom.push([f, t]) });
  } });
});
"""


@pytest.fixture
def track_page(page: Any, replay_site: dict[str, Any]) -> tuple[Any, Watch, dict[str, float]]:
    page.add_init_script(TRACK_VIEW)
    watch = _open(page, replay_site["served"], "#view=tracktest")
    page.wait_for_function("() => window.__track")
    box = page.evaluate(
        "() => { const r = __track.u.over.getBoundingClientRect();"
        " return { x: r.left, y: r.top, w: r.width, h: r.height }; }"
    )
    return page, watch, box


def _x_at(page: Any, box: dict[str, float], frac: float) -> float:
    return float(page.evaluate(f"() => __track.u.posToVal({box['w'] * frac}, 'x')"))


def test_track_shift_drag_selects(track_page: Any) -> None:
    page, watch, box = track_page
    y = box["y"] + box["h"] / 2
    page.keyboard.down("Shift")
    page.mouse.move(box["x"] + box["w"] * 0.2, y)
    page.mouse.down()
    page.mouse.move(box["x"] + box["w"] * 0.4, y)
    band = page.evaluate("() => __track.selecting()")
    page.mouse.move(box["x"] + box["w"] * 0.6, y)
    page.mouse.up()
    page.keyboard.up("Shift")
    events = page.evaluate("() => __ev")
    assert band is not None and events["pick"] == []
    ((a, b),) = events["select"]
    assert a == pytest.approx(_x_at(page, box, 0.2), abs=0.2)
    assert b == pytest.approx(_x_at(page, box, 0.6), abs=0.2)
    assert page.evaluate("() => __track.selecting()") is None
    assert watch.clean(), watch


def test_track_plain_drag_picks(track_page: Any) -> None:
    page, watch, box = track_page
    y = box["y"] + box["h"] / 2
    before = page.evaluate("() => [__track.u.scales.x.min, __track.u.scales.x.max]")
    page.mouse.move(box["x"] + box["w"] * 0.2, y)
    page.mouse.down()
    page.mouse.move(box["x"] + box["w"] * 0.5, y)
    page.mouse.up()
    events = page.evaluate("() => __ev")
    assert events["select"] == [] and len(events["pick"]) >= 2
    assert events["pick"][-1] == pytest.approx(_x_at(page, box, 0.5), abs=0.2)
    assert page.evaluate("() => [__track.u.scales.x.min, __track.u.scales.x.max]") == before
    assert watch.clean(), watch


def test_track_ctrl_wheel_zooms_plain_wheel_scrolls(track_page: Any) -> None:
    page, watch, box = track_page
    page.mouse.move(box["x"] + box["w"] * 0.25, box["y"] + box["h"] / 2)
    page.keyboard.down("Control")
    page.mouse.wheel(0, -100)
    page.keyboard.up("Control")
    page.wait_for_function("() => __ev.zoom.length === 1")
    ((factor, at),) = page.evaluate("() => __ev.zoom")
    assert factor > 1 and at == pytest.approx(_x_at(page, box, 0.25), abs=0.2)
    assert page.evaluate("() => window.scrollY") == 0
    page.mouse.wheel(0, 200)
    page.wait_for_function("() => window.scrollY > 0")
    assert len(page.evaluate("() => __ev.zoom")) == 1
    assert watch.clean(), watch


def test_track_set_x_reranges_y(track_page: Any) -> None:
    page, watch, _ = track_page
    full = page.evaluate("() => __track.u.scales.y.max")
    page.evaluate("() => __track.setX(10, 40)")
    scales = page.evaluate(
        "() => [__track.u.scales.x.min, __track.u.scales.x.max, __track.u.scales.y.max]"
    )
    assert scales[:2] == [10, 40]
    assert full > 10 and scales[2] < 5  # only the 1-2 half is in view
    assert watch.clean(), watch


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


# --- Replay match list filters (spec: Match list filters) ---------------------------------


@pytest.fixture(scope="module")
def season_site(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    root = tmp_path_factory.mktemp("replay-season")
    lake = build_replay_season(root / "lake")
    export = root / "export"
    export_static(lake, export)
    server = _serve(lake)
    yield {
        "served": f"http://127.0.0.1:{server.server_address[1]}/",
        "static": (export / "index.html").as_uri(),
        "lake": lake,
    }
    _stop(server)


@pytest.fixture(params=["served", "static"])
def season_app(request: pytest.FixtureRequest, season_site: dict[str, Any]) -> str:
    return str(season_site[request.param])


def _listed(page: Any) -> list[str]:
    keys = page.eval_on_selector_all("nav.rail-l a.match-btn", "as => as.map(a => a.dataset.match)")
    return sorted(str(k) for k in keys)


def _filters(page: Any) -> tuple[str, str, str]:
    return (
        page.input_value("#f-season"),
        page.input_value("#f-robot"),
        page.input_value("#f-event"),
    )


def _options(page: Any, select: str) -> list[str]:
    return [
        str(v) for v in page.eval_on_selector_all(f"{select} option", "os => os.map(o => o.value)")
    ]


def test_filters_default_to_most_recent_event(page: Any, season_app: str) -> None:
    watch = _open(page, season_app, "#view=replay")
    page.wait_for_selector("#f-event")
    assert _filters(page) == ("2026", "all", "2026gacmp")
    assert _listed(page) == ["2026gacmp_qm7", "2026gacmp_qm8"]
    assert _options(page, "#f-season") == ["all", "2026", "2025"]
    assert "event=2026gacmp" in page.url and "season=2026" in page.url
    assert watch.clean(), watch


def test_filters_combine_and_season_narrows_events(page: Any, season_app: str) -> None:
    watch = _open(page, season_app, "#view=replay")
    page.wait_for_selector("#f-event")
    page.select_option("#f-event", "all")
    page.select_option("#f-robot", "practice")
    assert _listed(page) == ["2026gadal_qm2"]
    assert _options(page, "#f-event") == ["all", "2026gacmp", "2026gadal"]
    page.select_option("#f-season", "all")
    assert _listed(page) == ["2025gaalb_qm2", "2026gadal_qm2"]
    assert _options(page, "#f-event") == ["all", "2026gacmp", "2026gadal", "2025gaalb"]
    assert "robot=practice" in page.url and "season=all" in page.url
    assert watch.clean(), watch


def test_filters_nothing_matches_and_clear(page: Any, season_app: str) -> None:
    watch = _open(page, season_app, "#view=replay")
    page.wait_for_selector("#f-event")
    page.select_option("#f-robot", "practice")  # practice played no 2026gacmp match
    assert _listed(page) == []
    assert "No matches for these filters" in page.inner_text("#filter-note")
    page.click("#filters-clear")
    assert _filters(page) == ("all", "all", "all") and len(_listed(page)) == 7
    assert watch.clean(), watch


def test_filters_hide_the_selected_match(page: Any, season_app: str) -> None:
    watch = _open(page, season_app, "#view=replay&m=2026gacmp_qm7&event=2026gadal")
    page.wait_for_selector("#readout tr")
    assert page.inner_text("h1.title-d3") == "Q7"
    assert _listed(page) == ["2026gadal_qm1", "2026gadal_qm2", "2026gadal_qm3"]
    assert "Q7 is hidden by the filters" in page.inner_text("#filter-note")
    assert watch.clean(), watch


def test_filters_selected_match_sets_the_default_event(page: Any, season_app: str) -> None:
    watch = _open(page, season_app, "#view=replay&m=2026gadal_qm1")
    page.wait_for_selector("#readout tr")
    assert _filters(page) == ("2026", "all", "2026gadal")
    assert watch.clean(), watch


def test_filters_unknown_value_named(page: Any, season_app: str) -> None:
    watch = _open(page, season_app, "#view=replay&event=2019zzzz")
    page.wait_for_selector("#f-event")
    assert _listed(page) == [] and page.input_value("#f-event") == "2019zzzz"
    note = page.inner_text("#filter-note")
    assert "No matches for these filters" in note and "2019zzzz" in note
    page.click("#filters-clear")
    assert len(_listed(page)) == 7
    assert watch.clean(), watch


def test_filters_old_index_disables_season(page: Any, season_site: dict[str, Any]) -> None:
    def old_index(route: Any) -> None:
        body = route.fetch().text()
        payload = json.loads(body[len("FP.index(") : -3])
        payload["version"] = 1
        for entry in payload["matches"]:
            del entry["season"]
        route.fulfill(body="FP.index(" + json.dumps(payload) + ");\n",
                      content_type="text/javascript")  # fmt: skip

    watch = _watch(page)
    page.route("**/data/matches.js*", old_index)  # registered after the watch, so it wins
    page.goto(season_site["served"] + "#view=replay")
    page.wait_for_selector("#f-event")
    assert page.is_disabled("#f-season") and page.input_value("#f-season") == "all"
    assert "Rebuild with flashpoint report to filter by season" in page.inner_text("nav.rail-l")
    assert _listed(page) == ["2026gacmp_qm7", "2026gacmp_qm8"]
    page.select_option("#f-event", "all")
    page.select_option("#f-robot", "practice")
    assert _listed(page) == ["2025gaalb_qm2", "2026gadal_qm2"]
    assert watch.clean(), watch


def test_filters_from_a_history_link(page: Any, season_app: str) -> None:
    watch = _open(
        page, season_app, "#view=replay&m=2026gadal_qm1&season=2026&robot=comp&track=hood"
    )
    page.wait_for_selector("#readout tr")
    assert _filters(page) == ("2026", "comp", "all")
    assert _listed(page) == ["2026gacmp_qm7", "2026gacmp_qm8", "2026gadal_qm1", "2026gadal_qm3"]
    assert watch.clean(), watch


def test_static_export_filters_offline(page: Any, tmp_path: Path) -> None:
    lake = build_replay_season(tmp_path / "lake", events=("2026gadal", "2026gacmp"))
    export_static(lake, tmp_path / "export")
    page.context.set_offline(True)
    watch = _open(page, (tmp_path / "export" / "index.html").as_uri(), "#view=replay")
    page.wait_for_selector("#readout tr")
    assert _filters(page) == ("2026", "all", "2026gacmp")
    requests: list[str] = []
    page.on("request", lambda r: requests.append(r.url))
    page.select_option("#f-event", "2026gadal")  # the older event
    assert _listed(page) == ["2026gadal_qm1", "2026gadal_qm2", "2026gadal_qm3"]
    assert requests == [] and watch.clean(), (requests, watch)


def test_filters_keyboard_reaches_every_match(page: Any, season_site: dict[str, Any]) -> None:
    watch = _open(page, season_site["served"], "#view=replay&season=all&robot=all&event=all")
    page.wait_for_selector("#f-season")
    page.focus("#f-season")
    seen: set[str] = set()
    ring: set[str] = set()
    for _ in range(20):
        page.keyboard.press("Tab")
        found = page.evaluate(
            "() => { const a = document.activeElement; const s = getComputedStyle(a);"
            " return [a.closest('nav.rail-l') !== null, a.dataset.match || null, s.outlineStyle]; }"
        )
        if found[0]:
            ring.add(found[2])
        if found[1]:
            seen.add(found[1])
    assert len(seen) == 7 and ring == {"solid"}
    assert watch.clean(), watch


# --- Replay timeline zoom (spec: Timeline zoom) -------------------------------------------

FULL = (-2.0, 167.15)  # the zoom Q7 window: 165 s plus 2 s each side, 995 buckets of 170 ms
FULL_SPAN = FULL[1] - FULL[0]


def _drag(page: Any, a: float, b: float, track: int = 0) -> None:
    x0, x1, y = page.evaluate(
        "([a, b, i]) => { const u = FP.replay.charts()[i];"
        " const r = u.over.getBoundingClientRect();"
        " return [r.left + u.valToPos(a, 'x'), r.left + u.valToPos(b, 'x'),"
        " r.top + r.height / 2]; }",
        [a, b, track],
    )
    page.keyboard.down("Shift")
    page.mouse.move(x0, y)
    page.mouse.down()
    page.mouse.move(x1, y, steps=4)
    page.mouse.up()
    page.keyboard.up("Shift")


def _view(page: Any) -> tuple[float, float]:
    view = page.evaluate("() => FP.replay.view()")
    return float(view["from"]), float(view["to"])


def _scales(page: Any) -> list[tuple[float, float]]:
    scales = page.evaluate("() => FP.replay.charts().map(u => [u.scales.x.min, u.scales.x.max])")
    return [(float(a), float(b)) for a, b in scales]


def _open_q7(page: Any, url: str, extra: str = "") -> Watch:
    watch = _open(page, url, "#view=replay&m=2026gacmp_qm7" + extra)
    page.wait_for_selector("#readout tr")
    page.wait_for_function("() => FP.replay.charts().length === 4")
    return watch


def test_zoom_to_a_brownout(page: Any, season_app: str) -> None:
    watch = _open_q7(page, season_app)
    _drag(page, 95.0, 99.0, track=1)
    assert page.inner_text("#view-window") == "T+95.0 – T+99.0 s"
    for lo, hi in _scales(page):
        assert lo == pytest.approx(95.0, abs=0.05) and hi == pytest.approx(99.0, abs=0.05)
    brownout = page.locator("[data-marker]:has-text('✕')")
    assert brownout.is_visible()
    centre = page.evaluate(
        "(b) => { const r = b.getBoundingClientRect(); return r.left + r.width / 2; }",
        brownout.element_handle(),
    )
    under = page.evaluate(
        "() => FP.replay.charts()"
        ".map(u => u.over.getBoundingClientRect().left + u.valToPos(97, 'x'))"
    )
    assert all(abs(x - centre) < 1.5 for x in under), (centre, under)
    assert page.inner_text("#markers-left").startswith("◂ 1")
    assert page.inner_text("#markers-right").startswith("1 ▸")
    assert page.locator("[data-marker]:visible").count() == 1
    assert watch.clean(), watch


def test_zoom_buttons_keys_and_reset(page: Any, season_app: str) -> None:
    watch = _open_q7(page, season_app, "&t=90")
    page.click("#zoom-in")
    lo, hi = _view(page)
    assert hi - lo == pytest.approx(FULL_SPAN / 2) and lo < 90 < hi
    page.click("#zoom-in")
    lo, hi = _view(page)
    assert hi - lo == pytest.approx(FULL_SPAN / 4) and lo < 90 < hi
    page.click("#zoom-reset")
    assert _view(page) == pytest.approx(FULL)
    page.focus("#scrub")
    page.keyboard.press("+")
    page.keyboard.press("+")
    assert _view(page)[1] - _view(page)[0] == pytest.approx(FULL_SPAN / 4)
    page.keyboard.press("-")
    assert _view(page)[1] - _view(page)[0] == pytest.approx(FULL_SPAN / 2)
    page.keyboard.press("0")
    assert _view(page) == pytest.approx(FULL)
    assert page.inner_text("#scrub-label") == "T+90.00 s"  # zoom keys never move the cursor
    assert watch.clean(), watch


def test_zoom_without_cursor_centres_on_window(page: Any, season_app: str) -> None:
    watch = _open_q7(page, season_app)
    page.click("#zoom-in")
    lo, hi = _view(page)
    assert (lo + hi) / 2 == pytest.approx(sum(FULL) / 2) and hi - lo == pytest.approx(FULL_SPAN / 2)
    assert watch.clean(), watch


def test_zoom_window_clamps(page: Any, season_app: str) -> None:
    watch = _open_q7(page, season_app, "&t=160")
    for _ in range(3):
        page.click("#zoom-out")
    assert _view(page) == pytest.approx(FULL)
    for _ in range(12):
        page.click("#zoom-in")
    lo, hi = _view(page)
    assert hi - lo == pytest.approx(0.5) and lo >= FULL[0] and hi <= FULL[1]
    _drag(page, 159.0, 159.1)
    lo, hi = _view(page)
    assert hi - lo == pytest.approx(0.5)
    assert watch.clean(), watch


def test_zoom_hidden_marker_selection_pans(page: Any, season_app: str) -> None:
    watch = _open_q7(page, season_app)
    _drag(page, 95.0, 99.0)
    page.click("#markers-left")
    lo, hi = _view(page)
    assert lo < 20.0 < hi and hi - lo == pytest.approx(4.0, abs=0.1)
    assert page.inner_text("#scrub-label") == "T+20.00 s"
    assert "Temperature 66" in page.inner_text("#event")
    assert page.locator("#markers-left").is_hidden()
    assert page.inner_text("#markers-right").startswith("2 ▸")
    assert watch.clean(), watch


def test_zoom_range_readouts_follow_window(page: Any, season_app: str) -> None:
    watch = _open_q7(page, season_app)
    battery = "[data-range='0']"
    assert page.inner_text(battery) == "6.00 – 12.00 V"
    _drag(page, 40.0, 60.0)
    assert page.inner_text(battery) == "12.00 – 12.00 V"
    page.click("#zoom-reset")
    _drag(page, 95.0, 99.0)
    assert page.inner_text(battery) == "6.00 – 12.00 V"
    assert watch.clean(), watch


def test_zoom_overlay_follows_window(page: Any, season_app: str) -> None:
    watch = _open_q7(page, season_app)
    page.select_option("#compare", "2026gacmp_qm8")
    page.wait_for_function(
        "() => FP.replay.charts().length === 4"
        " && document.querySelector('.readout-table th:nth-child(5)')"
    )
    _drag(page, 95.0, 99.0)
    labels = page.evaluate("() => FP.replay.charts().map(u => u.series.map(s => s.label))")
    assert all("Q8" in series for series in labels)
    for lo, hi in _scales(page):
        assert lo == pytest.approx(95.0, abs=0.05) and hi == pytest.approx(99.0, abs=0.05)
    assert watch.clean(), watch


def test_zoom_static_bucket_resolution(page: Any, season_site: dict[str, Any]) -> None:
    watch = _open_q7(page, season_site["static"])
    assert page.inner_text("#resolution") == ""
    _drag(page, 96.0, 98.0)
    assert page.inner_text("#resolution") == "bucket resolution (170 ms)"
    assert watch.clean(), watch


# --- Served detail on zoom (spec: Windowed detail in served mode) --------------------------


def _envelope_requests(page: Any) -> list[str]:
    requests: list[str] = []
    page.on("request", lambda r: requests.append(r.url) if "/api/envelope/" in r.url else None)
    return requests


def _spacing(page: Any) -> list[float]:
    spacing = page.evaluate("() => FP.replay.charts().map(u => u.data[0][1] - u.data[0][0])")
    return [round(float(x), 3) for x in spacing]


def test_detail_fetched_once_after_zoom(page: Any, season_site: dict[str, Any]) -> None:
    watch = _open_q7(page, season_site["served"])
    requests = _envelope_requests(page)
    _drag(page, 96.0, 98.0)
    page.wait_for_function("() => FP.replay.detail() !== null")
    page.wait_for_timeout(300)
    assert len(requests) == 1 and "from=96" in requests[0] and "to=98" in requests[0]
    assert _spacing(page) == [0.01] * 4
    assert "10 ms" in page.inner_text("#resolution")
    for lo, hi in _scales(page):
        assert lo == pytest.approx(96.0, abs=0.05) and hi == pytest.approx(98.0, abs=0.05)
    spike = page.evaluate(
        "() => { const u = FP.replay.charts()[3];"
        " return Math.max(...u.data[1].filter(v => v != null)); }"
    )  # the hood supply current track's band max: the 4 ms 150 A spike at T+97
    assert spike == 150.0
    assert watch.clean(), watch


def test_detail_burst_applies_only_the_last(page: Any, season_site: dict[str, Any]) -> None:
    watch = _open_q7(page, season_site["served"])
    requests = _envelope_requests(page)
    page.evaluate(
        "() => { const o = FP.replay.charts()[0].over; const r = o.getBoundingClientRect();"
        " for (let i = 0; i < 5; i += 1) o.dispatchEvent(new WheelEvent('wheel', { deltaY: -150,"
        " ctrlKey: true, clientX: r.left + r.width / 2, clientY: r.top + 10, bubbles: true,"
        " cancelable: true })); }"
    )
    page.wait_for_function("() => FP.replay.detail() !== null")
    page.wait_for_timeout(300)
    assert len(requests) == 1  # debounced
    held: list[Any] = []
    page.route("**/api/envelope/**", lambda r: held.append(r))
    page.click("#zoom-reset")
    _drag(page, 96.0, 98.0)
    for _ in range(40):
        if held:
            break
        page.wait_for_timeout(50)
    page.click("#zoom-in")  # 96.5-97.5 s, before the first answer arrives
    for _ in range(40):
        if len(held) == 2:
            break
        page.wait_for_timeout(50)
    assert len(held) == 2
    held[1].continue_()
    page.wait_for_function("() => FP.replay.detail() && FP.replay.detail().t0 > 96.4")
    held[0].continue_()
    page.wait_for_timeout(400)
    assert page.evaluate("() => FP.replay.detail().t0") == pytest.approx(96.5, abs=0.01)
    assert _view(page) == pytest.approx((96.5, 97.5), abs=0.01)
    assert watch.clean(), watch


def test_detail_unavailable_without_silver(page: Any, tmp_path: Path) -> None:
    lake = build_replay_season(tmp_path / "lake", events=("2026gacmp",))
    shutil.rmtree(lake.silver)
    server = _serve(lake)
    try:
        watch = _open_q7(page, f"http://127.0.0.1:{server.server_address[1]}/")
        requests = _envelope_requests(page)
        _drag(page, 96.0, 98.0)
        page.wait_for_function(
            "() => document.getElementById('resolution').textContent.includes('unavailable')"
        )
        note = page.inner_text("#resolution")
        assert note.startswith("bucket resolution (170 ms)") and "finer data unavailable" in note
        assert len(requests) == 1 and _spacing(page) == [0.17] * 4
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_detail_dropped_when_zoomed_out(page: Any, season_site: dict[str, Any]) -> None:
    watch = _open_q7(page, season_site["served"])
    requests = _envelope_requests(page)
    _drag(page, 96.0, 98.0)
    page.wait_for_function("() => FP.replay.detail() !== null")
    page.click("#zoom-reset")
    page.wait_for_timeout(300)
    assert page.evaluate("() => FP.replay.detail()") is None
    assert len(requests) == 1 and _spacing(page) == [0.17] * 4
    assert page.inner_text("#resolution") == ""
    assert watch.clean(), watch


def test_detail_header_names_overlay_resolution(page: Any, season_site: dict[str, Any]) -> None:
    watch = _open_q7(page, season_site["served"], "&o=2026gacmp_qm8")
    _drag(page, 96.0, 98.0)
    page.wait_for_function("() => FP.replay.detail() !== null")
    note = page.inner_text("#resolution")
    assert "10 ms" in note and "Q8" in note and "170 ms" in note
    assert watch.clean(), watch


# --- Shareable links: filters and window (spec: Shareable view links) ---------------------


def test_link_restores_filters_and_window(page: Any, season_app: str, browser: Any) -> None:
    watch = _open_q7(page, season_app)
    page.select_option("#f-robot", "comp")
    _drag(page, 95.0, 99.0)
    assert "z=95.00%2C99.00" in page.url or "z=95.00,99.00" in page.url
    copied = page.url
    other = browser.new_page()
    watch2 = _open(other, copied)
    other.wait_for_function("() => FP.replay.charts().length === 4")
    assert other.input_value("#f-robot") == "comp"
    assert _listed(other) == ["2026gacmp_qm7", "2026gacmp_qm8"]
    assert other.inner_text("#view-window") == "T+95.0 – T+99.0 s"
    for lo, hi in _scales(other):
        assert lo == pytest.approx(95.0, abs=0.01) and hi == pytest.approx(99.0, abs=0.01)
    other.close()
    assert watch.clean() and watch2.clean(), (watch, watch2)


def test_link_window_places_markers(page: Any, season_app: str) -> None:
    """Opening on a window: markers are placed once the plots have their size."""
    watch = _open_q7(page, season_app, "&z=95.00,99.00")
    page.wait_for_timeout(200)
    centre = page.evaluate(
        "() => { const b = [...document.querySelectorAll('[data-marker]')].find(x => !x.hidden);"
        " const r = b.getBoundingClientRect(); return r.left + r.width / 2; }"
    )
    under = page.evaluate(
        "() => FP.replay.charts()"
        ".map(u => u.over.getBoundingClientRect().left + u.valToPos(97, 'x'))"
    )
    assert all(abs(x - centre) < 1.5 for x in under), (centre, under)
    assert watch.clean(), watch


@pytest.mark.parametrize("z", ["300,20", "500,600", "x,4", "95", "nan,99"])
def test_bad_window_in_link_opens_full(page: Any, season_app: str, z: str) -> None:
    watch = _open_q7(page, season_app, f"&z={z}")
    assert _view(page) == pytest.approx(FULL)
    assert "z=" not in page.url  # omitted at the full window
    assert watch.clean(), watch


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
    watch = _open(
        page, history_site, "#view=history&season=2026&robot=2026-comp&unit=ctre%3ADRIVE-FL"
    )
    page.wait_for_selector("#unit-matches a")
    page.click("#unit-matches a >> nth=0")
    page.wait_for_selector("text=not built")
    assert f"m={match_key(59)}" in page.url and "track=drive-fl" in page.url
    assert "season=2026" in page.url and "robot=2026-comp" in page.url  # carried to Replay
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
