"""The Anomalies view, triage, and the navigation badge in Chromium (spec: anomaly-review)."""

import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.anomaly import detect
from flashpoint.lake.paths import LakePaths
from flashpoint.views.anomalies import AnomalyQueries
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.server import FlashpointServer
from tests.web.anomaly_lake import build_anomaly_lake, build_event_boundary_lake, ids
from tests.web.test_browser import NetworkServer, Recorder, _open, _stop

pytestmark = pytest.mark.browser
HOSTILE = "<img src=x onerror=alert(1)>"


@pytest.fixture
def site(tmp_path: Path) -> Iterator[dict[str, Any]]:
    lake, config_dir = build_anomaly_lake(tmp_path)
    spawned = Recorder()
    server = FlashpointServer(
        ("127.0.0.1", 0), lake, HistoryQueries(lake),
        anomalies=AnomalyQueries(lake, config_dir), spawner=spawned,
    )  # fmt: skip
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield {"url": f"http://127.0.0.1:{server.server_address[1]}/", "lake": lake,
           "config": config_dir, "ids": ids(lake), "spawned": spawned}  # fmt: skip
    _stop(server)


def _anomalies(page: Any, url: str, fragment: str = "") -> Any:
    watch = _open(page, url, "#view=anomalies" + fragment)
    page.wait_for_selector("#queue .anom, #queue-empty")
    return watch


def _queue(page: Any) -> list[str]:
    return [
        str(x)
        for x in page.eval_on_selector_all("#queue .anom", "els => els.map(e => e.dataset.id)")
    ]


def _selected(page: Any) -> str:
    return str(page.get_attribute("#queue .anom[aria-pressed='true']", "data-id"))


def _wait_open(page: Any, n: int) -> None:
    page.wait_for_function(
        f"() => document.querySelector('[data-tab=open]').innerText === 'OPEN · {n}'"
    )


def _sens(page: Any, value: str) -> None:
    page.click(f"#sens [data-sens='{value}']")
    page.wait_for_function(f"() => location.hash.includes('sens={value}')")
    page.wait_for_selector(f"#sens [data-sens='{value}'][aria-pressed='true']")


# --- the view ---------------------------------------------------------------------------------


def test_queue_and_detail(page: Any, site: dict[str, Any]) -> None:
    watch = _anomalies(page, site["url"])
    found = site["ids"]
    assert _queue(page) == [found["step"], found["spec"]]  # HIGH first
    assert page.inner_text("#open-count") == "2"
    assert "scanned through 2026syn_qm25" in page.inner_text("#open-sub")
    page.wait_for_selector("#detail #why")
    assert page.inner_text("#why").startswith("STEP:")
    assert "stepped up" in page.inner_text("#title")
    chart = page.evaluate("() => FP.anomalyChart()")
    assert chart["flagged"][-1] is not None
    assert page.locator("#causes li").count() >= 1
    assert "not a diagnosis" in page.inner_text("#detail")
    assert watch.clean(), watch


def test_event_boundary_chip(page: Any, tmp_path: Path) -> None:
    lake, config_dir = build_event_boundary_lake(tmp_path)
    server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake),
                              anomalies=AnomalyQueries(lake, config_dir))  # fmt: skip
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        watch = _anomalies(page, f"http://127.0.0.1:{server.server_address[1]}/")
        chip = page.locator("#queue .anom [data-event-boundary]")
        assert chip.count() == 1 and chip.inner_text() == "EVENT BOUNDARY · AAA → BBB"
        page.click("#queue .anom")
        page.wait_for_selector("#detail #event-boundary")
        assert "AAA → BBB" in page.inner_text("#event-boundary")
        assert "first match at 2026bbb, after 2026aaa" in page.inner_text("#why")
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_sensitivity(page: Any, site: dict[str, Any]) -> None:
    watch = _anomalies(page, site["url"])
    assert page.inner_text("[data-tab='open']") == "OPEN · 2"
    _sens(page, "strict")
    page.wait_for_function(
        "() => document.querySelector('[data-tab=open]').innerText === 'OPEN · 3'"
    )
    assert site["ids"]["drift"] in _queue(page)
    _sens(page, "relaxed")
    page.wait_for_function(
        "() => document.querySelector('[data-tab=open]').innerText === 'OPEN · 2'"
    )
    assert site["ids"]["drift"] not in _queue(page)
    assert watch.clean(), watch


def test_nothing_open(page: Any, tmp_path: Path) -> None:
    from tests.anomaly.gold_builder import GoldLake

    gold = GoldLake(tmp_path / "lake", matches=3, slots=["hood", "drive-fl"])
    lake = gold.write()
    detect.detect(lake, config.config_root())
    server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        watch = _anomalies(page, f"http://127.0.0.1:{server.server_address[1]}/")
        assert "Nothing is flagged" in page.inner_text("#queue-empty")
        assert page.inner_text("#building").startswith("2 unit(s) are still building")
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_link_keeps_the_selection(page: Any, site: dict[str, Any], browser: Any) -> None:
    drift = site["ids"]["drift"]
    _anomalies(page, site["url"])
    _sens(page, "strict")
    page.click(f"#queue .anom[data-id='{drift}']")
    page.wait_for_function(f"() => location.hash.includes('id={drift}')")
    link = page.url
    other = browser.new_page()
    try:
        watch = _open(other, link)
        other.wait_for_selector("#queue .anom[aria-pressed='true']")
        assert _selected(other) == drift
        assert other.get_attribute("#sens [data-sens='strict']", "aria-pressed") == "true"
        assert other.get_attribute("[data-tab='open']", "aria-pressed") == "true"
        assert watch.clean(), watch
    finally:
        other.close()


def test_evidence_point_opens_replay(page: Any, site: dict[str, Any]) -> None:
    watch = _anomalies(page, site["url"], "&id=" + site["ids"]["step"])
    page.wait_for_selector("#evidence .u-over")
    points = page.evaluate("() => FP.anomalyChart()")
    onset = next(i for i, v in enumerate(points["flagged"]) if v is not None)
    over = page.locator("#evidence .u-over").bounding_box()
    frac = (onset + 0.5) / len(points["flagged"])
    page.mouse.click(over["x"] + frac * over["width"], over["y"] + over["height"] / 2)
    page.wait_for_function("() => location.hash.includes('view=replay')")
    assert "m=2026syn_qm21" in page.url and "track=hood" in page.url
    assert watch.clean(), watch


def test_subject_opens_history_on_the_metric(page: Any, site: dict[str, Any]) -> None:
    watch = _anomalies(page, site["url"], "&id=" + site["ids"]["step"])
    page.wait_for_selector("#subject-link")
    page.click("#subject-link")
    page.wait_for_function("() => location.hash.includes('view=history')")
    assert "unit=ctre%3AHOOD" in page.url and "metric=peak_temp" in page.url
    assert watch.clean(), watch


# --- triage -----------------------------------------------------------------------------------


def _act(page: Any, to: str) -> None:
    page.click(f"#actions [data-to='{to}']")


def test_investigate_then_resolve(page: Any, site: dict[str, Any]) -> None:
    step = site["ids"]["step"]
    watch = _anomalies(page, site["url"], "&id=" + step)
    page.wait_for_selector("#actions")
    _act(page, "investigating")
    page.wait_for_function("() => document.querySelector('#state').innerText === 'INVESTIGATING'")
    _act(page, "resolved")
    page.fill("#outcome-input", "wheel bearing replaced")
    page.click("#triage-save")
    page.wait_for_function("() => document.querySelector('#open-count').innerText === '1'")
    page.click("[data-tab='resolved']")
    page.wait_for_selector(f"#queue .anom[data-id='{step}']")
    page.wait_for_selector("#outcome")
    assert page.inner_text("#outcome") == "wheel bearing replaced"
    assert page.locator("#history li").count() == 2
    assert watch.clean(), watch


def test_close_without_a_reason(page: Any, site: dict[str, Any]) -> None:
    watch = _anomalies(page, site["url"], "&id=" + site["ids"]["spec"])
    page.wait_for_selector("#actions")
    _act(page, "dismissed")
    page.click("#triage-save")
    page.wait_for_selector("#triage-error")
    assert "outcome is required" in page.inner_text("#triage-error")
    assert page.inner_text("#state") == "NEW"
    assert watch.errors == [] or all("400" in e for e in watch.errors)
    assert not watch.dialogs and not watch.remote


def test_hostile_outcome_text(page: Any, site: dict[str, Any]) -> None:
    spec = site["ids"]["spec"]
    watch = _anomalies(page, site["url"], "&id=" + spec)
    page.wait_for_selector("#actions")
    _act(page, "dismissed")
    page.fill("#outcome-input", HOSTILE)
    page.click("#triage-save")
    page.click("[data-tab='dismissed']")
    page.wait_for_selector(f"#queue .anom[data-id='{spec}']")
    page.wait_for_selector("#outcome")
    assert page.inner_text("#outcome") == HOSTILE
    assert page.locator("#detail img").count() == 0
    assert watch.clean(), watch


def test_restart_baseline_pending(page: Any, site: dict[str, Any]) -> None:
    step = site["ids"]["step"]
    watch = _anomalies(page, site["url"], "&id=" + step)
    page.wait_for_selector("#actions")
    _act(page, "dismissed")
    page.fill("#outcome-input", "new heat sink fitted")
    page.check("#restart-input")
    page.click("#triage-save")
    page.click("[data-tab='dismissed']")
    page.wait_for_selector("#restart-pending")
    assert len(site["spawned"].calls) == 1  # the detached `flashpoint detect`
    detect.detect(site["lake"], site["config"])  # what that process does
    page.reload()
    page.wait_for_selector("#queue .anom, #queue-empty")
    page.wait_for_selector("#detail h1")
    assert page.locator("#restart-pending").count() == 0
    assert watch.clean(), watch


def test_read_only_when_shared(page: Any, tmp_path: Path) -> None:
    lake, config_dir = build_anomaly_lake(tmp_path)
    server = NetworkServer(("127.0.0.1", 0), lake, HistoryQueries(lake),
                           anomalies=AnomalyQueries(lake, config_dir))  # fmt: skip
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        watch = _anomalies(page, f"http://127.0.0.1:{server.server_address[1]}/")
        page.wait_for_selector("#read-only")
        assert page.locator("#actions").count() == 0
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_keyboard_reaches_triage(page: Any, site: dict[str, Any]) -> None:
    _anomalies(page, site["url"], "&id=" + site["ids"]["step"])
    page.wait_for_selector("#actions")
    page.focus("#queue .anom >> nth=0")
    for _ in range(40):
        page.keyboard.press("Tab")
        if page.evaluate("() => document.activeElement.dataset.to === 'investigating'"):
            break
    style = page.evaluate(
        "() => { const s = getComputedStyle(document.activeElement);"
        " return [document.activeElement.dataset.to, s.outlineStyle]; }"
    )
    assert style == ["investigating", "solid"]
    page.keyboard.press("Enter")
    page.wait_for_function("() => document.querySelector('#state').innerText === 'INVESTIGATING'")


# --- navigation badge -------------------------------------------------------------------------


def test_nav_badge(page: Any, site: dict[str, Any]) -> None:
    watch = _open(page, site["url"], "#view=replay")
    badge = page.locator("[data-badge='anomalies']")
    page.wait_for_selector("[data-badge='anomalies']:not([hidden])")
    assert badge.inner_text() == "2"
    assert "tab__badge--hot" in str(badge.get_attribute("class"))
    _anomalies(page, site["url"], "&id=" + site["ids"]["step"])
    page.wait_for_selector("#actions")
    _act(page, "dismissed")
    page.fill("#outcome-input", "expected heat")
    page.click("#triage-save")
    page.wait_for_function(
        "() => document.querySelector('[data-badge=anomalies]').innerText === '1'"
    )
    assert "tab__badge--hot" not in str(badge.get_attribute("class"))
    assert watch.clean(), watch


@pytest.mark.corpus
def test_queue_from_the_corpus(page: Any, derived_corpus_lake: Any, brownout_config: Path) -> None:
    """Test thresholds (brownout 8.5 V, sag 9.0 V): the shipped ones find no Q7 brownout."""
    detect.detect(derived_corpus_lake, brownout_config, force=True)
    server = FlashpointServer(("127.0.0.1", 0), derived_corpus_lake,
                              HistoryQueries(derived_corpus_lake))  # fmt: skip
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        watch = _anomalies(page, f"http://127.0.0.1:{server.server_address[1]}/")
        first = page.locator("#queue .anom").first
        assert first.inner_text().startswith("HIGH · Battery")
        assert "brownout" in first.inner_text()
        page.wait_for_selector("#why")
        assert page.inner_text("#why").startswith("SPEC:")
        page.wait_for_function("() => FP.anomalyChart() != null")  # drawn just after the text
        assert page.evaluate("() => FP.anomalyChart()")["flagged"][-1] is not None
        assert watch.clean(), watch
    finally:
        _stop(server)


# --- Replay badges and the static snapshot (spec: match-reports / Anomaly badges in Replay) -----

Q7 = "2026gacmp_qm7"


@pytest.fixture(scope="module")
def q7_site(
    corpus_group: Any, tmp_path_factory: pytest.TempPathFactory, brownout_config: Path
) -> Iterator[dict[str, Any]]:
    """Q7 under the test thresholds (brownout 8.5 V, sag 9.0 V), so it has a brownout."""
    import os
    import subprocess
    import sys

    from flashpoint.report.build import ReportBuilder
    from flashpoint.report.export import export_static

    root = tmp_path_factory.mktemp("q7-badges")
    match_dir = root / "q7"
    match_dir.mkdir()
    for path in corpus_group("2026-gacmp-q7"):
        (match_dir / path.name).symlink_to(path)
    lake = LakePaths(root / "lake")
    subprocess.run(  # ingest, derive, and detect, as a pit laptop does
        [sys.executable, "-m", "flashpoint", "ingest", "--lake", str(lake.root), str(match_dir)],
        check=True, capture_output=True,
        env={**os.environ, config.CONFIG_ENV: str(brownout_config)},
    )  # fmt: skip
    builder = ReportBuilder(lake, brownout_config)
    builder.build()
    builder.close()
    export = root / "export"
    export_static(lake, export, config_dir=brownout_config)
    server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    brownout = next(r["id"] for r in detect.read_anomalies(lake).to_dicts()
                    if r["metric"] == "brownouts")  # fmt: skip
    yield {"url": f"http://127.0.0.1:{server.server_address[1]}/", "lake": lake,
           "static": (export / "index.html").as_uri(), "brownout": brownout}  # fmt: skip
    _stop(server)


def _data_digest(lake: LakePaths) -> dict[str, bytes]:
    data = lake.root / "report" / "data"
    return {p.name: p.read_bytes() for p in sorted(data.glob("*.js"))}


@pytest.mark.corpus
def test_served_badge(page: Any, q7_site: dict[str, Any]) -> None:
    watch = _open(page, q7_site["url"], f"#view=replay&m={Q7}")
    page.wait_for_selector(f"a.match-btn[data-match='{Q7}'] .anom-glyph")
    assert page.inner_text(f"a.match-btn[data-match='{Q7}'] .anom-glyph") == "HIGH"
    page.wait_for_selector(f"[data-anomaly-badge='{q7_site['brownout']}']")  # tracks render later
    badge = page.locator(f"[data-anomaly-badge='{q7_site['brownout']}']")
    assert badge.count() == 1 and "brownouts" in badge.inner_text()
    battery = page.locator(".track").first  # the battery track
    assert (
        "BATTERY" in battery.inner_text() and battery.locator("[data-anomaly-badge]").count() == 1
    )
    badge.click()
    page.wait_for_function("() => location.hash.includes('view=anomalies')")
    assert f"id={q7_site['brownout']}" in page.url
    page.wait_for_selector("#why")
    assert watch.clean(), watch


@pytest.mark.corpus
def test_resolved_anomaly_drops_its_badge(page: Any, q7_site: dict[str, Any]) -> None:
    lake = q7_site["lake"]
    before = _data_digest(lake)
    queries = AnomalyQueries(lake, config.config_root())
    queries.triage(q7_site["brownout"], {"to": "investigating"})
    queries.triage(q7_site["brownout"], {"to": "resolved", "outcome": "battery replaced"})
    try:
        watch = _open(page, q7_site["url"], f"#view=replay&m={Q7}")
        page.wait_for_selector("#readout tr")
        # Q7's intake stalls stay open (MED); the brownout's badge and the HIGH glyph go.
        assert page.locator(f"[data-anomaly-badge='{q7_site['brownout']}']").count() == 0
        assert page.inner_text(f"a.match-btn[data-match='{Q7}'] .anom-glyph") == "MED"
        assert _data_digest(lake) == before
        assert watch.clean(), watch
    finally:
        queries.triage(q7_site["brownout"], {"to": "new"})  # reopen for the other tests


@pytest.mark.corpus
def test_static_snapshot_badge(page: Any, q7_site: dict[str, Any]) -> None:
    watch = _open(page, q7_site["static"], f"#view=replay&m={Q7}")
    page.wait_for_selector(f"[data-anomaly-badge='{q7_site['brownout']}']")
    badge = page.locator(f"[data-anomaly-badge='{q7_site['brownout']}']")
    assert badge.evaluate("e => e.tagName") == "SPAN"  # read-only: no link anywhere
    assert "snapshot" in badge.inner_text()
    assert page.inner_text(f"a.match-btn[data-match='{Q7}'] .anom-glyph") == "HIGH"
    assert "ANOMALIES" not in page.inner_text("nav.tabs")
    assert watch.clean(), watch  # nothing but file:// was requested


@pytest.mark.corpus
def test_detection_does_not_rebuild_replay(
    q7_site: dict[str, Any], tmp_path: Path, brownout_config: Path
) -> None:
    import shutil

    from flashpoint.report.build import ReportBuilder

    lake = q7_site["lake"]
    before = {p.name: p.stat().st_mtime_ns for p in (lake.root / "report" / "data").glob("*.js")}
    config_dir = tmp_path / "config"
    shutil.copytree(brownout_config, config_dir)  # the site's configuration ...
    (config_dir / "anomaly.toml").write_text("[spec]\nstall_med_s = 10\n")  # ... but detection
    assert detect.detect(lake, config_dir).ran
    builder = ReportBuilder(lake, config_dir)
    try:
        summary = builder.build()
    finally:
        builder.close()
    assert summary.built == [] and Q7 in summary.unchanged
    after = {p.name: p.stat().st_mtime_ns for p in (lake.root / "report" / "data").glob("*.js")
             if p.name != "matches.js"}  # fmt: skip
    assert after == {k: v for k, v in before.items() if k != "matches.js"}
    detect.detect(lake, brownout_config)  # back to the site's configuration


# --- History health (spec: lifetime-trends / Device table and summary tiles) --------------------


def test_history_health_from_an_anomaly(page: Any, tmp_path: Path) -> None:
    from tests.views.season import build_season
    from tests.views.test_queries import _sibling_lake

    season = build_season(tmp_path / "season")
    anomalies = _sibling_lake(season, tmp_path)
    queries = HistoryQueries(anomalies.lake, season.robots, anomalies=anomalies)
    server = FlashpointServer(("127.0.0.1", 0), anomalies.lake, queries, anomalies=anomalies)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        watch = _open(page, f"http://127.0.0.1:{server.server_address[1]}/", "#view=history")
        page.wait_for_selector("[data-anomalies='ctre:DRIVE-BL']")
        row = page.locator("#rows tr", has=page.locator("[data-anomalies='ctre:DRIVE-BL']"))
        assert "HOT" in row.inner_text() and "1 HIGH" in row.inner_text()
        assert page.inner_text("#tiles").split()[-1] == "3"  # UNITS WARN/HOT
        page.click("[data-anomalies='ctre:DRIVE-BL']")
        page.wait_for_selector("#queue .anom")
        assert "unit=ctre%3ADRIVE-BL" in page.url
        assert page.inner_text("#unit-chip").startswith("UNIT")
        assert watch.clean(), watch
    finally:
        _stop(server)
