"""The team's name and palette in the browser (spec: team-branding, "Team display name" and
"Theme applied everywhere"). Run with `pytest -m browser`."""

import shutil
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config, theme
from flashpoint.lake.paths import LakePaths
from flashpoint.report.build import ReportBuilder
from flashpoint.report.export import export_static
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.server import FlashpointServer
from tests.report.lake import MatchLake, quiet_rows
from tests.web.test_browser import _open

pytestmark = pytest.mark.browser
playwright = pytest.importorskip("playwright.sync_api")

MATCH = "2026gacmp_qm7"
AMBER = {theme.AMBER_TOKENS["accent"].lower(), theme.AMBER_TOKENS["accent-hi"].lower()}
# Records every colour the canvases stroke with (uPlot can't be asked for its series colours).
RECORD_STROKES = """
(() => {
  const stroke = CanvasRenderingContext2D.prototype.stroke;
  window.__strokes = [];
  CanvasRenderingContext2D.prototype.stroke = function (...args) {
    window.__strokes.push(String(this.strokeStyle));
    return stroke.apply(this, args);
  };
})();
"""


@pytest.fixture(scope="module")
def lake(tmp_path_factory: pytest.TempPathFactory) -> LakePaths:
    site = MatchLake(tmp_path_factory.mktemp("theme") / "lake")
    site.add_match(MATCH, "q7", quiet_rows())
    builder = ReportBuilder(site.write_meta(), config.config_root())
    builder.build()
    builder.close()
    return site.lake


def _config(tmp_path: Path, theme_toml: str | None, team: bool = True) -> Path:
    root = tmp_path / "cfg"
    shutil.copytree(config.config_root(), root)  # Ignite's configuration: team 6829
    if not team:
        (root / "acquire.toml").unlink()
    if theme_toml is not None:
        (root / "theme.toml").write_text(theme_toml)
    return root


@pytest.fixture
def served(lake: LakePaths) -> Iterator[Callable[[Path], str]]:
    started: list[FlashpointServer] = []

    def start(config_dir: Path) -> str:
        server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake))
        server.config_dir = config_dir
        threading.Thread(target=server.serve_forever, daemon=True).start()
        started.append(server)
        return f"http://127.0.0.1:{server.server_address[1]}/"

    yield start
    for server in started:
        server.shutdown()
        server.server_close()
        server.queries.close()


def _team(page: Any) -> str | None:
    found: str | None = page.evaluate(
        "() => { const t = document.querySelector('.app-header .brand__team');"
        " return t ? t.textContent : null; }"
    )
    return found


def _token(page: Any, name: str) -> str:
    return str(
        page.evaluate(
            f"() => getComputedStyle(document.documentElement).getPropertyValue('--{name}').trim()"
        )
    ).upper()


# --- Team display name ----------------------------------------------------------------------


def test_name_and_number_in_every_view(
    page: Any, served: Callable[[Path], str], tmp_path: Path
) -> None:
    url = served(_config(tmp_path, 'team_name = "Ignite Robotics"\n'))
    watch = _open(page, url)
    views = page.eval_on_selector_all("nav.tabs a.tab", "els => els.map(e => e.dataset.view)")
    assert len(views) >= 4
    for view in views:
        # A fresh load per view: leaving Setup mid-load hits a race in setup.js unrelated to
        # the theme (loadChecks writes to `ui` after unmount nulls it).
        page.goto("about:blank")
        _open(page, url, f"#view={view}")
        assert _team(page) == "Ignite Robotics · 6829", view
        assert page.locator(".app-header .brand__team").is_visible(), view
        assert "Flashpoint · Ignite Robotics · 6829" in page.title(), view
    assert watch.clean(), watch


def test_no_name(page: Any, served: Callable[[Path], str], tmp_path: Path) -> None:
    watch = _open(page, served(_config(tmp_path, None, team=False)))
    assert _team(page) is None
    sub = page.eval_on_selector(".app-header .brand__sub", "e => e.textContent")
    assert sub == "telemetry"
    assert page.title() == "Replay · Flashpoint"
    assert watch.clean(), watch


def test_markup_in_the_name(page: Any, served: Callable[[Path], str], tmp_path: Path) -> None:
    watch = _open(page, served(_config(tmp_path, 'team_name = "<b>Team</b>"\n', team=False)))
    assert _team(page) == "<b>Team</b>"
    assert page.eval_on_selector(".app-header", "e => e.querySelectorAll('b').length") == 0
    assert "<b>Team</b>" in page.title()
    assert watch.clean(), watch


# --- Theme applied everywhere ---------------------------------------------------------------


def test_served_palette_applies_before_the_shell(
    page: Any, served: Callable[[Path], str], tmp_path: Path
) -> None:
    green = theme.preset_tokens("green")
    watch = _open(page, served(_config(tmp_path, 'palette = "green"\n')))
    for name in (
        "accent",
        "accent-hi",
        "bg",
        "line",
        "warn",
        "warn-rgb",
        "accent-hi-rgb",
        "bg-rgb",
    ):
        assert _token(page, name) == green[name], name
    mark = page.eval_on_selector(".brand__mark", "e => getComputedStyle(e).color")
    assert mark == "rgb({})".format(green["accent-rgb"])
    assert page.evaluate("() => FP.tokens().accent").upper() == green["accent"]
    assert watch.clean(), watch


def test_broken_theme_file_uses_the_defaults(
    page: Any, served: Callable[[Path], str], tmp_path: Path
) -> None:
    toml = 'team_name = "Ignite Robotics"\npalette = "green"\ncolour = "red"\n'
    watch = _open(page, served(_config(tmp_path, toml)))
    assert _token(page, "accent") == theme.AMBER_TOKENS["accent"]
    assert _team(page) is None
    assert watch.clean(), watch


def test_static_export(page: Any, lake: LakePaths, tmp_path: Path) -> None:
    cfg = _config(tmp_path, 'team_name = "Ignite Robotics"\npalette = "green"\n')
    export_static(lake, tmp_path / "export", config_dir=cfg)
    page.context.set_offline(True)
    requests: list[str] = []
    page.on("request", lambda r: requests.append(r.url))
    watch = _open(page, (tmp_path / "export" / "index.html").as_uri(), f"#view=replay&m={MATCH}")
    page.wait_for_selector("#readout tr")
    green = theme.preset_tokens("green")
    assert _token(page, "accent") == green["accent"] and _token(page, "bg") == green["bg"]
    assert _team(page) == "Ignite Robotics · 6829"
    assert all(r.startswith("file:") for r in requests), requests
    assert watch.clean(), watch


@pytest.mark.parametrize(
    "primary",
    [
        "#00A3E0",  # the spec's example
        "#54CCFF",
    ],
)
def test_charts_follow_the_palette(
    page: Any, served: Callable[[Path], str], tmp_path: Path, primary: str
) -> None:
    url = served(_config(tmp_path, f'palette = "custom"\n[custom]\nprimary = "{primary}"\n'))
    page.add_init_script(RECORD_STROKES)
    watch = _open(page, url, f"#view=replay&m={MATCH}&t=90")
    page.wait_for_selector("#readout tr")
    page.wait_for_selector(".track__plot canvas")
    tokens = theme.derive_tokens(primary)
    strokes = set(page.evaluate("() => window.__strokes"))
    assert tokens["accent"].lower() in strokes  # the primary series
    assert tokens["accent-hi"].lower() in strokes  # the cursor
    assert not strokes & AMBER, strokes & AMBER
    assert watch.clean(), watch
