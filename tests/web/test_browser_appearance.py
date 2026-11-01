"""The Setup view's Appearance section in Chromium (team-branding design D-f): a preset or
custom colours, a live preview scoped to one container, and the config editor's guarded save."""

import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flashpoint import theme
from flashpoint.lake.paths import LakePaths
from tests.web.test_browser import Watch, _open, _stop
from tests.web.test_browser_setup import _site

pytestmark = pytest.mark.browser
AMBER = "rgb(255, 176, 0)"

COLOUR = "(sel) => getComputedStyle(document.querySelector(sel)).color"
ROOT_TOKEN = "(name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim()"


@pytest.fixture
def site(tmp_path: Path) -> Iterator[dict[str, Any]]:
    server, root = _site(tmp_path, LakePaths(tmp_path / "lake"))
    yield {"url": f"http://127.0.0.1:{server.server_address[1]}/", "root": root}
    _stop(server)


def _appearance(page: Any, url: str) -> Watch:
    watch = _open(page, url, "#view=setup&tab=config")
    page.wait_for_selector("#cfg-theme #theme-palette")
    return watch


def _rgb(hex_colour: str) -> str:
    r, g, b = (int(hex_colour[i : i + 2], 16) for i in (1, 3, 5))
    return f"rgb({r}, {g}, {b})"


def _previewed(page: Any, token: str, value: str) -> None:
    page.wait_for_function(
        "([name, value]) => document.getElementById('theme-preview')"
        ".style.getPropertyValue(name) === value",
        arg=[f"--{token}", value],
    )


def test_preview_before_saving(page: Any, site: dict[str, Any]) -> None:
    """Preview before saving: the preview shows the new colours, the rest of the app keeps the
    saved palette, and theme.toml is unchanged."""
    watch = _appearance(page, site["url"])
    page.select_option("#theme-palette", "custom")
    page.fill("#theme-primary", "#00A3E0")
    _previewed(page, "accent", "#00A3E0")
    derived = theme.derive_tokens("#00A3E0")
    preview = "#theme-preview"
    assert page.evaluate(COLOUR, f"{preview} .brand__mark") == "rgb(0, 163, 224)"
    assert page.evaluate(COLOUR, f"{preview} .brand__team") == _rgb(derived["accent-hi"])
    assert page.evaluate(COLOUR, f"{preview} .callout") == _rgb(derived["warn"])
    glow = page.evaluate(
        "(sel) => getComputedStyle(document.querySelector(sel)).getPropertyValue('--glow-40')",
        preview,
    )
    assert "0, 163, 224" in glow  # glows follow the previewed accent inside the container
    assert page.locator(f"{preview} canvas").is_visible()  # the example chart
    chart = page.evaluate(
        """(sel) => {
          const c = document.querySelector(sel);
          const d = c.getContext('2d').getImageData(0, 0, c.width, c.height).data;
          const seen = new Set();
          for (let i = 0; i < d.length; i += 4) seen.add(d[i] + ',' + d[i + 1] + ',' + d[i + 2]);
          return [...seen];
        }""",
        f"{preview} canvas",
    )
    assert "0,163,224" in chart and "255,176,0" not in chart  # drawn in the previewed accent

    # The rest of the app keeps the saved (default amber) palette.
    assert page.evaluate(COLOUR, ".app-header .brand__mark") == AMBER
    assert page.evaluate(ROOT_TOKEN, "--accent") == "#FFB000"
    assert page.evaluate("() => FP.tokens().accent") == "#FFB000"
    save_fill = "() => getComputedStyle(document.getElementById('save-theme')).backgroundColor"
    assert page.evaluate(save_fill) == AMBER
    assert not (site["root"] / "theme.toml").exists()
    assert watch.clean(), watch


def test_preview_shows_the_name(page: Any, site: dict[str, Any]) -> None:
    watch = _appearance(page, site["url"])
    page.fill("#theme-name", "<b>Team</b>")
    page.wait_for_function(
        "() => document.querySelector('#theme-preview .brand__team')?.textContent"
        " === '<b>Team</b> · 6829'"
    )
    assert page.locator("#theme-preview b").count() == 0
    assert page.inner_text(".app-header .brand__team") == "6829"  # not saved, not shown
    assert watch.clean(), watch


def test_refused_in_the_view(page: Any, site: dict[str, Any]) -> None:
    """Refused in the view: the validation message and the suggested colour; theme.toml is
    unchanged."""
    watch = _appearance(page, site["url"])
    page.select_option("#theme-palette", "custom")
    page.fill("#theme-primary", "#202020")
    suggested = theme.resolve_palette(
        theme.parse_theme_config({"palette": "custom", "custom": {"primary": "#202020"}})
    ).suggestion["primary"]
    page.wait_for_selector("#theme-check[data-ok='false']")
    assert "accent #202020" in page.inner_text("#theme-check")
    assert page.inner_text("#theme-use-primary").endswith(suggested)
    page.click("#save-theme")
    page.wait_for_selector("#msg-theme[role='alert']")
    message = page.inner_text("#msg-theme")
    assert "Not saved" in message and "palette refused" in message and suggested in message
    assert page.input_value("#theme-primary") == "#202020"  # the edit is kept
    assert not (site["root"] / "theme.toml").exists()

    page.click("#theme-use-primary")  # take the suggestion
    assert page.input_value("#theme-primary") == suggested
    page.wait_for_selector("#theme-check[data-ok='true']")
    assert not watch.dialogs and not watch.remote
    assert all("status of 400" in e for e in watch.errors), watch


def test_save_a_preset_and_reload(page: Any, site: dict[str, Any]) -> None:
    watch = _appearance(page, site["url"])
    page.fill("#theme-name", "Ignite Robotics")
    page.select_option("#theme-palette", "green")
    _previewed(page, "accent", theme.preset_tokens("green")["accent"])
    page.click("#save-theme")
    page.wait_for_selector("#msg-theme[role='status']")
    assert "Saved theme.toml" in page.inner_text("#msg-theme")
    assert "page load" in page.inner_text("#msg-theme")
    with (site["root"] / "theme.toml").open("rb") as f:
        assert tomllib.load(f) == {"team_name": "Ignite Robotics", "palette": "green"}
    page.reload()
    page.wait_for_selector("#cfg-theme #theme-palette")
    green = theme.preset_tokens("green")["accent"]
    assert page.evaluate(ROOT_TOKEN, "--accent") == green
    assert page.inner_text(".app-header .brand__team") == "Ignite Robotics · 6829"
    assert page.input_value("#theme-palette") == "green"
    assert page.input_value("#theme-name") == "Ignite Robotics"
    assert watch.clean(), watch


def test_keyboard_reaches_appearance(page: Any, site: dict[str, Any]) -> None:
    watch = _appearance(page, site["url"])
    page.select_option("#theme-palette", "custom")
    page.fill("#theme-primary", "#202020")
    page.wait_for_selector("#theme-use-primary")
    page.focus("#setup-tab-config")
    seen = []
    for _ in range(100):
        page.keyboard.press("Tab")
        seen.append(page.evaluate("() => document.activeElement.id"))
        if seen[-1] == "theme-use-primary":
            break
    else:
        pytest.fail(f"Tab never reached the suggestion: {seen}")
    for id_ in ("save-theme", "theme-name", "theme-number", "theme-palette", "theme-primary"):
        assert id_ in seen, seen
    page.keyboard.press("Enter")  # use the suggested colour
    page.wait_for_selector("#theme-check[data-ok='true']")
    for _ in range(100):
        page.keyboard.press("Shift+Tab")
        if page.evaluate("() => document.activeElement.id") == "save-theme":
            break
    else:
        pytest.fail("Shift+Tab never reached SAVE")
    page.keyboard.press("Enter")
    page.wait_for_selector("#msg-theme[role='status']")
    with (site["root"] / "theme.toml").open("rb") as f:
        assert tomllib.load(f)["palette"] == "custom"
    assert watch.clean(), watch


def test_read_only_when_shared(page: Any, tmp_path: Path) -> None:
    from tests.web.test_browser import NetworkServer

    server, root = _site(tmp_path, LakePaths(tmp_path / "lake"), NetworkServer)
    try:
        watch = _appearance(page, f"http://127.0.0.1:{server.server_address[1]}/")
        assert page.locator("#save-theme").count() == 0
        assert page.locator("#theme-palette").is_disabled()
        assert page.input_value("#theme-palette") == "amber"
        assert watch.clean(), watch
    finally:
        _stop(server)


def test_leaving_setup_mid_preview_is_quiet(page: Any, site: dict[str, Any]) -> None:
    """An edit schedules a preview; switching views before it runs must not throw."""
    watch = _appearance(page, site["url"])
    page.evaluate(
        """() => {
          const select = document.getElementById('theme-palette');
          select.value = 'blue';
          select.dispatchEvent(new Event('change'));
          location.hash = '#view=history';
        }"""
    )
    page.wait_for_function("() => !document.getElementById('theme-palette')")
    page.wait_for_timeout(400)  # well past the preview delay
    assert watch.clean(), watch
