"""Theme delivery (team-branding design D-d): `GET /theme.css` and the team in `mode.js`."""

import json
import re
from pathlib import Path
from typing import Any

import pytest

from flashpoint import theme
from flashpoint.web import branding
from tests.report.lake import MatchLake

_DECL = re.compile(r"^\s*--([a-z0-9-]+):\s*([^;]+);\s*$")


def _config(tmp_path: Path, theme_toml: str | None = None, team: int | None = 6829) -> Path:
    root = tmp_path / "cfg"
    root.mkdir(exist_ok=True)
    if team is not None:
        (root / "acquire.toml").write_text(f"team = {team}\n")
    if theme_toml is not None:
        (root / "theme.toml").write_text(theme_toml)
    return root


def _declarations(css: str) -> dict[str, str]:
    """The `--name: value;` lines of one `:root { ... }` block; anything else fails."""
    body = css.strip()
    assert body.startswith(":root {") and body.endswith("}"), css
    found = {}
    for line in body.removeprefix(":root {").removesuffix("}").strip().splitlines():
        match = _DECL.match(line)
        assert match, f"not a colour token declaration: {line!r}"
        found[match.group(1)] = match.group(2).strip()
    return found


# --- theme.css ------------------------------------------------------------------------------


def test_theme_css_holds_the_colour_tokens_only(tmp_path: Path) -> None:
    root = _config(tmp_path, 'team_name = "Ignite Robotics"\npalette = "green"\n')
    css = branding.load_branding(root).css
    assert _declarations(css) == theme.preset_tokens("green")
    assert "Ignite" not in css  # the name goes into mode.js, as text


def test_default_theme_css_is_todays_amber(tmp_path: Path) -> None:
    css = branding.load_branding(_config(tmp_path)).css
    assert _declarations(css) == theme.AMBER_TOKENS


@pytest.mark.parametrize(
    "toml",
    [
        'palette = "amber"\ncolour = "red"\n',  # unknown key (spec: Broken theme file)
        'palette = "custom"\n[custom]\nprimary = "blue"\n',  # bad colour
        'palette = "custom"\n[custom]\nprimary = "#202020"\n',  # refused palette
        "palette = [\n",  # not TOML
    ],
)
def test_broken_theme_gives_empty_css_and_no_team(tmp_path: Path, toml: str) -> None:
    found = branding.load_branding(_config(tmp_path, 'team_name = "X"\n' + toml))
    assert found.css == ""
    assert found.team is None
    assert found.error is not None and "theme.toml" in found.error


def test_broken_theme_names_the_unknown_key(tmp_path: Path) -> None:
    found = branding.load_branding(_config(tmp_path, 'palette = "amber"\ncolour = "red"\n'))
    assert found.error is not None and "colour" in found.error


def test_cached_by_file_hash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _config(tmp_path, 'palette = "green"\n')
    calls: list[Any] = []
    real = theme.resolve_palette

    def counting(config: theme.ThemeConfig) -> theme.PaletteResult:
        calls.append(config)
        return real(config)

    monkeypatch.setattr(theme, "resolve_palette", counting)
    branding.clear_cache()
    first = branding.load_branding(root)
    assert branding.load_branding(root) == first
    assert len(calls) == 1  # same bytes: no second derivation
    (root / "theme.toml").write_text('palette = "blue"\n')
    changed = branding.load_branding(root)
    assert len(calls) == 2
    assert _declarations(changed.css) == theme.preset_tokens("blue")


# --- the team in mode.js --------------------------------------------------------------------


def test_team_name_and_number(tmp_path: Path) -> None:
    found = branding.load_branding(_config(tmp_path, 'team_name = "Ignite Robotics"\n'))
    assert found.team == {"name": "Ignite Robotics", "number": 6829}  # number from acquire


def test_theme_number_overrides_acquire(tmp_path: Path) -> None:
    found = branding.load_branding(_config(tmp_path, "team_number = 254\n"))
    assert found.team == {"name": None, "number": 254}


def test_no_name_and_no_team(tmp_path: Path) -> None:
    assert branding.load_branding(_config(tmp_path, team=None)).team is None


def test_broken_acquire_file_gives_no_number(tmp_path: Path) -> None:
    root = _config(tmp_path, 'team_name = "Ignite Robotics"\n', team=None)
    (root / "acquire.toml").write_text("team = 0\n")
    assert branding.load_branding(root).team == {"name": "Ignite Robotics", "number": None}


def test_mode_js_is_a_json_assignment_with_the_team() -> None:
    team = {"name": "</script><b>x </b>", "number": 6829}
    text = branding.mode_js({"static": False, "raw": True, "team": team})
    assert text.startswith("window.FP_MODE = ") and text.endswith(";\n")
    assert "</" not in text and " " not in text
    payload = json.loads(text.removeprefix("window.FP_MODE = ").removesuffix(";\n"))
    assert payload == {"static": False, "raw": True, "team": team}


# --- served -------------------------------------------------------------------------------


def _served(serve: Any, tmp_path: Path, config_dir: Path) -> Any:
    lake = MatchLake(tmp_path / "lake")
    lake.write_meta()
    app = serve(lake.lake)
    app.server.config_dir = config_dir
    return app


def test_get_theme_css(serve: Any, tmp_path: Path) -> None:
    app = _served(serve, tmp_path, _config(tmp_path, 'palette = "green"\n'))
    reply = app.get("/theme.css")
    assert reply.status == 200
    assert reply.headers["Content-Type"].startswith("text/css")
    assert reply.headers["Cache-Control"] == "no-store"  # a saved theme shows on next load
    assert _declarations(reply.body.decode()) == theme.preset_tokens("green")
    assert app.get("/theme.css", "HEAD").body == b""


def test_get_theme_css_broken_theme_is_empty(serve: Any, tmp_path: Path) -> None:
    app = _served(serve, tmp_path, _config(tmp_path, "nope = 1\n"))
    reply = app.get("/theme.css")
    assert reply.status == 200 and reply.body == b""


def test_served_mode_js_carries_the_team(serve: Any, tmp_path: Path) -> None:
    root = _config(tmp_path, 'team_name = "Ignite Robotics"\n')
    app = _served(serve, tmp_path, root)
    reply = app.get("/static/mode.js")
    assert reply.status == 200 and reply.headers["Content-Type"].startswith("text/javascript")
    assert reply.headers["Cache-Control"] == "no-store"
    payload = json.loads(reply.body.decode().removeprefix("window.FP_MODE = ").rstrip(";\n"))
    assert payload == {
        "static": False,
        "raw": True,
        "team": {"name": "Ignite Robotics", "number": 6829},
    }


def test_index_links_theme_css_right_after_tokens_css() -> None:
    from flashpoint.web.server import STATIC_DIR

    index = (STATIC_DIR / "index.html").read_text()
    links = re.findall(r'<link rel="stylesheet" href="([^"]+)">', index)
    assert links.index("theme.css") == links.index("static/tokens.css") + 1
