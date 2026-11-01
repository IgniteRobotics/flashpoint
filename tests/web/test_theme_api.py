"""The Setup view's theme endpoints (team-branding design D-f): `POST /api/theme/preview`, a
guarded read-only computation, and `PUT /api/config/theme` through the config-editor save."""

import json
import shutil
import tomllib
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config, theme
from flashpoint.lake.paths import LakePaths
from flashpoint.web.server import FlashpointServer
from tests.web.test_config_api import _put, _tree_hash
from tests.web.test_setup_api import _headers

PREVIEW = "/api/theme/preview"


@pytest.fixture
def root(tmp_path: Path) -> Path:
    target = tmp_path / "cfg"
    shutil.copytree(config.config_root(), target)
    return target


@pytest.fixture
def app(serve: Any, tmp_path: Path, root: Path) -> Any:
    running = serve(LakePaths(tmp_path / "lake"))
    server: FlashpointServer = running.server
    server.config_dir = root
    return running


def _preview(app: Any, body: Any, **overrides: str | None) -> tuple[int, Any]:
    data = json.dumps(body).encode()
    reply = app.get(PREVIEW, "POST", data, _headers(app, **overrides))
    return reply.status, json.loads(reply.body)


def _section(app: Any) -> dict[str, Any]:
    section: dict[str, Any] = app.json("/api/config")[1]["sections"]["theme"]
    return section


def _theme(root: Path) -> dict[str, Any]:
    with (root / "theme.toml").open("rb") as f:
        return tomllib.load(f)


# --- POST /api/theme/preview ------------------------------------------------------------------


def test_preview_a_custom_primary(app: Any, root: Path) -> None:
    before = _tree_hash(root)
    body = {"palette": "custom", "custom": {"primary": "#54CCFF"}}
    status, reply = _preview(app, body)
    expected = theme.resolve_palette(theme.parse_theme_config(body))
    assert status == 200 and reply["ok"] is True and reply["failures"] == []
    assert reply["tokens"] == expected.tokens and reply["tokens"]["accent"] == "#54CCFF"
    assert set(reply["tokens"]) == set(theme.TOKEN_NAMES)
    assert _tree_hash(root) == before  # writes nothing


@pytest.mark.parametrize("name", theme.PRESET_NAMES)
def test_preview_a_preset(app: Any, name: str) -> None:
    status, reply = _preview(app, {"palette": name})
    assert status == 200 and reply["ok"] is True
    assert reply["tokens"] == theme.preset_tokens(name)


def test_preview_too_dark_on_dark(app: Any, root: Path) -> None:
    before = _tree_hash(root)
    status, reply = _preview(app, {"palette": "custom", "custom": {"primary": "#202020"}})
    assert status == 200 and reply["ok"] is False
    accent = next(f for f in reply["failures"] if f["token"] == "accent")
    assert accent["rule"] == theme.RULE_TEXT and accent["ratio"] < 4.5
    assert accent["colour"] == "#202020" and accent["against"] in theme.SURFACES
    suggested = reply["suggestion"]["primary"]
    assert theme.contrast(suggested, theme.DEFAULT_BACKGROUND) > 4.5
    assert suggested in reply["message"] and "accent" in reply["message"]
    assert reply["tokens"]["accent"] == "#202020"  # still shown, so the preview can show why
    assert _tree_hash(root) == before


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        ({"palette": "custom", "custom": {"primary": "blue"}}, "#RRGGBB"),
        ({"palette": "purple"}, "palette"),
        ({"palette": "custom"}, "primary"),
        ({"palette": "amber", "colour": "#FFFFFF"}, "colour"),
        ({"team_name": "x" * 41}, "40 characters"),
        ([], "JSON object"),
    ],
)
def test_preview_refuses_bad_input(app: Any, body: Any, reason: str) -> None:
    status, reply = _preview(app, body)
    assert status == 400 and reply["ok"] is False and reason in reply["reason"]


def test_preview_is_guarded(app: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    body = {"palette": "green"}
    status, reply = _preview(app, body, **{"X-Flashpoint": None})
    assert status == 403 and reply["ok"] is False and "X-Flashpoint" in reply["reason"]
    status, reply = _preview(app, body, Origin="http://evil.example")
    assert status == 403 and "cross-origin" in reply["reason"]
    monkeypatch.setattr(FlashpointServer, "bound_local", property(lambda self: False))
    assert _preview(app, body)[0] == 403


def test_preview_is_post_only(app: Any) -> None:
    assert app.get(PREVIEW).status == 404
    assert app.get(PREVIEW, "PUT", b"{}", _headers(app)).status == 405


# --- the theme section and PUT /api/config/theme ---------------------------------------------


def test_theme_section(app: Any) -> None:
    section = _section(app)
    assert section["source"] == "theme.toml" and section["default"] is True
    assert section["data"] == {"palette": "amber"} and section["error"] is None
    assert section["presets"] == list(theme.PRESET_NAMES)
    assert len(section["sha256"]) == 64 and "page load" in section["affects"]


def test_save_a_custom_palette(app: Any, root: Path) -> None:
    section = _section(app)
    values = {
        "team_name": "Ignite Robotics",
        "team_number": None,
        "palette": "custom",
        "custom": {"primary": "#54CCFF", "background": None, "warning": None},
    }
    status, body = _put(app, "theme", {"sha256": section["sha256"], "values": values})
    assert status == 200 and body["saved"] and body["path"] == "theme.toml"
    assert body["backup"] is None  # the bundled default was in force: nothing of the team's
    assert _theme(root) == {
        "team_name": "Ignite Robotics",
        "palette": "custom",
        "custom": {"primary": "#54CCFF"},
    }
    assert "# team_number" in (root / "theme.toml").read_text()  # comments kept
    section = _section(app)
    assert section["default"] is False and section["sha256"] == body["sha256"]

    values = {"team_name": "Ignite Robotics", "palette": "green", "custom": None}
    status, body = _put(app, "theme", {"sha256": section["sha256"], "values": values})
    assert status == 200 and body["backup"]  # the team's earlier copy is kept
    assert _theme(root) == {"team_name": "Ignite Robotics", "palette": "green"}


def test_refused_palette_is_not_saved(app: Any, root: Path) -> None:
    """Refused in the view: the validation message and the suggested colour; nothing written."""
    section = _section(app)
    values = {"palette": "custom", "custom": {"primary": "#202020"}}
    before = _tree_hash(root)
    status, body = _put(app, "theme", {"sha256": section["sha256"], "values": values})
    assert status == 400 and body["saved"] is False
    reason = body["reason"]
    assert reason.startswith("theme.toml: ") and "palette refused" in reason
    assert "accent #202020" in reason and "nearest primary that passes: #" in reason
    assert _tree_hash(root) == before and not (root / "theme.toml").exists()


@pytest.mark.parametrize(
    ("values", "reason"),
    [
        ({"team_name": "x" * 41}, "40 characters"),
        ({"palette": "custom", "custom": {"primary": "blue"}}, "#RRGGBB"),
        ({"colour": "#FFFFFF"}, "colour"),
        ({"palette": "purple"}, "palette"),
    ],
)
def test_bad_values_are_refused(app: Any, root: Path, values: Any, reason: str) -> None:
    status, body = _put(app, "theme", {"sha256": _section(app)["sha256"], "values": values})
    assert status == 400 and reason in body["reason"]
    assert not (root / "theme.toml").exists()


def test_theme_save_conflict_and_guard(app: Any, root: Path) -> None:
    section = _section(app)
    (root / "theme.toml").write_text('palette = "red"\n')
    payload = {"sha256": section["sha256"], "values": {"palette": "blue"}}
    status, body = _put(app, "theme", payload)
    assert status == 409 and "theme.toml" in body["reason"]
    payload["sha256"] = _section(app)["sha256"]
    status, body = _put(app, "theme", payload, **{"X-Flashpoint": None})
    assert status == 403 and _theme(root) == {"palette": "red"}
    assert _put(app, "theme/extra", payload)[0] == 404


@pytest.mark.parametrize("body", [[], {"values": "x"}, {"values": []}])
def test_malformed_theme_bodies(app: Any, body: Any) -> None:
    assert _put(app, "theme", body)[0] == 400


def test_saving_repairs_an_unknown_key(app: Any, root: Path) -> None:
    """Broken theme file: the Appearance section's save drops a key theme.toml doesn't know,
    so the team can fix the file from Setup (a key sent in the values is still refused)."""
    (root / "theme.toml").write_text('palette = "amber"\ncolour = "x"  # typo\n')
    section = _section(app)
    assert section["error"] and "colour" in section["error"]
    values = {"team_name": None, "team_number": None, "palette": "blue", "custom": None}
    status, body = _put(app, "theme", {"sha256": section["sha256"], "values": values})
    assert status == 200, body
    assert _theme(root) == {"palette": "blue"}


def test_unreadable_theme_file_is_refused_with_a_reason(app: Any, root: Path) -> None:
    (root / "theme.toml").write_text("palette = [\n")
    values = {"palette": "blue"}
    status, body = _put(app, "theme", {"sha256": _section(app)["sha256"], "values": values})
    assert status == 400 and body["saved"] is False and "theme.toml" in body["reason"]
    assert (root / "theme.toml").read_text() == "palette = [\n"


@pytest.mark.parametrize("section", ["theme", "team"])
def test_values_tomlkit_cannot_write_are_refused(app: Any, root: Path, section: str) -> None:
    sha = app.json("/api/config")[1]["sections"][section]["sha256"]
    before = _tree_hash(root)
    status, body = _put(app, section, {"sha256": sha, "values": {"team_name": [1, None]}})
    assert status == 400 and body["saved"] is False and "team_name" in body["reason"]
    assert _tree_hash(root) == before
