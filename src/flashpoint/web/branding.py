"""The team's theme as the browser gets it (team-branding design D-d): `theme.css`, a `:root`
block of colour tokens that overrides `tokens.css`, and the team for `window.FP_MODE.team`.

The server and the static export share this. Results are cached by the bytes of `theme.toml`
and `acquire.toml`, so a request re-derives only after a file changes. A theme that can't be
used gives an empty `theme.css` (the `tokens.css` defaults apply) and no team; the setup checks
report why.
"""

import hashlib
import json
import threading
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from flashpoint import theme
from flashpoint.acquire.config import CONFIG_FILE as ACQUIRE_FILE
from flashpoint.acquire.config import AcquireConfig
from flashpoint.config import config_file
from flashpoint.setup.checks import describe_error

THEME_CSS = "theme.css"


@dataclass(frozen=True)
class Branding:
    css: str  # theme.css: the colour tokens, or "" when the theme can't be used
    team: dict[str, Any] | None  # {"name", "number"}; None when neither is set
    error: str | None = None  # why the theme can't be used, naming theme.toml


CACHE_SIZE = 16
_cache: dict[tuple[str, str], Branding] = {}
_lock = threading.Lock()


def clear_cache() -> None:
    with _lock:
        _cache.clear()


def _read(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except OSError:
        return None


def _digest(data: bytes | None) -> str:
    return hashlib.sha256(data).hexdigest() if data is not None else "-"


def theme_css(tokens: dict[str, str]) -> str:
    lines = [f"  --{name}: {tokens[name]};" for name in theme.TOKEN_NAMES]
    return ":root {\n" + "\n".join(lines) + "\n}\n"


def _acquire_team(data: bytes | None) -> int | None:
    if data is None:
        return None
    try:
        return AcquireConfig.model_validate(tomllib.loads(data.decode("utf-8"))).team
    except (UnicodeDecodeError, tomllib.TOMLDecodeError, ValidationError):
        return None  # doctor names the broken acquire.toml


def _build(theme_data: bytes | None, acquire_data: bytes | None) -> Branding:
    try:
        if theme_data is None:
            config = theme.ThemeConfig()
        else:
            try:
                parsed = tomllib.loads(theme_data.decode("utf-8"))
            except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
                raise theme.ThemeError(f"{theme.THEME_FILE}: {exc}") from exc
            config = theme.parse_theme_config(parsed)
        result = theme.resolve_palette(config)
        if not result.ok:
            raise theme.ThemeError(f"{theme.THEME_FILE}: {result.message}")
    except theme.ThemeError as exc:
        message = describe_error(exc)
        if not message.startswith(theme.THEME_FILE):
            message = f"{theme.THEME_FILE}: {message}"
        return Branding("", None, message)
    number = config.team_number if config.team_number is not None else _acquire_team(acquire_data)
    team = None
    if config.team_name is not None or number is not None:
        team = {"name": config.team_name, "number": number}
    return Branding(theme_css(result.tokens), team)


def load_branding(config_dir: Path) -> Branding:
    """The theme.css and team for this configuration, cached by the files' hashes."""
    theme_data = _read(config_file(config_dir, theme.THEME_FILE))
    acquire_data = _read(config_dir / ACQUIRE_FILE)
    key = (_digest(theme_data), _digest(acquire_data))
    with _lock:
        found = _cache.get(key)
    if found is None:
        found = _build(theme_data, acquire_data)
        with _lock:
            if len(_cache) >= CACHE_SIZE:
                _cache.clear()
            _cache[key] = found
    return found


def mode_js(mode: dict[str, Any]) -> str:
    """`window.FP_MODE = {...};`: JSON with every non-ASCII character escaped, and no `</`."""
    return "window.FP_MODE = " + json.dumps(mode).replace("</", "<\\/") + ";\n"
