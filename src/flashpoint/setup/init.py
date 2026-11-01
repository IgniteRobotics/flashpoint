"""`flashpoint init`: create a configuration root from the bundled defaults."""

import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import tomlkit

from flashpoint.acquire.config import CONFIG_FILE
from flashpoint.config import DEFAULTS_ROOT
from flashpoint.setup.checks import describe_error
from flashpoint.theme import (
    DEFAULT_PALETTE,
    PRESET_NAMES,
    THEME_FILE,
    ThemeError,
    parse_theme_config,
)

TEAM_MIN, TEAM_MAX = 1, 99999
# Bundled files a team is expected to edit; copied so the copy is theirs.
EDITABLE_DEFAULTS = (
    "report.toml",
    "anomaly.toml",
    "anomaly-causes.toml",
    "seasons/2025.toml",
    "seasons/2026.toml",
    "units/units-seed.csv",
)


class InitError(ValueError):
    """A bad team number or lake folder; nothing was written."""


@dataclass
class InitResult:
    root: Path
    created: list[Path] = field(default_factory=list)
    existing: list[Path] = field(default_factory=list)  # set when a configuration already exists


def check_team(team: int) -> int:
    if not TEAM_MIN <= team <= TEAM_MAX:
        raise InitError(f"team number must be a whole number from {TEAM_MIN} to {TEAM_MAX}")
    return team


def parse_team(text: str) -> int:
    try:
        return check_team(int(text.strip()))
    except ValueError as exc:
        if isinstance(exc, InitError):
            raise
        raise InitError(
            f"team number must be a whole number from {TEAM_MIN} to {TEAM_MAX}, not {text!r}"
        ) from exc


def parse_palette(text: str) -> str:
    """A preset name (blank: the default); `init` offers presets only, not custom colours."""
    name = text.strip().lower() or DEFAULT_PALETTE
    if name not in PRESET_NAMES:
        raise InitError(f"palette must be one of {', '.join(PRESET_NAMES)}, not {text.strip()!r}")
    return name


def theme_toml(team_name: str | None, palette: str) -> str:
    """The bundled theme.toml with the team's name and preset set; its comments are kept, so
    with no name and the default preset it is the bundled file byte for byte."""
    name = (team_name or "").strip() or None
    data: dict[str, Any] = {"palette": palette} | ({"team_name": name} if name else {})
    try:
        parse_theme_config(data)
    except ThemeError as exc:
        raise InitError(describe_error(exc)) from exc
    text = (DEFAULTS_ROOT / THEME_FILE).read_text(encoding="utf-8")
    text = _set_line(text, r'^palette = "[^"]*"', f"palette = {tomlkit.item(palette).as_string()}")
    if name:
        quoted = tomlkit.item(name).as_string()
        text = _set_line(text, r'^# team_name = "[^"]*"', f"team_name = {quoted}")
    return text


def _set_line(text: str, pattern: str, replacement: str) -> str:
    new, count = re.subn(pattern, lambda _: replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:  # the bundled file lost its line: put the key first instead
        return f"{replacement}\n{text}"
    return new


def default_lake() -> Path:
    return Path.home() / "flashpoint-lake"


def _acquire_toml(team: int, lake: Path) -> str:
    # json.dumps gives a TOML basic string: quotes and backslashes (Windows paths) are escaped.
    return (
        "# Written by `flashpoint init`. Robot addresses come from the team number\n"
        "# (10.TE.AM.2, roborio-TEAM-frc.local, then USB); set `hosts` to replace them.\n"
        f"team = {team}\n"
        f"lake = {json.dumps(str(lake))}\n"
    )


def existing_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file() and ".backups" not in p.parts)


def init_config(
    root: Path,
    team: int,
    lake: Path,
    team_name: str | None = None,
    palette: str = DEFAULT_PALETTE,
) -> InitResult:
    """Write a starting configuration; never overwrite. With `acquire.toml` already present,
    change nothing and report the files that exist. `theme.toml` gets the team's display name
    and colour preset (team-branding)."""
    check_team(team)
    theme = theme_toml(team_name, parse_palette(palette))
    result = InitResult(root)
    if (root / CONFIG_FILE).is_file():
        result.existing = existing_files(root)
        return result
    lake = lake.expanduser().absolute()
    try:
        lake.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise InitError(f"cannot create the lake folder {lake}: {exc}") from exc
    (root / "robots").mkdir(parents=True, exist_ok=True)
    for name in EDITABLE_DEFAULTS:
        target = root / name
        if target.exists():
            result.existing.append(target)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(DEFAULTS_ROOT / name, target)
        result.created.append(target)
    theme_path = root / THEME_FILE
    if theme_path.exists():
        result.existing.append(theme_path)
    else:
        theme_path.write_text(theme, encoding="utf-8")
        result.created.append(theme_path)
    acquire = root / CONFIG_FILE
    acquire.write_text(_acquire_toml(team, lake), encoding="utf-8")
    result.created.insert(0, acquire)
    return result
