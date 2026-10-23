"""`flashpoint init`: create a configuration root from the bundled defaults."""

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from flashpoint.acquire.config import CONFIG_FILE
from flashpoint.config import DEFAULTS_ROOT

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


def init_config(root: Path, team: int, lake: Path) -> InitResult:
    """Write a starting configuration; never overwrite. With `acquire.toml` already present,
    change nothing and report the files that exist."""
    check_team(team)
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
    acquire = root / CONFIG_FILE
    acquire.write_text(_acquire_toml(team, lake), encoding="utf-8")
    result.created.insert(0, acquire)
    return result
