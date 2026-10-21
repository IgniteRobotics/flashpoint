"""Paths and versioning shared across Flashpoint."""

import os
import platform
from pathlib import Path

# Bump whenever bronze or metadata output changes; `flashpoint rebuild` reprocesses older files.
PIPELINE_VERSION = 2  # v2: health profile adds DriveState/Pose, RotorVelocity, motor constants
# Bump whenever silver or gold output changes; `flashpoint derive` recomputes every session
# (bronze is untouched, so no re-ingest).
DERIVE_VERSION = 5  # v5: supply path resistance; v4: gold/can_*; v3: silver/signals; v2: usage
# CAN-health gaps shorter than this are counted in gold but not kept as events, so no marker
# threshold may be lower.
CAN_EVENT_FLOOR_S = 0.1

LAKE_ENV = "FLASHPOINT_LAKE"
CACHE_ENV = "FLASHPOINT_CACHE"


def lake_root(explicit: Path | None) -> Path:
    """Lake root: explicit argument, then $FLASHPOINT_LAKE, then `lake` in acquire.toml, then
    ~/flashpoint-lake. An unreadable acquire.toml is skipped here; `doctor` reports it."""
    if explicit is not None:
        return explicit
    env = os.environ.get(LAKE_ENV)
    if env:
        return Path(env)
    from flashpoint.acquire.config import AcquireConfig, AcquireConfigError

    try:
        configured = AcquireConfig.load(config_root()).lake
    except AcquireConfigError:
        configured = None
    return configured.expanduser() if configured else Path.home() / "flashpoint-lake"


def cache_root() -> Path:
    """Cache root for downloaded tools: $FLASHPOINT_CACHE or ~/.cache/flashpoint."""
    env = os.environ.get(CACHE_ENV)
    return Path(env) if env else Path.home() / ".cache" / "flashpoint"


CONFIG_ENV = "FLASHPOINT_CONFIG"
# Shared starting configuration shipped in the package; a team's own file replaces it per file.
DEFAULTS_ROOT = Path(__file__).resolve().parent / "defaults"


def user_config_dir() -> Path:
    """The per-user configuration folder: %APPDATA%\\flashpoint on Windows, else
    $XDG_CONFIG_HOME/flashpoint or ~/.config/flashpoint (also on macOS, where students look)."""
    if platform.system() == "Windows":
        appdata = os.environ.get("APPDATA")
        return Path(appdata) / "flashpoint" if appdata else Path.home() / "flashpoint"
    xdg = os.environ.get("XDG_CONFIG_HOME")
    return Path(xdg) / "flashpoint" if xdg else Path.home() / ".config" / "flashpoint"


def config_root(explicit: Path | None = None) -> Path:
    """Configuration root: explicit, then $FLASHPOINT_CONFIG, then the per-user folder."""
    if explicit is not None:
        return explicit
    env = os.environ.get(CONFIG_ENV)
    if env:
        return Path(env)
    return user_config_dir()


def config_file(root: Path, name: str) -> Path:
    """`name` (relative) under the configuration root, else the bundled default."""
    path = root / name
    return path if path.is_file() else DEFAULTS_ROOT / name


def is_default(root: Path, name: str) -> bool:
    """True when `name` comes from the bundled defaults rather than the root."""
    return config_file(root, name) != root / name
