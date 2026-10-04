"""Paths and versioning shared across Flashpoint."""

import os
from pathlib import Path

# Bump whenever bronze or metadata output changes; `flashpoint rebuild` reprocesses older files.
PIPELINE_VERSION = 2  # v2: health profile adds DriveState/Pose, RotorVelocity, motor constants

LAKE_ENV = "FLASHPOINT_LAKE"
CACHE_ENV = "FLASHPOINT_CACHE"


def lake_root(explicit: Path | None) -> Path:
    """Lake root: explicit argument, then $FLASHPOINT_LAKE, then ~/flashpoint-lake."""
    if explicit is not None:
        return explicit
    env = os.environ.get(LAKE_ENV)
    return Path(env) if env else Path.home() / "flashpoint-lake"


def cache_root() -> Path:
    """Cache root for downloaded tools: $FLASHPOINT_CACHE or ~/.cache/flashpoint."""
    env = os.environ.get(CACHE_ENV)
    return Path(env) if env else Path.home() / ".cache" / "flashpoint"


CONFIG_ENV = "FLASHPOINT_CONFIG"
_REPO_CONFIG = Path(__file__).resolve().parents[2] / "config"


def config_root(explicit: Path | None = None) -> Path:
    """Robot configuration root: explicit, $FLASHPOINT_CONFIG, repo config/, else ~/.config."""
    if explicit is not None:
        return explicit
    env = os.environ.get(CONFIG_ENV)
    if env:
        return Path(env)
    if _REPO_CONFIG.is_dir():
        return _REPO_CONFIG
    return Path.home() / ".config" / "flashpoint"
