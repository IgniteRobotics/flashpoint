"""Paths and versioning shared across Flashpoint."""

import os
from pathlib import Path

# Bump whenever bronze or metadata output changes; `flashpoint rebuild` reprocesses older files.
PIPELINE_VERSION = 1

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
