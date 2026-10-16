"""Where Replay data lives in a lake (light: the server imports it without polars)."""

import re
from pathlib import Path

from flashpoint.lake.paths import LakePaths

_KEY = re.compile(r"^[0-9a-z_]+$")


def report_dir(lake: LakePaths) -> Path:
    return lake.root / "report"


def data_dir(lake: LakePaths) -> Path:
    return report_dir(lake) / "data"


def state_path(lake: LakePaths) -> Path:
    return report_dir(lake) / "site-state.json"


def file_stem(match_key: str) -> str:
    return match_key if _KEY.match(match_key) else re.sub(r"[^0-9a-z_]", "_", match_key.lower())
