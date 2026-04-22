from __future__ import annotations

import re
import tomllib
from pathlib import Path

_POS_PREFIX = re.compile(r"^(FL|FR|BL|BR)\s+", re.IGNORECASE)
_TRAILING_NUM = re.compile(r"\s+\d+$")


def func_sort_key(name: str) -> tuple[str, str]:
    """Sort key grouping motors by function: strip leading position prefix and trailing numbers."""
    stem = _POS_PREFIX.sub("", name)
    stem = _TRAILING_NUM.sub("", stem)
    return (stem, name)


def load(path: Path) -> dict[str, dict[str, str]]:
    """Load a motor names TOML. All section values must be strings."""
    with open(path, "rb") as f:
        return tomllib.load(f)  # type: ignore[return-value]


def resolve(match_id: str, config: dict[str, dict[str, str]]) -> dict[str, str]:
    result = {f"TalonFX-{k}": v for k, v in config.get("default", {}).items()}
    competition_code = match_id.split("_")[0]
    match_key = max(
        (key for key in config if key != "default"
         and (match_id.startswith(key) or competition_code.endswith(key))),
        key=len,
        default=None,
    )
    if match_key is not None:
        result.update({f"TalonFX-{k}": v for k, v in config[match_key].items()})
    return result
