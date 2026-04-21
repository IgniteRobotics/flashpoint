from __future__ import annotations

import tomllib
from pathlib import Path


def load(path: Path) -> dict[str, dict[str, str]]:
    """Load a motor names TOML. All section values must be strings."""
    with open(path, "rb") as f:
        return tomllib.load(f)  # type: ignore[return-value]


def resolve(match_id: str, config: dict[str, dict[str, str]]) -> dict[str, str]:
    result = {f"TalonFX-{k}": v for k, v in config.get("default", {}).items()}
    match_key = max(
        (key for key in config if key != "default" and match_id.startswith(key)),
        key=len,
        default=None,
    )
    if match_key is not None:
        result.update({f"TalonFX-{k}": v for k, v in config[match_key].items()})
    return result
