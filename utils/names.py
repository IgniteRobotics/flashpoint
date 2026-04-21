from __future__ import annotations

import tomllib
from pathlib import Path


def load(path: Path) -> dict[str, dict[str, str]]:
    with open(path, "rb") as f:
        return tomllib.load(f)


def resolve(match_id: str, config: dict[str, dict[str, str]]) -> dict[str, str]:
    result = {
        f"TalonFX-{k}": v
        for k, v in config.get("default", {}).items()
    }
    for key, overrides in config.items():
        if key != "default" and match_id.startswith(key):
            result.update({f"TalonFX-{k}": v for k, v in overrides.items()})
            break
    return result
