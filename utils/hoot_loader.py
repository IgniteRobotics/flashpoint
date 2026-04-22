from __future__ import annotations

import hashlib
import platform
from pathlib import Path

import pandas as pd


_REPO_ROOT = Path(__file__).parent.parent


def _file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def _owlet_path() -> Path:
    name = {
        "Darwin": "owlet-2026-mac",
        "Windows": "owlet-2026-win.exe",
    }.get(platform.system(), "owlet-2026-linux")
    return _REPO_ROOT / "executables" / name


def convert_hoot(path: Path, cache_dir: Path) -> Path | None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_csv = cache_dir / f"{_file_hash(path)}.csv"

    if cache_csv.exists():
        try:
            pd.read_csv(cache_csv, nrows=1)
            return cache_csv
        except Exception:
            pass  # corrupt — fall through to re-convert

    return None  # conversion not yet implemented
