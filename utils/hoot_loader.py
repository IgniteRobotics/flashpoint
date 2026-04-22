from __future__ import annotations

import hashlib
import platform
from pathlib import Path

import pandas as pd

from .loader import MOTOR_COL_PATTERN


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


def _pivot_records(records) -> pd.DataFrame:  # type: ignore[type-arg]
    """Walk a DataLogReader iterable, keep only TalonFX double entries, pivot to wide format."""
    entries: dict[int, str] = {}
    data: dict[float, dict[str, float]] = {}

    for record in records:
        if record.isStart():
            try:
                sd = record.getStartData()
                if MOTOR_COL_PATTERN.match(sd.name) and sd.type == "double":
                    entries[sd.entry] = sd.name
            except TypeError:
                pass
        elif record.isFinish() or record.isSetMetadata() or record.isControl():
            continue
        else:
            col = entries.get(record.entry)
            if col is None:
                continue
            try:
                ts_s = record.timestamp * 1e-6
                # Later value for the same (timestamp, channel) pair wins; log collisions are rare.
                data.setdefault(ts_s, {})[col] = record.getDouble()
            except TypeError:
                pass

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(data, orient="index")
    df.index.name = "Timestamp"
    return df.sort_index().reset_index()


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
