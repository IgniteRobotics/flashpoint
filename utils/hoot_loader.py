from __future__ import annotations

import hashlib
import platform
import subprocess
import sys
import tempfile
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


def _run_owlet(hoot_path: Path, wpilog_path: Path) -> None:
    exe = _owlet_path()
    if not exe.exists():
        print(f"error: owlet executable not found: {exe}", file=sys.stderr)
        sys.exit(1)
    result = subprocess.run(
        [str(exe), "-f", "wpilog", str(hoot_path), str(wpilog_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # Owlet often exits non-zero when a hoot file is truncated at the end (robot power-off
        # mid-recording). If the output file was still written, treat it as a warning and proceed.
        if wpilog_path.exists() and wpilog_path.stat().st_size > 0:
            print(
                f"  warning: owlet reported an error on {hoot_path.name} (truncated log?), "
                f"proceeding with partial output",
                file=sys.stderr,
            )
        else:
            print(
                f"error: owlet failed on {hoot_path.name}:\n{result.stderr}",
                file=sys.stderr,
            )
            sys.exit(1)


def _wpilog_to_df(path: Path) -> pd.DataFrame:
    import mmap
    from datalog import DataLogReader  # repo-root module, importable from project root

    with open(path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            return _pivot_records(DataLogReader(mm))


def convert_hoot(path: Path, cache_dir: Path) -> Path | None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_csv = cache_dir / f"{_file_hash(path)}.csv"

    if cache_csv.exists():
        try:
            pd.read_csv(cache_csv, nrows=1)
            return cache_csv
        except Exception:
            pass  # corrupt — fall through to re-convert

    with tempfile.NamedTemporaryFile(suffix=".wpilog", delete=False) as tmp:
        wpilog_path = Path(tmp.name)

    try:
        _run_owlet(path, wpilog_path)
        df = _wpilog_to_df(wpilog_path)
    finally:
        wpilog_path.unlink(missing_ok=True)

    if df.empty:
        print(
            f"warning: no TalonFX entries found in {path.name}, skipping",
            file=sys.stderr,
        )
        return None

    df.to_csv(cache_csv, index=False)
    return cache_csv
