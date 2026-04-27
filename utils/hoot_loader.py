from __future__ import annotations

import hashlib
import platform
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd


MOTOR_COL_PATTERN = re.compile(
    r"Phoenix6/TalonFX-(\d+)/(MotorVoltage|StatorCurrent|SupplyVoltage|SupplyCurrent|Velocity|DeviceTemp)"
)

_UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
_HEX32_PATTERN = re.compile(r"[0-9A-Fa-f]{32}")

_REPO_ROOT = Path(__file__).parent.parent


def get_motor_ids(df: pd.DataFrame) -> list[str]:
    """Return sorted list of unique TalonFX motor IDs found in DataFrame columns."""
    ids: set[str] = set()
    for col in df.columns:
        m = MOTOR_COL_PATTERN.match(col)
        if m:
            ids.add(f"TalonFX-{m.group(1)}")
    return sorted(ids, key=lambda x: int(x.split("-")[1]))


def extract_match_id(path: Path) -> str:
    """Extract match ID from a hoot filename."""
    stem = path.stem
    stem = re.sub(r"[_-](filtered|raw)$", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"[_-]\d{4}-\d{2}-\d{2}.*$", "", stem)
    stem = re.sub(r"[_-][0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"[_-][0-9A-Fa-f]{32}$", "", stem)
    stem = re.sub(r"[_-]rio$", "", stem, flags=re.IGNORECASE)
    return stem


def find_matches(paths: list[Path]) -> dict[str, list[Path]]:
    """Group hoot files by match ID."""
    groups: dict[str, list[Path]] = {}
    for path in paths:
        match_id = extract_match_id(path)
        groups.setdefault(match_id, []).append(path)
    return groups


def merge_dataframes(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Outer-join a list of DataFrames on Timestamp and sort."""
    if len(dfs) == 1:
        return dfs[0].sort_values("Timestamp").reset_index(drop=True)
    merged = dfs[0]
    for df in dfs[1:]:
        merged = pd.merge(merged, df, on="Timestamp", how="outer")
    return merged.sort_values("Timestamp").reset_index(drop=True)


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
            pass

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
