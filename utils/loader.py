from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
HEX32_PATTERN = re.compile(r"[0-9A-Fa-f]{32}")
MOTOR_COL_PATTERN = re.compile(
    r"Phoenix6/TalonFX-(\d+)/(MotorVoltage|StatorCurrent|SupplyVoltage|SupplyCurrent|Velocity)"
)


def extract_match_id(path: Path) -> str:
    """Extract match ID from a CSV filename.

    Handles these filename patterns:
    - COMPETITIONID_MATCH_CANBUSID_suffix  (where CANBUSID is a UUID or 32-char hex)
    - COMPETITIONID_MATCH-canbusname
    - COMPETITIONID_MATCH_canbusname
    """
    stem = path.stem
    # Strip known trailing suffixes (e.g. _filtered, _raw)
    stem = re.sub(r"[_-](filtered|raw)$", "", stem, flags=re.IGNORECASE)
    # Strip trailing date/time stamp (e.g. _2026-03-21_20-40-47)
    stem = re.sub(r"[_-]\d{4}-\d{2}-\d{2}.*$", "", stem)
    # Try dashed UUID first
    uuid_match = UUID_PATTERN.search(stem)
    if uuid_match:
        return stem[: uuid_match.start()].rstrip("-_")
    # Try 32-char undashed hex device ID
    hex_match = HEX32_PATTERN.search(stem)
    if hex_match:
        return stem[: hex_match.start()].rstrip("-_")
    # Fallback: everything before the last separator
    parts = re.split(r"[-_]", stem)
    return "_".join(parts[:-1]) if len(parts) > 1 else stem


def find_matches(paths: list[Path]) -> dict[str, list[Path]]:
    """Group CSV files by match ID."""
    groups: dict[str, list[Path]] = {}
    for path in paths:
        match_id = extract_match_id(path)
        groups.setdefault(match_id, []).append(path)
    return groups


def load_match(files: list[Path]) -> pd.DataFrame:
    """Load and outer-join all CAN bus CSVs for a match into one DataFrame."""
    dfs = [pd.read_csv(f, na_values=["null"]) for f in files]
    if len(dfs) == 1:
        return dfs[0].sort_values("Timestamp").reset_index(drop=True)
    merged = dfs[0]
    for df in dfs[1:]:
        merged = pd.merge(merged, df, on="Timestamp", how="outer")
    return merged.sort_values("Timestamp").reset_index(drop=True)


def get_motor_ids(df: pd.DataFrame) -> list[str]:
    """Return sorted list of unique TalonFX motor IDs found in DataFrame columns."""
    ids: set[str] = set()
    for col in df.columns:
        m = MOTOR_COL_PATTERN.match(col)
        if m:
            ids.add(f"TalonFX-{m.group(1)}")
    return sorted(ids, key=lambda x: int(x.split("-")[1]))
