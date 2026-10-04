"""Windows removable-volume detection through one PowerShell call that emits JSON."""

import json
import re
from pathlib import Path
from typing import Any

from flashpoint.acquire import volumes
from flashpoint.acquire.volumes import Volume, VolumeDetectionError

REMOVABLE_BUS_TYPES = frozenset({"USB", "SD", "MMC"})
POWERSHELL_SCRIPT = (
    "Get-Partition | Where-Object { $_.DriveLetter } | ForEach-Object { "
    "$disk = Get-Disk -Number $_.DiskNumber; "
    "$vol = Get-Volume -DriveLetter $_.DriveLetter; "
    "[pscustomobject]@{ DriveLetter = [string]$_.DriveLetter; BusType = [string]$disk.BusType; "
    "UniqueId = [string]$vol.UniqueId; FileSystemLabel = [string]$vol.FileSystemLabel } "
    "} | ConvertTo-Json -Compress"
)
_GUID = re.compile(r"Volume\{([0-9A-Fa-f-]+)\}")


def parse_powershell_json(text: str) -> list[Volume]:
    """Parse the script's JSON (a lone object or an array) into removable volumes."""
    if not text.strip():
        return []
    try:
        payload: Any = json.loads(text)
    except ValueError as exc:
        raise VolumeDetectionError(f"unreadable PowerShell JSON: {exc}") from exc
    rows = [payload] if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise VolumeDetectionError("unexpected PowerShell JSON shape")

    found: list[Volume] = []
    for row in rows:
        letter = str(row.get("DriveLetter") or "").strip()
        if not letter or str(row.get("BusType") or "").upper() not in REMOVABLE_BUS_TYPES:
            continue
        letter = letter[0].upper()
        label = str(row.get("FileSystemLabel") or "") or f"{letter}:"
        unique_id = str(row.get("UniqueId") or "")
        guid = _GUID.search(unique_id)
        found.append(
            Volume(
                id=guid.group(1) if guid else (unique_id or label),
                label=label,
                mount=Path(f"{letter}:\\"),
            )
        )
    return found


def detect_windows() -> list[Volume]:
    output = volumes.run_os_command(["powershell", "-NoProfile", "-Command", POWERSHELL_SCRIPT])
    return parse_powershell_json(output.decode("utf-8", "replace"))
