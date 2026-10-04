"""One-time conversion of the legacy per-season device CSVs into a robot configuration."""

import csv
import re
from pathlib import Path

_DEVICE = re.compile(r"^(?P<prefix>Phoenix6|Pheonix6)/(?P<model>[A-Za-z0-9]+)-(?P<id>\d+)/")


def _slug(*parts: str) -> str:
    words = " ".join(p for p in parts if p).lower().replace("_", " ").split()
    return "-".join(words)


def migrate_device_maps(
    robot: str,
    season: int,
    project: str,
    rio_map: Path,
    canivore_map: Path,
    nt_maps: list[Path],
) -> tuple[str, list[str]]:
    """Return (robot TOML text, human-readable report lines)."""
    report: list[str] = []
    slots: dict[tuple[str, str, int], dict[str, str | int]] = {}
    for bus, path in (("rio", rio_map), ("canivore", canivore_map)):
        with path.open(newline="") as f:
            for row in csv.DictReader(f):
                match = _DEVICE.match(row["entry"])
                if not match:
                    report.append(f"unmapped {path.name}: {row['entry']} (not a device signal)")
                    continue
                if match["prefix"] == "Pheonix6":
                    report.append(f"corrected {path.name}: {row['entry']} (Pheonix6 -> Phoenix6)")
                key = (bus, match["model"], int(match["id"]))
                if key not in slots:
                    location = (row["assembly"], row["subassembly"], row["component"])
                    slots[key] = {
                        "id": _slug(row["subsystem"], *location) + f"-{match['id']}",
                        "bus": bus,
                        "model": match["model"],
                        "can_id": int(match["id"]),
                        "subsystem": row["subsystem"].lower().replace("_", " "),
                        "role": _slug(*location).replace("-", " ") or "motor",
                    }
    for path in nt_maps:
        with path.open(newline="") as f:
            for row in csv.DictReader(f):
                report.append(
                    f"unmapped {path.name}: {row['entry']} (NetworkTables signal, not a CAN slot)"
                )

    lines = [
        f"# Migrated from legacy datamaps by tools/migrate-legacy-config.py ({len(slots)} slots).",
        f'robot = "{robot}"',
        f"season = {season}",
        f'project = "{project}"',
    ]
    for slot in sorted(slots.values(), key=lambda s: (str(s["bus"]), int(s["can_id"]))):
        lines.append("\n[[slot]]")
        lines += [f'{k} = "{v}"' if isinstance(v, str) else f"{k} = {v}" for k, v in slot.items()]
    return "\n".join(lines) + "\n", report
