"""One-time conversion of the legacy per-season CSVs and log configuration into robot and
season configuration."""

import csv
import json
import re
from pathlib import Path

from flashpoint.readers.wpilog import WpilogReader
from flashpoint.semantics.robot_config import Slot

_DEVICE = re.compile(r"^(?P<prefix>Phoenix6|Pheonix6)/(?P<model>[A-Za-z0-9]+)-(?P<id>\d+)/")
# Legacy log-config key -> season root name.
ROOT_KEYS = {
    "metrics_prefix": "robot",
    "photon_prefix": "photonvision",
    "camerapub_prefix": "camera_publisher",
    "preferences_prefix": "preferences",
    "fms_prefix": "fms",
}
NUMERIC_TYPES = {"double", "float", "int64", "boolean"}
# Legacy motor metrics a CAN slot already carries (Phoenix 6 status signals).
MOTOR_METRICS = {"VOLTAGE", "CURRENT", "TEMP", "POSITION", "VELOCITY"}
NOT_IN_LOG = "not in reference log"
NON_NUMERIC = "non-numeric in reference log"


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


def _snake(text: str) -> str:
    """`targetPitch` -> `target_pitch`, `Is Detected` -> `is_detected`."""
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", text)
    return "_".join(re.split(r"[^A-Za-z0-9]+", spaced)).strip("_").lower()


def _toml(value: str) -> str:
    return json.dumps(value)  # a JSON string is a valid TOML basic string


def reference_entries(path: Path) -> dict[str, set[str]]:
    """Entry name -> the types it was started with, from a reference wpilog's catalog."""
    reader = WpilogReader(path)
    for _ in reader:
        pass
    entries: dict[str, set[str]] = {}
    for entry in reader.catalog:
        entries.setdefault(entry.name, set()).add(entry.type)
    return entries


def _check(name: str, reference: dict[str, set[str]]) -> str | None:
    types = reference.get(name)
    if types is None:
        return NOT_IN_LOG
    return None if types & NUMERIC_TYPES else NON_NUMERIC


def _superseding_slot(row: dict[str, str], slots: list[Slot]) -> Slot | None:
    if "MOTOR" not in row["component"].upper() or row["metric"].upper() not in MOTOR_METRICS:
        return None
    location = _slug(row["subsystem"], row["assembly"], row["subassembly"], row["component"])
    exact = [s for s in slots if s.id.rsplit("-", 1)[0] == location]
    subsystem = row["subsystem"].lower().replace("_", " ")
    in_subsystem = [s for s in slots if s.subsystem == subsystem]
    candidates = exact or in_subsystem
    return candidates[0] if candidates else None


def migrate_nt_maps(
    season: int,
    metrics_map: Path,
    vision_map: Path,
    log_config: Path,
    reference: dict[str, set[str]],
    slots: list[Slot],
) -> tuple[str, str, list[str]]:
    """Return (season TOML text, robot `[[signal]]` TOML text, report lines).

    Every legacy row and log-config prefix is either mapped or reported with a reason.
    """
    report: list[str] = []
    roots: dict[str, str] = {}
    for key, prefix in json.loads(log_config.read_text()).items():
        if key in ROOT_KEYS:
            roots[ROOT_KEYS[key]] = prefix
            continue
        types = set().union(*(t for n, t in reference.items() if n.startswith(prefix)))
        reason = NOT_IN_LOG if not types else NON_NUMERIC if not types & NUMERIC_TYPES else None
        report.append(f"unmapped {log_config.name}: {key} = {prefix} ({reason or 'no root'})")

    signals: list[dict[str, str]] = []

    def consider(
        path: Path, root: str, entry: str, subsystem: str, component: str, metric: str
    ) -> None:
        reason = _check(roots[root] + entry, reference) if root in roots else NOT_IN_LOG
        if reason is not None:
            report.append(f"unmapped {path.name}: {entry} ({reason})")
            return
        signal = {"id": _slug(subsystem, component, metric), "root": root, "entry": entry}
        signal["subsystem"] = subsystem
        if component:
            signal["component"] = component
        signals.append(signal | {"metric": metric})

    with metrics_map.open(newline="") as f:
        for row in csv.DictReader(f):
            entry = row["entry"]
            slot = _superseding_slot(row, slots)
            if slot is not None:
                report.append(
                    f"superseded {metrics_map.name}: {entry} (superseded by CAN slot {slot.id})"
                )
                continue
            consider(
                metrics_map, "robot", entry,
                row["subsystem"].lower().replace("_", " "),
                _slug(row["assembly"], row["subassembly"], row["component"]),
                _snake(row["metric"] or entry.rsplit("/", 1)[-1]),
            )  # fmt: skip
    with vision_map.open(newline="") as f:
        for row in csv.DictReader(f):
            entry = row["entry"]
            consider(
                vision_map, "photonvision", entry, "vision", _slug(row["camera"]),
                _snake(row["metric"] or entry.rsplit("/", 1)[-1]),
            )  # fmt: skip

    season_lines = [
        f"# Migrated from legacy {log_config.name} by tools/migrate-legacy-config.py.",
        f"season = {season}",
        "",
        "[roots]",
        *(f"{name} = {_toml(prefix)}" for name, prefix in roots.items()),
    ]
    signal_lines: list[str] = []
    for signal in signals:
        signal_lines.append("\n[[signal]]")
        signal_lines += [f"{k} = {_toml(v)}" for k, v in signal.items()]
    signal_text = "\n".join(signal_lines) + "\n" if signal_lines else ""
    return "\n".join(season_lines) + "\n", signal_text, report
