"""Slots (roles on a robot) linked to units (physical devices) for each session.

A unit is `ctre:<serial>` when the session's wpilog carries a valid CAN inventory, otherwise
`legacy:<robot>:<slot>:<epoch>`, where the epoch counts the swap dates declared in the robot
configuration on or before the session day.
"""

import json
from dataclasses import dataclass, field
from datetime import date

from flashpoint.meta.extract import validate_inventory
from flashpoint.semantics.robot_config import RobotConfig, Slot


@dataclass(frozen=True)
class DeviceSighting:
    bus: str
    model: str
    can_id: int


@dataclass(frozen=True)
class InventoryEvent:
    ts_us: int  # wpilog clock
    payload: str


@dataclass(frozen=True)
class Observation:
    session_id: str
    slot_id: str
    unit_id: str
    source: str  # inventory | legacy
    from_ts_us: int | None = None  # None: from the start of the session
    to_ts_us: int | None = None  # None: to the end of the session


@dataclass
class IdentityResult:
    observations: list[Observation] = field(default_factory=list)
    swaps: list[tuple[str, int, str, str]] = field(default_factory=list)  # slot, ts, old, new
    unmapped: list[tuple[str, str, int, str]] = field(
        default_factory=list
    )  # bus, model, id, source


def legacy_unit(robot: RobotConfig, slot: Slot, day: date) -> str:
    epoch = sum(1 for swap in slot.swaps if swap <= day)
    return f"legacy:{robot.robot}:{slot.id}:{epoch}"


def _timelines(
    robot: RobotConfig, inventories: list[InventoryEvent], result: IdentityResult
) -> dict[str, list[tuple[int, str]]]:
    """Per slot, the (ts, serial) points where the observed serial changes."""
    timelines: dict[str, list[tuple[int, str]]] = {}
    unmapped: set[tuple[str, str, int, str]] = set()
    for event in sorted(inventories, key=lambda e: e.ts_us):
        valid, _ = validate_inventory(event.payload)
        if not valid:
            continue
        for device in json.loads(event.payload)["devices"]:
            bus, model, can_id = str(device["bus"]), str(device["model"]), int(device["id"])
            slot = robot.slot_for(bus, model, can_id)
            if slot is None:
                unmapped.add((bus, model.replace(" ", ""), can_id, "inventory"))
                continue
            line = timelines.setdefault(slot.id, [])
            if not line or line[-1][1] != device["serial"]:
                line.append((event.ts_us, str(device["serial"])))
    result.unmapped += sorted(unmapped)
    return timelines


def resolve_identity(
    session_id: str,
    robot: RobotConfig,
    day: date,
    sightings: list[DeviceSighting],
    inventories: list[InventoryEvent],
) -> IdentityResult:
    result = IdentityResult()
    timelines = _timelines(robot, inventories, result)
    for slot_id, line in timelines.items():
        for i, (ts, serial) in enumerate(line):
            start = None if i == 0 else ts
            end = line[i + 1][0] if i + 1 < len(line) else None
            result.observations.append(
                Observation(session_id, slot_id, f"ctre:{serial}", "inventory", start, end)
            )
            if i > 0:
                result.swaps.append((slot_id, ts, f"ctre:{line[i - 1][1]}", f"ctre:{serial}"))
    seen: set[str] = set(timelines)
    unmapped: set[tuple[str, str, int, str]] = set()
    for sighting in sightings:
        slot = robot.slot_for(sighting.bus, sighting.model, sighting.can_id)
        if slot is None:
            unmapped.add((sighting.bus, sighting.model, sighting.can_id, "log"))
        elif slot.id not in seen:
            seen.add(slot.id)
            result.observations.append(
                Observation(session_id, slot.id, legacy_unit(robot, slot, day), "legacy")
            )
    result.unmapped += sorted(unmapped)
    return result
