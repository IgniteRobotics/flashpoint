"""The live CAN health contract: the NetworkTables topics robot code publishes for Flashpoint.

This module is the only place topic names and types are spelled out; the service, the fake
robot and the guide's drift test all read them from here.
"""

import re
from collections.abc import Iterable
from typing import NamedTuple

CONTRACT_VERSION = 1
# Not under /Flashpoint/CANBus/: the wpilog copy of these topics is "NT:/Flashpoint/Live/...",
# which the season's `flashpoint` root (/Flashpoint/) must not match.
ROOT = "/Flashpoint/Live/CANHealth/"
BUS_DIR = "Bus"

# NetworkTables 4 type strings.
TOP_FIELDS: dict[str, str] = {"Version": "int", "Project": "string", "Enabled": "boolean"}
DEVICE_FIELDS: dict[str, str] = {
    "Connected": "boolean",
    "Dropouts": "int",
    "MaxAgeMs": "double",
    "Resets": "int",
    "Faults": "int",
    "MinSupplyV": "double",
}
BUS_FIELDS: dict[str, str] = {
    "Utilization": "double",
    "BusOffCount": "int",
    "TxFullCount": "int",
    "ReceiveErrorCount": "int",
    "TransmitErrorCount": "int",
}
# Running totals since robot code started: they only rise, and a drop means a restart.
DEVICE_TOTALS = ("Dropouts", "Resets")
BUS_TOTALS = ("BusOffCount", "TxFullCount", "ReceiveErrorCount", "TransmitErrorCount")

FAULT_BITS: dict[int, str] = {
    0: "under-voltage",
    1: "boot during enable",
    2: "hardware",
    3: "bridge brownout",
    4: "bad magnet",
}

MAX_CAN_ID = 62
_DEVICE = re.compile(r"^([A-Za-z][A-Za-z0-9]*)-(\d{1,2})$")


class TopTopic(NamedTuple):
    field: str


class DeviceTopic(NamedTuple):
    bus: str
    model: str
    can_id: int
    field: str


class BusTopic(NamedTuple):
    bus: str
    field: str


Topic = TopTopic | DeviceTopic | BusTopic


def parse_device(name: str) -> tuple[str, int] | None:
    """`TalonFX-11` -> ("TalonFX", 11), the hoot device name form."""
    match = _DEVICE.match(name)
    if match is None or int(match.group(2)) > MAX_CAN_ID:
        return None
    return match.group(1), int(match.group(2))


def device_name(model: str, can_id: int) -> str:
    return f"{model}-{can_id}"


def parse_topic(name: str) -> Topic | None:
    """Classify a full topic name; None for anything outside the contract."""
    if not name.startswith(ROOT):
        return None
    parts = name[len(ROOT):].split("/")
    if len(parts) == 1:
        return TopTopic(parts[0]) if parts[0] in TOP_FIELDS else None
    if len(parts) != 3 or not parts[0]:
        return None
    bus, middle, field = parts
    if middle == BUS_DIR:
        return BusTopic(bus, field) if field in BUS_FIELDS else None
    device = parse_device(middle)
    if device is None or field not in DEVICE_FIELDS:
        return None
    return DeviceTopic(bus, device[0], device[1], field)


def fault_names(bits: int) -> list[str]:
    return [name for bit, name in FAULT_BITS.items() if bits >> bit & 1]


def topic_names(bus: str, devices: Iterable[str]) -> list[str]:
    """Every topic for one bus and its devices (plus the top-level ones), sorted."""
    names = [ROOT + field for field in TOP_FIELDS]
    names += [f"{ROOT}{bus}/{BUS_DIR}/{field}" for field in BUS_FIELDS]
    for device in devices:
        names += [f"{ROOT}{bus}/{device}/{field}" for field in DEVICE_FIELDS]
    return sorted(names)
