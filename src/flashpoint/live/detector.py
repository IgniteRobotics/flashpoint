"""Live CAN health detection: compare each NetworkTables snapshot with the previous one.

Pure: a snapshot (full topic name -> (type string, value)) and the time the server received it
go in, events come out. Robot code counts dropouts; this only works out what rose. Dropouts are
grouped by the snapshot they arrive in, never by matching timestamps (pitfalls #14, #38).
"""

import math
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol

from flashpoint.live import contract

Snapshot = Mapping[str, tuple[str, Any]]

# A dropout is bus-wide when at least this share of the bus's other devices dropped too
# (the can-health sibling rule).
BUS_WIDE_SHARE = 0.5
# A device shows as dropped or rebooted for this long after the event.
RECENT_S = 5.0
# Totals that go down within this long of a restart belong to the same restart: robot code
# zeroes everything at once, but the zeros can arrive over two NetworkTables updates.
RESTART_GRACE_S = 2.0

_PY_TYPES: dict[str, type] = {"int": int, "double": float, "boolean": bool, "string": str}


@dataclass(frozen=True)
class SlotInfo:
    id: str
    role: str
    subsystem: str


@dataclass(frozen=True)
class MissingSlot:
    id: str
    role: str
    model: str
    can_id: int
    bus: str


class SlotMap(Protocol):
    robot: str

    def slot_for(self, bus: str, model: str, can_id: int) -> SlotInfo | None: ...

    def missing(self, reported: set[tuple[str, str, int]]) -> list[MissingSlot]: ...


SlotResolver = Callable[[str], SlotMap | None]


@dataclass
class Event:
    kind: str  # dropout, bus-dropout, disconnected, reconnected, reboot, fault, bus-errors, restart
    at: float  # when the server received the update (epoch seconds)
    enabled: bool | None
    bus: str | None = None
    device: str | None = None
    slot: str | None = None
    count: int | None = None
    max_age_ms: float | None = None
    fault: str | None = None
    devices: list[str] | None = None
    counts: dict[str, int] | None = None

    def to_json(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class DeviceState:
    bus: str
    name: str
    model: str
    can_id: int
    values: dict[str, Any] = field(default_factory=dict)  # last good value per field
    notes: dict[str, str] = field(default_factory=dict)
    dropouts: int = 0  # isolated dropouts since the connection opened
    resets: int = 0
    last_dropout: float | None = None
    last_reset: float | None = None


@dataclass
class BusState:
    name: str
    values: dict[str, Any] = field(default_factory=dict)
    notes: dict[str, str] = field(default_factory=dict)
    bus_dropouts: int = 0
    last_bus_dropout: float | None = None


_COUNTS = contract.DEVICE_TOTALS + contract.BUS_TOTALS + ("Faults",)


def _check(type_string: str, value: Any, expected: str, field_name: str) -> tuple[Any, str | None]:
    """The usable value, or None and a note saying why it can't be used."""
    if type_string != expected:
        return None, f"wrong type: {type_string}"
    if expected == "double" and isinstance(value, int) and not isinstance(value, bool):
        value = float(value)
    py = _PY_TYPES[expected]
    if not isinstance(value, py) or (py is int and isinstance(value, bool)):
        return None, f"wrong type: {type(value).__name__}"
    if isinstance(value, float) and not math.isfinite(value):
        return None, f"not a number: {value}"
    if field_name in _COUNTS and isinstance(value, int) and value < 0:
        return None, f"negative count: {value}"
    return value, None


class Detector:
    def __init__(self, resolver: SlotResolver | None = None) -> None:
        self._resolver = resolver
        self._slots: SlotMap | None = None
        self._resolved_project: str | None = None
        self.devices: dict[tuple[str, str], DeviceState] = {}
        self.buses: dict[str, BusState] = {}
        self.notes: dict[str, str] = {}
        self.version: Any = None
        self.project: str | None = None
        self.enabled: bool | None = None
        self._last_restart: float | None = None

    # --- reading a snapshot ---------------------------------------------------------------

    def _top(self, snapshot: Snapshot) -> None:
        self.notes = {}
        top: dict[str, Any] = {}
        for name, expected in contract.TOP_FIELDS.items():
            entry = snapshot.get(contract.ROOT + name)
            if entry is None:
                top[name] = None
                continue
            top[name], note = _check(entry[0], entry[1], expected, name)
            if note:
                self.notes[name] = note
        published = snapshot.get(contract.ROOT + "Version")
        self.version = top["Version"] if published is None or top["Version"] is not None else (
            published[1]
        )
        self.project = top["Project"]
        self.enabled = top["Enabled"]
        if self.project != self._resolved_project:
            self._resolved_project = self.project
            self._slots = self._resolver(self.project) if self._resolver and self.project else None

    @property
    def status(self) -> str:
        if self.version is None:
            return "not-publishing"
        if self.version != contract.CONTRACT_VERSION or "Version" in self.notes:
            return "unsupported"
        return "ok"

    def _read(
        self, snapshot: Snapshot
    ) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, dict[str, Any]]]:
        """Good values per device and bus; bad ones become notes."""
        devices: dict[tuple[str, str], dict[str, Any]] = {}
        buses: dict[str, dict[str, Any]] = {}
        for name, (type_string, value) in snapshot.items():
            topic = contract.parse_topic(name)
            if isinstance(topic, contract.DeviceTopic):
                key = (topic.bus, contract.device_name(topic.model, topic.can_id))
                device = self.devices.get(key)
                if device is None:
                    device = DeviceState(topic.bus, key[1], topic.model, topic.can_id)
                    self.devices[key] = device
                fields = devices.setdefault(key, {})
                good, note = _check(
                    type_string, value, contract.DEVICE_FIELDS[topic.field], topic.field
                )
                if note:
                    device.notes[topic.field] = note
                else:
                    device.notes.pop(topic.field, None)
                    fields[topic.field] = good
            elif isinstance(topic, contract.BusTopic):
                bus = self.buses.setdefault(topic.bus, BusState(topic.bus))
                fields = buses.setdefault(topic.bus, {})
                good, note = _check(
                    type_string, value, contract.BUS_FIELDS[topic.field], topic.field
                )
                if note:
                    bus.notes[topic.field] = note
                else:
                    bus.notes.pop(topic.field, None)
                    fields[topic.field] = good
        return devices, buses

    # --- comparing ------------------------------------------------------------------------

    def update(self, snapshot: Snapshot, at: float) -> list[Event]:
        """Compare `snapshot` with the previous one; `at` is when the server received it."""
        self._top(snapshot)
        if self.status != "ok":
            # Unknown contract: show nothing, record nothing, and baseline afresh later.
            self.devices.clear()
            self.buses.clear()
            return []
        devices, buses = self._read(snapshot)
        if self._went_down(devices, buses):
            self._rebaseline(devices, buses)
            in_grace = self._last_restart is not None and at - self._last_restart < RESTART_GRACE_S
            if in_grace:
                return []
            self._last_restart = at
            return [Event("restart", at, self.enabled)]
        events: list[Event] = []
        dropped: dict[str, list[tuple[DeviceState, int, float | None]]] = {}
        for key, fields in devices.items():
            device = self.devices[key]
            previous = device.values
            for name, value in fields.items():
                if name not in previous:
                    continue  # this field's baseline
                old = previous[name]
                if name == "Dropouts" and value > old:
                    dropped.setdefault(device.bus, []).append(
                        (device, value - old, fields.get("MaxAgeMs"))
                    )
                elif name == "Resets" and value > old:
                    device.resets += value - old
                    device.last_reset = at
                    events.append(self._device_event("reboot", at, device, count=value - old))
                elif name == "Connected" and value != old:
                    kind = "reconnected" if value else "disconnected"
                    events.append(self._device_event(kind, at, device))
                elif name == "Faults":
                    for fault in contract.fault_names(value & ~old):
                        events.append(self._device_event("fault", at, device, fault=fault))
            # Fields with a bad value this time lose their baseline.
            device.values = {**{k: v for k, v in previous.items() if k not in device.notes},
                             **fields}
        events = self._dropouts(dropped, at) + events
        for name, fields in buses.items():
            bus = self.buses[name]
            rises = {
                k: fields[k] - bus.values[k]
                for k in contract.BUS_TOTALS
                if k in fields and k in bus.values and fields[k] > bus.values[k]
            }
            if rises:
                events.append(Event("bus-errors", at, self.enabled, bus=name, counts=rises))
            bus.values = {**{k: v for k, v in bus.values.items() if k not in bus.notes}, **fields}
        return events

    def _went_down(
        self,
        devices: dict[tuple[str, str], dict[str, Any]],
        buses: dict[str, dict[str, Any]],
    ) -> bool:
        for key, fields in devices.items():
            previous = self.devices[key].values
            if any(k in previous and k in fields and fields[k] < previous[k]
                   for k in contract.DEVICE_TOTALS):
                return True
        for name, fields in buses.items():
            previous = self.buses[name].values
            if any(k in previous and k in fields and fields[k] < previous[k]
                   for k in contract.BUS_TOTALS):
                return True
        return False

    def _rebaseline(
        self,
        devices: dict[tuple[str, str], dict[str, Any]],
        buses: dict[str, dict[str, Any]],
    ) -> None:
        for key, fields in devices.items():
            self.devices[key].values = dict(fields)
        for name, fields in buses.items():
            self.buses[name].values = dict(fields)

    def _dropouts(
        self, dropped: dict[str, list[tuple[DeviceState, int, float | None]]], at: float
    ) -> list[Event]:
        events: list[Event] = []
        for bus_name, rises in dropped.items():
            reporting = sum(1 for d in self.devices.values() if d.bus == bus_name)
            others = reporting - 1
            if others > 0 and (len(rises) - 1) >= BUS_WIDE_SHARE * others:
                bus = self.buses.setdefault(bus_name, BusState(bus_name))
                bus.bus_dropouts += 1
                bus.last_bus_dropout = at
                ages = [age for _, _, age in rises if age is not None]
                events.append(Event(
                    "bus-dropout", at, self.enabled, bus=bus_name,
                    devices=[d.name for d, _, _ in rises],
                    count=max(c for _, c, _ in rises),
                    max_age_ms=max(ages) if ages else None,
                ))
                continue
            for device, count, age in rises:
                device.dropouts += count
                device.last_dropout = at
                events.append(
                    self._device_event("dropout", at, device, count=count, max_age_ms=age)
                )
        return events

    def _slot(self, device: DeviceState) -> SlotInfo | None:
        if self._slots is None:
            return None
        return self._slots.slot_for(device.bus, device.model, device.can_id)

    def _device_event(self, kind: str, at: float, device: DeviceState, **extra: Any) -> Event:
        slot = self._slot(device)
        return Event(
            kind, at, self.enabled, bus=device.bus, device=device.name,
            slot=slot.id if slot else None, **extra,
        )

    # --- what the view shows --------------------------------------------------------------

    def _device_state(self, device: DeviceState, slot: SlotInfo | None, now: float) -> str:
        values = device.values
        if values.get("Connected") is False:
            return "disconnected"
        if device.last_dropout is not None and now - device.last_dropout < RECENT_S:
            return "dropped"
        if device.last_reset is not None and now - device.last_reset < RECENT_S:
            return "rebooted"
        if values.get("Faults"):
            return "faulted"
        return "ok" if slot is not None else "unmapped"

    def view(self, now: float) -> dict[str, Any]:
        """The state the Live CAN view renders (JSON-ready)."""
        out: dict[str, Any] = {
            "status": self.status,
            "version": self.version if isinstance(self.version, int | str) else None,
            "supported_version": contract.CONTRACT_VERSION,
            "project": self.project,
            "robot": self._slots.robot if self._slots else None,
            "project_found": self._slots is not None if self.project else None,
            "enabled": self.enabled,
            "notes": dict(self.notes),
            "buses": [],
        }
        if self.status != "ok":
            return out
        by_bus: dict[str, list[dict[str, Any]]] = {}
        for device in sorted(self.devices.values(), key=lambda d: (d.bus, d.model, d.can_id)):
            slot = self._slot(device)
            values = device.values
            by_bus.setdefault(device.bus, []).append({
                "name": device.name,
                "model": device.model,
                "can_id": device.can_id,
                "slot": slot.id if slot else None,
                "role": slot.role if slot else None,
                "state": self._device_state(device, slot, now),
                "connected": values.get("Connected"),
                "dropouts": device.dropouts,
                "resets": device.resets,
                "max_age_ms": values.get("MaxAgeMs"),
                "min_supply_v": values.get("MinSupplyV"),
                "faults": contract.fault_names(values.get("Faults", 0)),
                "notes": dict(device.notes),
            })
        missing: dict[str, list[dict[str, Any]]] = {}
        if self._slots is not None:
            reported = {(d.bus, d.model, d.can_id) for d in self.devices.values()}
            for gone in self._slots.missing(reported):
                missing.setdefault(gone.bus, []).append(asdict(gone) | {"state": "missing"})
        for name in sorted(set(by_bus) | set(self.buses) | set(missing)):
            bus = self.buses.get(name)
            values = bus.values if bus else {}
            out["buses"].append({
                "name": name,
                "utilization": values.get("Utilization"),
                "counts": {k: values[k] for k in contract.BUS_TOTALS if k in values},
                "bus_dropouts": bus.bus_dropouts if bus else 0,
                "dropped": bool(bus and bus.last_bus_dropout is not None
                                and now - bus.last_bus_dropout < RECENT_S),
                "notes": dict(bus.notes) if bus else {},
                "devices": by_bus.get(name, []),
                "missing": missing.get(name, []),
            })
        return out
