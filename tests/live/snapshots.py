"""Build detector snapshots the way the connection does: full topic name -> (type, value)."""

from collections.abc import Mapping
from typing import Any

from flashpoint.live import contract
from flashpoint.live.detector import Snapshot
from flashpoint.live.fake_robot import BUS_DEFAULTS, DEVICE_DEFAULTS

FIELDS = {**contract.TOP_FIELDS, **contract.DEVICE_FIELDS, **contract.BUS_FIELDS}


class Robot:
    """A mutable robot tree; `snapshot()` freezes it for one detector update."""

    def __init__(
        self,
        devices: Mapping[str, list[str]],
        project: str = "Robot-2026",
        enabled: bool = False,
        version: Any = contract.CONTRACT_VERSION,
    ) -> None:
        self.values: dict[str, tuple[str, Any]] = {}
        self.set("Version", version)
        self.set("Project", project)
        self.set("Enabled", enabled)
        for bus, names in devices.items():
            for field, value in BUS_DEFAULTS.items():
                self.set(f"{bus}/Bus/{field}", value)
            for device in names:
                for field, value in DEVICE_DEFAULTS.items():
                    self.set(f"{bus}/{device}/{field}", value)

    def set(self, suffix: str, value: Any, type_string: str | None = None) -> "Robot":
        if type_string is None:
            topic = contract.parse_topic(contract.ROOT + suffix)
            assert topic is not None, suffix
            type_string = FIELDS[topic.field]
        self.values[contract.ROOT + suffix] = (type_string, value)
        return self

    def device(self, bus: str, device: str, **values: Any) -> "Robot":
        for field, value in values.items():
            self.set(f"{bus}/{device}/{field}", value)
        return self

    def bus(self, bus: str, **values: Any) -> "Robot":
        for field, value in values.items():
            self.set(f"{bus}/Bus/{field}", value)
        return self

    def snapshot(self) -> Snapshot:
        return dict(self.values)
