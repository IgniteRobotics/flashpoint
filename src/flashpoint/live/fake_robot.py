"""A fake robot: a local NetworkTables server publishing the live CAN health contract.

Tests drive it in-process with steps. For work on the view without a robot:

    python -m flashpoint.live.fake_robot --scenario shake
    flashpoint serve --robot-host 127.0.0.1
"""

import argparse
import random
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any

import ntcore

from flashpoint.live import contract

NT4_PORT = 5810
DEVICE_DEFAULTS: dict[str, Any] = {
    "Connected": True,
    "Dropouts": 0,
    "MaxAgeMs": 4.0,
    "Resets": 0,
    "Faults": 0,
    "MinSupplyV": 12.4,
}
BUS_DEFAULTS: dict[str, Any] = {
    "Utilization": 0.35,
    "BusOffCount": 0,
    "TxFullCount": 0,
    "ReceiveErrorCount": 0,
    "TransmitErrorCount": 0,
}
# Ignite's 2026 robot, by robot-code bus name, for the scenarios.
IGNITE_2026: dict[str, list[str]] = {
    "DriveTrain": [
        *(f"TalonFX-{i}" for i in (11, 12, 21, 22, 31, 32, 41, 42)),
        "Pigeon2-10",
        *(f"CANcoder-{i}" for i in (13, 23, 33, 43)),
    ],
    "rio": [f"TalonFX-{i}" for i in (1, 2, 3, 4, 5, 6, 7, 8, 10, 14)],
}


@dataclass(frozen=True)
class SetDevice:
    bus: str
    device: str
    values: dict[str, Any]

    def __init__(self, bus: str, device: str, **values: Any) -> None:
        object.__setattr__(self, "bus", bus)
        object.__setattr__(self, "device", device)
        object.__setattr__(self, "values", values)


@dataclass(frozen=True)
class SetBus:
    bus: str
    values: dict[str, Any]

    def __init__(self, bus: str, **values: Any) -> None:
        object.__setattr__(self, "bus", bus)
        object.__setattr__(self, "values", values)


@dataclass(frozen=True)
class SetEnabled:
    enabled: bool


@dataclass(frozen=True)
class SetVersion:
    version: int


@dataclass(frozen=True)
class SetProject:
    project: str


@dataclass(frozen=True)
class Restart:
    """Robot code restarts: every running total goes back to zero."""


@dataclass(frozen=True)
class WrongType:
    """Publish `suffix` (relative to the contract root) with a value of the wrong type."""

    suffix: str
    value: str


Step = SetDevice | SetBus | SetEnabled | SetVersion | SetProject | Restart | WrongType


class FakeRobot:
    """Publishes the contract for `devices` ({bus: ["TalonFX-11", ...]}) on 127.0.0.1:`port`.
    With `publish=False` it runs the server but publishes nothing (robot code without the
    publisher)."""

    def __init__(
        self,
        devices: Mapping[str, Sequence[str]],
        port: int = NT4_PORT,
        project: str = "Robot-2026",
        version: int = contract.CONTRACT_VERSION,
        enabled: bool = False,
        publish: bool = True,
        host: str = "127.0.0.1",
    ) -> None:
        self.port = port
        self.devices = {bus: list(names) for bus, names in devices.items()}
        self._inst = ntcore.NetworkTableInstance.create()
        self._inst.startServer(persist_filename="", listen_address=host, port3=0, port4=port)
        self._pubs: dict[str, ntcore.GenericPublisher] = {}
        self._lock = threading.Lock()
        if publish:
            self._publish_all(project, version, enabled)

    def __enter__(self) -> "FakeRobot":
        return self

    def __exit__(
        self, kind: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            self._pubs.clear()  # dropping a publisher unpublishes it
            self._inst.stopServer()
            ntcore.NetworkTableInstance.destroy(self._inst)

    def clients(self) -> int:
        """How many NetworkTables clients are connected right now."""
        return len(self._inst.getConnections())

    def _publish_all(self, project: str, version: int, enabled: bool) -> None:
        self._set("Version", version)
        self._set("Project", project)
        self._set("Enabled", enabled)
        for bus, names in self.devices.items():
            for name, value in BUS_DEFAULTS.items():
                self._set(f"{bus}/{contract.BUS_DIR}/{name}", value)
            for device in names:
                for name, value in DEVICE_DEFAULTS.items():
                    self._set(f"{bus}/{device}/{name}", value)
        self._inst.flush()

    def _set(self, suffix: str, value: Any, type_string: str | None = None) -> None:
        topic = contract.parse_topic(contract.ROOT + suffix)
        if type_string is None:
            if topic is None:
                raise ValueError(f"{suffix!r} is not a contract topic")
            fields = {**contract.TOP_FIELDS, **contract.DEVICE_FIELDS, **contract.BUS_FIELDS}
            type_string = fields[topic.field]
        name = contract.ROOT + suffix
        pub = self._pubs.get(name)
        if pub is not None and pub.getTopic().getTypeString() != type_string:
            del self._pubs[name]  # unpublish, so the topic can take the new type
            pub = None
        if pub is None:
            pub = self._inst.getTopic(name).genericPublish(type_string)
            self._pubs[name] = pub
        value_of: dict[str, Callable[[Any], ntcore.Value]] = {
            "int": ntcore.Value.makeInteger,
            "double": ntcore.Value.makeDouble,
            "boolean": ntcore.Value.makeBoolean,
            "string": ntcore.Value.makeString,
        }
        pub.set(value_of[type_string](value))

    def apply(self, step: Step) -> None:
        """One step, sent as its own update."""
        self.run([step])

    def run(self, steps: Sequence[Step], interval_s: float = 0.0) -> None:
        """Steps sent together in one update, as one robot loop would; with `interval_s`, each
        step is its own update, that far apart."""
        if interval_s:
            for step in steps:
                self.run([step])
                time.sleep(interval_s)
            return
        with self._lock:
            for step in steps:
                self._apply(step)
            self._inst.flush()

    def _apply(self, step: Step) -> None:
        match step:
            case SetDevice(bus=bus, device=device, values=values):
                for name, value in values.items():
                    self._set(f"{bus}/{device}/{name}", value)
            case SetBus(bus=bus, values=values):
                for name, value in values.items():
                    self._set(f"{bus}/{contract.BUS_DIR}/{name}", value)
            case SetEnabled(enabled=enabled):
                self._set("Enabled", enabled)
            case SetVersion(version=version):
                self._set("Version", version)
            case SetProject(project=project):
                self._set("Project", project)
            case Restart():
                for bus, names in self.devices.items():
                    for name in contract.BUS_TOTALS:
                        self._set(f"{bus}/{contract.BUS_DIR}/{name}", 0)
                    for device in names:
                        for name in contract.DEVICE_TOTALS:
                            self._set(f"{bus}/{device}/{name}", 0)
            case WrongType(suffix=suffix, value=value):
                self._set(suffix, value, "string")


# --- scenarios for `python -m flashpoint.live.fake_robot` -------------------------------


class _Totals:
    """Keeps running totals so scenario steps only ever raise them."""

    def __init__(self, devices: Mapping[str, Sequence[str]]) -> None:
        self.dropouts = {(b, d): 0 for b, names in devices.items() for d in names}
        self.resets = dict.fromkeys(self.dropouts, 0)
        self.bus_errors = dict.fromkeys(devices, 0)

    def dropout(self, bus: str, device: str, count: int, age_ms: float) -> SetDevice:
        self.dropouts[bus, device] += count
        return SetDevice(bus, device, Dropouts=self.dropouts[bus, device], MaxAgeMs=age_ms)

    def reboot(self, bus: str, device: str) -> SetDevice:
        self.resets[bus, device] += 1
        return SetDevice(bus, device, Resets=self.resets[bus, device], Faults=0b10)

    def zero(self) -> None:
        self.dropouts = dict.fromkeys(self.dropouts, 0)
        self.resets = dict.fromkeys(self.resets, 0)
        self.bus_errors = dict.fromkeys(self.bus_errors, 0)


def _shake(devices: Mapping[str, Sequence[str]], rng: random.Random) -> Iterator[list[Step]]:
    """Disabled; one device at a time drops out, as a loose harness would."""
    totals = _Totals(devices)
    yield [SetEnabled(False)]
    while True:
        bus = rng.choice(list(devices))
        device = rng.choice(list(devices[bus]))
        yield [totals.dropout(bus, device, rng.randint(1, 5), rng.uniform(20, 200))]
        yield []
        yield []


def _drive(devices: Mapping[str, Sequence[str]], rng: random.Random) -> Iterator[list[Step]]:
    """Enabled; now and then the whole CANivore drops, a motor reboots, or bus errors rise."""
    totals = _Totals(devices)
    yield [SetEnabled(True)]
    buses = list(devices)
    while True:
        roll = rng.random()
        bus = rng.choice(buses)
        if roll < 0.3:
            yield [totals.dropout(bus, d, 1, rng.uniform(20, 60)) for d in devices[bus]]
        elif roll < 0.5:
            yield [totals.reboot(bus, rng.choice(list(devices[bus])))]
        elif roll < 0.7:
            totals.bus_errors[bus] += rng.randint(1, 20)
            yield [SetBus(bus, ReceiveErrorCount=totals.bus_errors[bus], Utilization=0.6)]
        else:
            yield [SetBus(bus, Utilization=rng.uniform(0.3, 0.5))]


def _restart(devices: Mapping[str, Sequence[str]], rng: random.Random) -> Iterator[list[Step]]:
    """Totals climb, then robot code restarts and they start again from zero."""
    totals = _Totals(devices)
    while True:
        for _ in range(6):
            bus = rng.choice(list(devices))
            yield [totals.dropout(bus, rng.choice(list(devices[bus])), 1, 30.0)]
        totals.zero()
        yield [Restart()]


SCENARIOS: dict[
    str, Callable[[Mapping[str, Sequence[str]], random.Random], Iterator[list[Step]]]
] = {"shake": _shake, "drive": _drive, "restart": _restart}


def run_scenario(
    name: str,
    port: int = NT4_PORT,
    period_s: float = 1.0,
    stop: threading.Event | None = None,
    seed: int | None = None,
    host: str = "127.0.0.1",
) -> int:
    """Run a scenario until `stop` is set (or Ctrl+C); returns the number of updates sent."""
    stop = stop or threading.Event()
    rng = random.Random(seed)
    sent = 0
    with FakeRobot(IGNITE_2026, port=port, host=host) as robot:
        steps = SCENARIOS[name](IGNITE_2026, rng)
        try:
            while not stop.is_set():
                robot.run(next(steps))
                sent += 1
                stop.wait(period_s)
        except KeyboardInterrupt:
            pass
    return sent


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m flashpoint.live.fake_robot",
        description="Publish fake live CAN health for the Live CAN view (Ctrl+C to stop).",
    )
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="shake")
    parser.add_argument("--port", type=int, default=NT4_PORT)
    parser.add_argument("--host", default="127.0.0.1", help="address to listen on")
    parser.add_argument("--period", type=float, default=1.0, help="seconds between updates")
    args = parser.parse_args(argv)
    print(
        f"fake robot: {args.scenario} on {args.host}:{args.port};"
        f" run `flashpoint serve --robot-host {args.host}` and open Live CAN (Ctrl+C to stop)"
    )
    sent = run_scenario(args.scenario, args.port, args.period, host=args.host)
    print(f"stopped after {sent} updates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
