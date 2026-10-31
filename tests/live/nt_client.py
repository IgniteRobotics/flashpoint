"""A bare NetworkTables test client that reads back what the fake robot publishes."""

import time
from types import TracebackType
from typing import Any

import ntcore

from flashpoint.live import contract

WAIT_S = 3.0


class TopicReader:
    def __init__(self, port: int) -> None:
        self.inst = ntcore.NetworkTableInstance.create()
        self.inst.startClient4("flashpoint-test")
        self.inst.setServer("127.0.0.1", port)
        self._sub = ntcore.MultiSubscriber(self.inst, [contract.ROOT])

    def __enter__(self) -> "TopicReader":
        return self

    def __exit__(
        self, kind: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self._sub.close()
        self.inst.stopClient()
        ntcore.NetworkTableInstance.destroy(self.inst)

    def value(self, suffix: str) -> Any:
        return self.inst.getTopic(contract.ROOT + suffix).genericSubscribe().get().value()

    def type_of(self, suffix: str) -> str:
        return self.inst.getTopic(contract.ROOT + suffix).getTypeString()

    def wait_connected(self) -> bool:
        deadline = time.monotonic() + WAIT_S
        while not self.inst.isConnected() and time.monotonic() < deadline:
            time.sleep(0.02)
        return bool(self.inst.isConnected())

    def wait_for(self, suffix: str, expected: Any, timeout_s: float = WAIT_S) -> Any:
        """Poll until `suffix` reads `expected`; returns the last value read."""
        deadline = time.monotonic() + timeout_s
        value = None
        while time.monotonic() < deadline:
            value = self.value(suffix)
            if value == expected:
                return value
            time.sleep(0.02)
        return value

    def topics(self) -> list[str]:
        return sorted(t.getName() for t in self.inst.getTopics(contract.ROOT))
