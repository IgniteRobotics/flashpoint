"""The live CAN service inside `flashpoint serve`: one NetworkTables client connection to the
robot, opened by the first subscriber, shared by all, and closed a while after the last leaves.

A connection thread reads the topic tree, runs the detector on each batch of updates, and puts
events (as they happen) and snapshots (at most `snapshot_interval_s` apart, when changed) on
each subscriber's bounded queue. The client only subscribes; it publishes nothing to the robot.
"""

import queue
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any

from flashpoint.live import contract
from flashpoint.live.detector import Detector, SlotResolver

NT4_PORT = 5810
# Spec: closed within 30 s after the last view closes. A stream notices its browser left within
# half a second (web.server.LIVE_POLL_S), so linger a little under 30 s.
LINGER_S = 29.0
QUEUE_SIZE = 256
SNAPSHOT_INTERVAL_S = 0.5  # at most 2 Hz
POLL_S = 0.02
# Connected but no `Version` for this long: robot code isn't publishing the contract.
NOT_PUBLISHING_AFTER_S = 1.0

Message = tuple[str, dict[str, Any]]  # ("snapshot" | "event", payload)


class Subscription:
    """One viewer's bounded queue. A viewer too slow to keep up is marked overflowed and gets
    nothing more; its stream closes, and the browser reconnects to a fresh snapshot."""

    def __init__(self, service: "LiveCanService", maxsize: int) -> None:
        self._service = service
        self._queue: queue.Queue[Message] = queue.Queue(maxsize)
        self.overflowed = False
        self.closed = False

    def put(self, message: Message) -> None:
        if self.overflowed or self.closed:
            return
        try:
            self._queue.put_nowait(message)
        except queue.Full:
            self.overflowed = True

    def get(self, timeout: float | None = None) -> Message | None:
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._service.unsubscribe(self)


class _Connection(threading.Thread):
    def __init__(
        self,
        hosts: Sequence[str],
        port: int,
        resolver: SlotResolver | None,
        broadcast: Callable[[Message], None],
        snapshot_interval_s: float,
        not_publishing_after_s: float,
        clock: Callable[[], float],
    ) -> None:
        super().__init__(name="flashpoint-live-can", daemon=True)
        self._hosts = list(hosts)
        self._port = port
        self._broadcast = broadcast
        self._snapshot_interval_s = snapshot_interval_s
        self._not_publishing_after_s = not_publishing_after_s
        self._clock = clock
        self._halt = threading.Event()
        self._lock = threading.Lock()  # the detector and connection fields, against view()
        self._detector = Detector(resolver)
        self._snapshot: dict[str, tuple[str, Any]] = {}
        self._connected_at: float | None = None
        self._host: str | None = None

    def view(self) -> dict[str, Any]:
        with self._lock:
            now = self._clock()
            out = self._detector.view(now)
            connected = self._connected_at is not None
            out["connection"] = "connected" if connected else "connecting"
            out["robot_host"] = self._host
            out["hosts"] = self._hosts
            waiting = self._connected_at is None or (
                now - self._connected_at < self._not_publishing_after_s
            )
            if out["status"] == "not-publishing" and waiting:
                out["status"] = "waiting"
            return out

    def stop(self) -> None:
        self._halt.set()
        if self.is_alive() and threading.current_thread() is not self:
            self.join(5)

    def run(self) -> None:
        import ntcore  # only when a live view opens

        inst = ntcore.NetworkTableInstance.create()
        try:
            inst.startClient4("flashpoint")
            inst.setServer([(host, self._port) for host in self._hosts])
            subscriber = ntcore.MultiSubscriber(inst, [contract.ROOT])
            poller = ntcore.NetworkTableListenerPoller(inst)
            poller.addListener(
                [contract.ROOT], ntcore.EventFlags.kValueAll | ntcore.EventFlags.kImmediate
            )
            poller.addConnectionListener(True)
            self._loop(ntcore, poller)
            subscriber.close()
            poller.close()
        finally:
            inst.stopClient()
            ntcore.NetworkTableInstance.destroy(inst)

    def _loop(self, ntcore: Any, poller: Any) -> None:
        last_view: dict[str, Any] | None = None
        last_sent = 0.0
        while not self._halt.is_set():
            changed = False
            for event in poller.readQueue():
                if event.is_(ntcore.EventFlags.kConnected):
                    with self._lock:
                        self._connected_at = self._clock()
                        self._host = event.data.remote_ip
                elif event.is_(ntcore.EventFlags.kDisconnected):
                    with self._lock:
                        self._connected_at = None
                        self._host = None
                elif event.is_(ntcore.EventFlags.kValueAll):
                    data = event.data
                    self._snapshot[data.topic.getName()] = (
                        data.topic.getTypeString(), data.value.value()
                    )
                    changed = True
            if changed:
                with self._lock:
                    events = self._detector.update(dict(self._snapshot), self._clock())
                for found in events:
                    self._broadcast(("event", found.to_json()))
            now = time.monotonic()
            if now - last_sent >= self._snapshot_interval_s:
                view = self.view()
                if view != last_view:
                    self._broadcast(("snapshot", view))
                    last_view = view
                    last_sent = now
            self._halt.wait(POLL_S)


class LiveCanService:
    def __init__(
        self,
        hosts: Sequence[str],
        port: int = NT4_PORT,
        resolver: SlotResolver | None = None,
        linger_s: float = LINGER_S,
        queue_size: int = QUEUE_SIZE,
        snapshot_interval_s: float = SNAPSHOT_INTERVAL_S,
        not_publishing_after_s: float = NOT_PUBLISHING_AFTER_S,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.hosts = list(hosts)
        self._port = port
        self._resolver = resolver
        self._linger_s = linger_s
        self._queue_size = queue_size
        self._snapshot_interval_s = snapshot_interval_s
        self._not_publishing_after_s = not_publishing_after_s
        self._clock = clock
        self._lock = threading.Lock()
        self._subs: list[Subscription] = []
        self._connection: _Connection | None = None
        self._timer: threading.Timer | None = None

    @property
    def is_open(self) -> bool:
        return self._connection is not None

    def subscribe(self) -> Subscription:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            if self._connection is None:
                self._connection = _Connection(
                    self.hosts, self._port, self._resolver, self._broadcast,
                    self._snapshot_interval_s, self._not_publishing_after_s, self._clock,
                )
                self._connection.start()
            sub = Subscription(self, self._queue_size)
            sub.put(("snapshot", self._connection.view()))
            self._subs.append(sub)
            return sub

    def unsubscribe(self, sub: Subscription) -> None:
        with self._lock:
            if sub in self._subs:
                self._subs.remove(sub)
            if not self._subs and self._connection is not None and self._timer is None:
                self._timer = threading.Timer(self._linger_s, self._close_if_idle)
                self._timer.daemon = True
                self._timer.start()

    def _close_if_idle(self) -> None:
        with self._lock:
            self._timer = None
            if self._subs or self._connection is None:
                return
            connection, self._connection = self._connection, None
        connection.stop()

    def _broadcast(self, message: Message) -> None:
        with self._lock:
            subs = list(self._subs)
        for sub in subs:
            sub.put(message)

    def state(self) -> dict[str, Any]:
        """The current view without opening a connection."""
        connection = self._connection
        if connection is None:
            return {
                "connection": "closed",
                "status": None,
                "hosts": self.hosts,
                "supported_version": contract.CONTRACT_VERSION,
                "buses": [],
            }
        return connection.view()

    def close(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            connection, self._connection = self._connection, None
            subs, self._subs = self._subs, []
        for sub in subs:
            sub.closed = True
        if connection is not None:
            connection.stop()
