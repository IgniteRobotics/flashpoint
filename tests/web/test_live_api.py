import contextlib
import http.client
import json
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flashpoint.lake.paths import LakePaths
from flashpoint.live.fake_robot import FakeRobot, SetDevice, SetEnabled
from flashpoint.live.service import LiveCanService
from flashpoint.live.slots import load_resolver
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.server import FlashpointServer
from tests.conftest import REPO_CONFIG
from tests.live.test_fake_robot import DEVICES, free_port

WAIT_S = 5.0


class Stream:
    """A server-sent events reader over one HTTP connection."""

    def __init__(self, port: int, path: str = "/api/live/can/stream") -> None:
        self.conn = http.client.HTTPConnection("127.0.0.1", port, timeout=WAIT_S)
        self.conn.request("GET", path)
        self.sock = self.conn.sock  # the response takes it over; close() shuts it for real
        self.response = self.conn.getresponse()

    def next(self) -> tuple[str, dict[str, Any], float]:
        kind, data = "message", ""
        while True:
            line = self.response.fp.readline().decode()
            if line == "":
                raise EOFError("stream closed")
            line = line.rstrip("\n")
            if line.startswith(":"):
                continue
            if line == "":
                if data:
                    return kind, json.loads(data), time.monotonic()
                continue
            name, _, value = line.partition(": ")
            if name == "event":
                kind = value
            elif name == "data":
                data += value

    def until(self, kind: str, check: Any = lambda m: True) -> dict[str, Any]:
        deadline = time.monotonic() + WAIT_S
        while time.monotonic() < deadline:
            got, message, _ = self.next()
            if got == kind and check(message):
                return message
        raise AssertionError(f"no {kind}")

    def close(self) -> None:
        with contextlib.suppress(OSError):
            self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        self.response.close()
        self.conn.close()


@pytest.fixture
def robot() -> Iterator[FakeRobot]:
    with FakeRobot(DEVICES, port=free_port()) as fake:
        yield fake


@pytest.fixture
def live_server(tmp_path: Path, robot: FakeRobot) -> Iterator[FlashpointServer]:
    lake = LakePaths(tmp_path / "lake")
    live = LiveCanService(
        ["127.0.0.1"], port=robot.port, resolver=load_resolver(REPO_CONFIG), linger_s=0.3
    )
    server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake), live=live)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()
    server.queries.close()


def port_of(server: FlashpointServer) -> int:
    return int(server.server_address[1])


def test_stream_headers_and_snapshot_first(live_server: FlashpointServer) -> None:
    stream = Stream(port_of(live_server))
    try:
        assert stream.response.status == 200
        assert stream.response.getheader("Content-Type") == "text/event-stream; charset=utf-8"
        assert stream.response.getheader("Cache-Control") == "no-store"
        kind, first, _ = stream.next()
        assert kind == "snapshot"
        assert first["connection"] in ("connecting", "connected")
    finally:
        stream.close()


def test_events_reach_the_browser_within_a_second(
    live_server: FlashpointServer, robot: FakeRobot
) -> None:
    stream = Stream(port_of(live_server))
    try:
        stream.until("snapshot", lambda s: s["status"] == "ok")
        robot.apply(SetEnabled(True))
        stream.until("snapshot", lambda s: s["enabled"] is True)
        sent = time.monotonic()
        robot.apply(SetDevice("DriveTrain", "TalonFX-11", Dropouts=2, MaxAgeMs=48.0))
        event = stream.until("event", lambda e: e["kind"] == "dropout")
        assert time.monotonic() - sent < 1.0
        assert (event["slot"], event["count"], event["max_age_ms"], event["enabled"]) == (
            "drive-fl", 2, 48.0, True
        )
    finally:
        stream.close()


def test_snapshots_at_most_two_per_second(
    live_server: FlashpointServer, robot: FakeRobot
) -> None:
    stream = Stream(port_of(live_server))
    stop = threading.Event()

    def churn() -> None:
        step = 0
        while not stop.is_set():
            step += 1
            robot.apply(SetDevice("rio", "TalonFX-2", MinSupplyV=10 + step % 20 / 10))
            stop.wait(0.03)

    churner = threading.Thread(target=churn, daemon=True)
    try:
        stream.until("snapshot", lambda s: s["status"] == "ok")
        churner.start()
        times: list[float] = []
        while len(times) < 5:
            kind, _, at = stream.next()
            if kind == "snapshot":
                times.append(at)
        gaps = [b - a for a, b in zip(times, times[1:], strict=False)]
        assert min(gaps) > 0.4, gaps
    finally:
        stop.set()
        churner.join(2)
        stream.close()


def test_state_route(live_server: FlashpointServer) -> None:
    conn = http.client.HTTPConnection("127.0.0.1", port_of(live_server), timeout=WAIT_S)
    conn.request("GET", "/api/live/can/state")
    reply = conn.getresponse()
    body = json.loads(reply.read())
    assert reply.status == 200
    assert body["connection"] == "closed"  # asking doesn't open a connection
    assert body["supported_version"] == 1
    assert not live_server.live.is_open
    stream = Stream(port_of(live_server))
    try:
        stream.until("snapshot", lambda s: s["status"] == "ok")
        conn.request("GET", "/api/live/can/state")
        body = json.loads(conn.getresponse().read())
        assert (body["connection"], body["status"]) == ("connected", "ok")
        assert len(body["buses"]) == 2
    finally:
        stream.close()
        conn.close()


def test_closing_the_stream_releases_the_robot(
    live_server: FlashpointServer, robot: FakeRobot
) -> None:
    stream = Stream(port_of(live_server))
    stream.until("snapshot", lambda s: s["status"] == "ok")
    assert robot.clients() == 1
    stream.close()
    deadline = time.monotonic() + WAIT_S
    while live_server.live.is_open and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not live_server.live.is_open


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE", "PATCH"])
@pytest.mark.parametrize("path", ["/api/live/can/stream", "/api/live/can/state"])
def test_writes_are_refused(live_server: FlashpointServer, method: str, path: str) -> None:
    conn = http.client.HTTPConnection("127.0.0.1", port_of(live_server), timeout=WAIT_S)
    conn.request(method, path, body=b"{}", headers={"Content-Type": "application/json"})
    reply = conn.getresponse()
    assert reply.status == 405
    conn.close()
    assert not live_server.live.is_open


def test_overflowed_subscriber_stream_ends(
    live_server: FlashpointServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream = Stream(port_of(live_server))
    try:
        stream.until("snapshot")
        for sub in list(live_server.live._subs):  # noqa: SLF001 - simulate a stuck browser
            sub.overflowed = True
        with pytest.raises(EOFError):
            while True:
                stream.next()
    finally:
        stream.close()
