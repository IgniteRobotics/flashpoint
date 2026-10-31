import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from flashpoint.live.fake_robot import FakeRobot, SetDevice, SetVersion
from flashpoint.live.service import LiveCanService, Subscription
from flashpoint.live.slots import load_resolver
from tests.conftest import REPO_CONFIG
from tests.live.test_fake_robot import DEVICES, free_port

WAIT_S = 5.0
LINGER_S = 0.3


def wait_until(check: Callable[[], bool], timeout_s: float = WAIT_S) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if check():
            return True
        time.sleep(0.02)
    return check()


def next_message(sub: Subscription, kind: str, check: Callable[[dict[str, Any]], bool]) -> Any:
    """The first `kind` message passing `check`, within WAIT_S."""
    deadline = time.monotonic() + WAIT_S
    while time.monotonic() < deadline:
        message = sub.get(timeout=0.1)
        if message is not None and message[0] == kind and check(message[1]):
            return message[1]
    raise AssertionError(f"no {kind} message matching")


@pytest.fixture
def robot() -> Iterator[FakeRobot]:
    with FakeRobot(DEVICES, port=free_port()) as fake:
        yield fake


def service_for(port: int, hosts: list[str] | None = None, **kw: Any) -> LiveCanService:
    return LiveCanService(
        hosts or ["127.0.0.1"], port=port, resolver=load_resolver(REPO_CONFIG),
        linger_s=LINGER_S, **kw,
    )


def test_no_connection_until_the_first_subscriber(robot: FakeRobot) -> None:
    service = service_for(robot.port)
    try:
        time.sleep(0.3)
        assert not service.is_open
        assert robot.clients() == 0
        assert service.state()["connection"] == "closed"
        sub = service.subscribe()
        assert service.is_open
        assert wait_until(lambda: robot.clients() == 1)
        sub.close()
    finally:
        service.close()


def test_one_connection_shared_by_two(robot: FakeRobot) -> None:
    service = service_for(robot.port)
    try:
        first, second = service.subscribe(), service.subscribe()
        for sub in (first, second):
            next_message(sub, "snapshot", lambda s: s["connection"] == "connected"
                         and s["status"] == "ok")
        assert robot.clients() == 1
        robot.apply(SetDevice("DriveTrain", "TalonFX-11", Dropouts=2, MaxAgeMs=48.0))
        for sub in (first, second):
            event = next_message(sub, "event", lambda e: e["kind"] == "dropout")
            assert (event["slot"], event["count"], event["enabled"]) == ("drive-fl", 2, False)
        first.close()
        second.close()
    finally:
        service.close()


def test_snapshot_first_for_a_late_joiner(robot: FakeRobot) -> None:
    service = service_for(robot.port)
    try:
        first = service.subscribe()
        next_message(first, "snapshot", lambda s: s["status"] == "ok")
        late = service.subscribe()
        message = late.get(timeout=1)
        assert message is not None and message[0] == "snapshot"
        assert message[1]["status"] == "ok"
        assert len(message[1]["buses"]) == 2
        first.close()
        late.close()
    finally:
        service.close()


def test_closed_after_the_last_leaves(robot: FakeRobot) -> None:
    service = service_for(robot.port)
    try:
        first, second = service.subscribe(), service.subscribe()
        assert wait_until(lambda: robot.clients() == 1)
        first.close()
        time.sleep(LINGER_S * 2)
        assert service.is_open  # one still watching
        second.close()
        assert service.is_open  # lingers
        assert wait_until(lambda: not service.is_open, LINGER_S + 2)
        assert wait_until(lambda: robot.clients() == 0)
    finally:
        service.close()


def test_rejoin_within_the_linger_keeps_the_connection(robot: FakeRobot) -> None:
    service = service_for(robot.port)
    try:
        service.subscribe().close()
        time.sleep(LINGER_S / 3)
        sub = service.subscribe()
        time.sleep(LINGER_S * 2)
        assert service.is_open
        sub.close()
    finally:
        service.close()


def test_totals_reset_when_the_connection_reopens(robot: FakeRobot) -> None:
    service = service_for(robot.port)
    try:
        sub = service.subscribe()
        next_message(sub, "snapshot", lambda s: s["status"] == "ok")
        robot.apply(SetDevice("rio", "TalonFX-2", Dropouts=3))
        next_message(sub, "event", lambda e: e["kind"] == "dropout")
        sub.close()
        assert wait_until(lambda: not service.is_open, LINGER_S + 2)
        again = service.subscribe()
        snap = next_message(again, "snapshot", lambda s: s["status"] == "ok")
        (rio,) = [b for b in snap["buses"] if b["name"] == "rio"]
        assert {d["name"]: d["dropouts"] for d in rio["devices"]}["TalonFX-2"] == 0
        again.close()
    finally:
        service.close()


def test_connecting_while_no_robot() -> None:
    service = service_for(free_port())
    try:
        sub = service.subscribe()
        snap = next_message(sub, "snapshot", lambda s: True)
        assert snap["connection"] == "connecting"
        time.sleep(0.5)
        assert service.state()["connection"] == "connecting"
        sub.close()
    finally:
        service.close()


def test_not_publishing() -> None:
    with FakeRobot(DEVICES, port=free_port(), publish=False) as fake:
        service = service_for(fake.port, not_publishing_after_s=0.3)
        try:
            sub = service.subscribe()
            snap = next_message(sub, "snapshot", lambda s: s["connection"] == "connected"
                                and s["status"] == "not-publishing")
            assert snap["buses"] == []
            sub.close()
        finally:
            service.close()


def test_unsupported_version(robot: FakeRobot) -> None:
    service = service_for(robot.port)
    try:
        sub = service.subscribe()
        next_message(sub, "snapshot", lambda s: s["status"] == "ok")
        robot.apply(SetVersion(2))
        snap = next_message(sub, "snapshot", lambda s: s["status"] == "unsupported")
        assert (snap["version"], snap["supported_version"], snap["buses"]) == (2, 1, [])
        sub.close()
    finally:
        service.close()


def test_tries_each_host(robot: FakeRobot) -> None:
    # The first address answers nothing; the client moves on to the fake robot.
    service = service_for(robot.port, hosts=["192.0.2.1", "127.0.0.1"])
    try:
        sub = service.subscribe()
        snap = next_message(sub, "snapshot", lambda s: s["connection"] == "connected")
        assert snap["robot_host"] == "127.0.0.1"
        sub.close()
    finally:
        service.close()


def test_slow_subscriber_is_dropped(robot: FakeRobot) -> None:
    service = service_for(robot.port, queue_size=4)
    try:
        slow, fast = service.subscribe(), service.subscribe()
        next_message(fast, "snapshot", lambda s: s["status"] == "ok")
        for step in range(1, 12):
            robot.apply(SetDevice("rio", "TalonFX-2", Dropouts=step))
            next_message(fast, "event", lambda e: e["kind"] == "dropout")
        assert wait_until(lambda: slow.overflowed)
        assert not fast.overflowed
        fast.close()
        slow.close()
    finally:
        service.close()
