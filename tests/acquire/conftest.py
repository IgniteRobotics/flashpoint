from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.pulls import PullLedger
from flashpoint.acquire.robot import AuthErrorThrottle, RobotClient
from tests.acquire.fake_robot import FakeRobot

ROBOT_ROOTS = ["/home/lvuser/logs", "/u/logs"]


@pytest.fixture
def fake_robot(tmp_path: Path) -> Iterator[FakeRobot]:
    """A running fake roboRIO serving ``tmp_path / "rio"``; stopped at teardown."""
    robot = FakeRobot(tmp_path / "rio")
    robot.start()
    try:
        yield robot
    finally:
        robot.stop()


@pytest.fixture
def pull_ledger(tmp_path: Path) -> Iterator[PullLedger]:
    ledger = PullLedger(tmp_path / "lake" / "meta" / "flashpoint.sqlite")
    yield ledger
    ledger.close()


@pytest.fixture
def connect_robot(fake_robot: FakeRobot, tmp_path: Path) -> Iterator[Callable[[], RobotClient]]:
    """Connects a fresh `RobotClient` to the fake robot (one per cycle); all closed at teardown."""
    clients: list[RobotClient] = []
    config = AcquireConfig(hosts=[f"{fake_robot.host}:{fake_robot.port}"], roots=ROBOT_ROOTS)

    def connect() -> RobotClient:
        client = RobotClient.connect(
            config,
            known_robots=tmp_path / "known-robots.json",
            throttle=AuthErrorThrottle(),
            tcp_timeout=0.5,
            auth_timeout=5.0,
        )
        assert client is not None
        clients.append(client)
        return client

    yield connect
    for client in clients:
        client.close()


@pytest.fixture
def robot_client(connect_robot: Callable[[], RobotClient]) -> RobotClient:
    return connect_robot()
