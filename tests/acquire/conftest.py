from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.acquire.fake_robot import FakeRobot


@pytest.fixture
def fake_robot(tmp_path: Path) -> Iterator[FakeRobot]:
    """A running fake roboRIO serving ``tmp_path / "rio"``; stopped at teardown."""
    robot = FakeRobot(tmp_path / "rio")
    robot.start()
    try:
        yield robot
    finally:
        robot.stop()
