import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from typing import Any

import pytest

from flashpoint.live import contract
from flashpoint.live.fake_robot import (
    IGNITE_2026,
    FakeRobot,
    Restart,
    SetBus,
    SetDevice,
    SetEnabled,
    SetVersion,
    WrongType,
    run_scenario,
)
from tests.live.nt_client import TopicReader

DEVICES = {
    "DriveTrain": ["TalonFX-11", "TalonFX-12", "CANcoder-13"],
    "rio": ["TalonFX-2", "TalonFX-8"],
}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def robot() -> Iterator[FakeRobot]:
    with FakeRobot(DEVICES, port=free_port()) as fake:
        yield fake


@pytest.fixture
def reader(robot: FakeRobot) -> Iterator[TopicReader]:
    with TopicReader(robot.port) as client:
        client.wait_for("Version", 1)
        yield client


def test_publishes_the_whole_contract(reader: TopicReader) -> None:
    reader.wait_for("rio/Bus/TransmitErrorCount", 0)
    expected = sorted(
        {name for bus, devices in DEVICES.items() for name in contract.topic_names(bus, devices)}
    )
    assert reader.topics() == expected
    for name in expected:
        topic = contract.parse_topic(name)
        assert topic is not None
        fields = {**contract.TOP_FIELDS, **contract.DEVICE_FIELDS, **contract.BUS_FIELDS}
        assert reader.type_of(name[len(contract.ROOT):]) == fields[topic.field]
    assert reader.value("Project") == "Robot-2026"
    assert reader.value("Enabled") is False
    assert reader.value("DriveTrain/TalonFX-11/Connected") is True


def test_set_totals_and_faults(robot: FakeRobot, reader: TopicReader) -> None:
    robot.apply(SetDevice("DriveTrain", "TalonFX-11", Dropouts=2, MaxAgeMs=48.0))
    assert reader.wait_for("DriveTrain/TalonFX-11/Dropouts", 2) == 2
    assert reader.value("DriveTrain/TalonFX-11/MaxAgeMs") == 48.0
    robot.apply(SetDevice("rio", "TalonFX-2", Resets=1, Faults=0b10))
    assert reader.wait_for("rio/TalonFX-2/Faults", 2) == 2
    assert reader.value("rio/TalonFX-2/Resets") == 1
    robot.apply(SetBus("rio", BusOffCount=3, Utilization=0.6))
    assert reader.wait_for("rio/Bus/BusOffCount", 3) == 3
    assert reader.value("rio/Bus/Utilization") == 0.6


def test_toggle_enabled(robot: FakeRobot, reader: TopicReader) -> None:
    robot.apply(SetEnabled(True))
    assert reader.wait_for("Enabled", True) is True
    robot.apply(SetEnabled(False))
    assert reader.wait_for("Enabled", False) is False


def test_restart_zeroes_every_total(robot: FakeRobot, reader: TopicReader) -> None:
    robot.apply(SetDevice("rio", "TalonFX-8", Dropouts=12, Resets=1))
    robot.apply(SetBus("DriveTrain", ReceiveErrorCount=5))
    assert reader.wait_for("rio/TalonFX-8/Dropouts", 12) == 12
    robot.apply(Restart())
    assert reader.wait_for("rio/TalonFX-8/Dropouts", 0) == 0
    assert reader.value("rio/TalonFX-8/Resets") == 0
    assert reader.wait_for("DriveTrain/Bus/ReceiveErrorCount", 0) == 0


def test_wrong_type(robot: FakeRobot, reader: TopicReader) -> None:
    robot.apply(WrongType("rio/TalonFX-8/Dropouts", "lots"))
    assert reader.wait_for("rio/TalonFX-8/Dropouts", "lots") == "lots"
    assert reader.type_of("rio/TalonFX-8/Dropouts") == "string"
    robot.apply(SetDevice("rio", "TalonFX-2", Dropouts=1))
    assert reader.wait_for("rio/TalonFX-2/Dropouts", 1) == 1


def test_version_two(robot: FakeRobot, reader: TopicReader) -> None:
    robot.apply(SetVersion(2))
    assert reader.wait_for("Version", 2) == 2


def test_not_publishing() -> None:
    with FakeRobot(DEVICES, port=free_port(), publish=False) as fake, TopicReader(fake.port) as r:
        assert r.wait_connected()
        assert r.wait_for("Version", 1, timeout_s=0.5) is None
        assert r.topics() == []


@pytest.mark.parametrize("scenario", ["shake", "drive", "restart"])
def test_scenario_runs_and_stops(scenario: str) -> None:
    port = free_port()
    stop = threading.Event()
    result: list[int] = []
    runner = threading.Thread(
        target=lambda: result.append(run_scenario(scenario, port, 0.02, stop, seed=1))
    )
    runner.start()
    try:
        with TopicReader(port) as reader:
            assert reader.wait_for("Version", 1) == 1
            first = _totals(reader)
            deadline = time.monotonic() + 3
            while _totals(reader) == first and time.monotonic() < deadline:
                time.sleep(0.05)
            assert _totals(reader) != first
    finally:
        stop.set()
        runner.join(5)
    assert not runner.is_alive()
    assert result and result[0] > 0


def _totals(reader: TopicReader) -> tuple[Any, ...]:
    return tuple(
        reader.value(f"{bus}/{device}/{field}")
        for bus, devices in IGNITE_2026.items()
        for device in devices
        for field in ("Dropouts", "Resets")
    ) + tuple(reader.value(f"{bus}/Bus/ReceiveErrorCount") for bus in IGNITE_2026) + (
        reader.value("Enabled"),
    )


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT to a child needs a console")
def test_module_stops_cleanly_on_interrupt() -> None:
    port = free_port()
    child = subprocess.Popen(
        [sys.executable, "-m", "flashpoint.live.fake_robot", "--scenario", "shake",
         "--port", str(port), "--period", "0.05"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        with TopicReader(port) as reader:
            assert reader.wait_for("Version", 1) == 1
        child.send_signal(signal.SIGINT)
        out, err = child.communicate(timeout=10)
    finally:
        if child.poll() is None:
            child.kill()
    assert child.returncode == 0, err
    assert "Traceback" not in err
    assert "stopped after" in out
