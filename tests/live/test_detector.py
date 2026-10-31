import random
from typing import Any

import pytest

from flashpoint.live import contract
from flashpoint.live.detector import Detector, Event
from tests.live.snapshots import Robot

DEVICES = {
    "DriveTrain": ["TalonFX-11", "TalonFX-12", "CANcoder-13"],
    "rio": ["TalonFX-2", "TalonFX-8"],
}
EIGHT = {"DriveTrain": [f"TalonFX-{i}" for i in (11, 12, 21, 22, 31, 32, 41, 42)]}


def kinds(events: list[Event]) -> list[str]:
    return [e.kind for e in events]


def started(robot: Robot, at: float = 100.0) -> Detector:
    detector = Detector()
    assert detector.update(robot.snapshot(), at) == []
    return detector


# --- 3.1 baselines and rising totals ------------------------------------------------------


def test_baseline_is_not_an_event() -> None:
    robot = Robot(DEVICES)
    for bus, names in DEVICES.items():
        for name in names:
            robot.device(bus, name, Dropouts=40, Resets=1, Faults=0b1, Connected=False)
        robot.bus(bus, BusOffCount=7)
    detector = Detector()
    assert detector.update(robot.snapshot(), 1.0) == []
    assert detector.update(robot.snapshot(), 2.0) == []
    robot.device("rio", "TalonFX-2", Dropouts=41)
    assert kinds(detector.update(robot.snapshot(), 3.0)) == ["dropout"]


def test_shake_test_dropout_while_disabled() -> None:
    robot = Robot(DEVICES, enabled=False)
    detector = started(robot)
    robot.device("DriveTrain", "TalonFX-11", Dropouts=2, MaxAgeMs=48.0)
    (event,) = detector.update(robot.snapshot(), 101.5)
    assert event.kind == "dropout"
    assert (event.bus, event.device, event.count, event.max_age_ms) == (
        "DriveTrain", "TalonFX-11", 2, 48.0
    )
    assert event.enabled is False
    assert event.at == 101.5


def test_drive_test_reboot_while_enabled() -> None:
    robot = Robot(DEVICES, enabled=True)
    detector = started(robot)
    robot.device("rio", "TalonFX-2", Resets=1, Faults=0b10)
    events = detector.update(robot.snapshot(), 101.0)
    assert kinds(events) == ["reboot", "fault"]
    assert all(e.enabled is True and e.device == "TalonFX-2" for e in events)
    assert events[0].count == 1
    assert events[1].fault == "boot during enable"


def test_enabled_comes_from_the_same_update() -> None:
    robot = Robot(DEVICES, enabled=False)
    detector = started(robot)
    robot.set("Enabled", True).device("rio", "TalonFX-8", Dropouts=1)
    (event,) = detector.update(robot.snapshot(), 101.0)
    assert event.enabled is True


def test_connected_and_disconnected() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.device("rio", "TalonFX-8", Connected=False)
    (event,) = detector.update(robot.snapshot(), 101.0)
    assert (event.kind, event.device) == ("disconnected", "TalonFX-8")
    assert detector.update(robot.snapshot(), 102.0) == []
    robot.device("rio", "TalonFX-8", Connected=True)
    assert kinds(detector.update(robot.snapshot(), 103.0)) == ["reconnected"]


def test_fault_bit_zero_to_one_only() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.device("rio", "TalonFX-2", Faults=0b101)
    events = detector.update(robot.snapshot(), 101.0)
    assert [e.fault for e in events] == ["under-voltage", "hardware"]
    # Still set: no new event. Hardware clears, bad magnet sets: only bad magnet.
    assert detector.update(robot.snapshot(), 102.0) == []
    robot.device("rio", "TalonFX-2", Faults=0b10001)
    assert [e.fault for e in detector.update(robot.snapshot(), 103.0)] == ["bad magnet"]
    # Cleared then set again counts again.
    robot.device("rio", "TalonFX-2", Faults=0)
    assert detector.update(robot.snapshot(), 104.0) == []
    robot.device("rio", "TalonFX-2", Faults=0b1)
    assert [e.fault for e in detector.update(robot.snapshot(), 105.0)] == ["under-voltage"]


def test_bus_error_counts() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.bus("rio", ReceiveErrorCount=4, TxFullCount=1, Utilization=0.9)
    (event,) = detector.update(robot.snapshot(), 101.0)
    assert (event.kind, event.bus, event.device) == ("bus-errors", "rio", None)
    assert event.counts == {"TxFullCount": 1, "ReceiveErrorCount": 4}
    robot.bus("rio", Utilization=0.2)
    assert detector.update(robot.snapshot(), 102.0) == []


def test_new_device_later_is_its_own_baseline() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.device("rio", "TalonFX-14", Connected=True, Dropouts=9, Resets=2, Faults=1)
    assert detector.update(robot.snapshot(), 101.0) == []
    robot.device("rio", "TalonFX-14", Dropouts=10)
    assert kinds(detector.update(robot.snapshot(), 102.0)) == ["dropout"]


def test_totals_since_the_connection_opened() -> None:
    robot = Robot(DEVICES)
    robot.device("rio", "TalonFX-2", Dropouts=40, Resets=1)
    detector = started(robot)
    robot.device("rio", "TalonFX-2", Dropouts=43, Resets=2)
    detector.update(robot.snapshot(), 101.0)
    device = detector.devices["rio", "TalonFX-2"]
    assert (device.dropouts, device.resets) == (3, 1)


# --- 3.2 sibling rule by batch ------------------------------------------------------------


def test_one_device_drops() -> None:
    robot = Robot(EIGHT)
    detector = started(robot)
    robot.device("DriveTrain", "TalonFX-31", Dropouts=1)
    events = detector.update(robot.snapshot(), 101.0)
    assert kinds(events) == ["dropout"]
    assert events[0].device == "TalonFX-31"
    assert detector.buses["DriveTrain"].bus_dropouts == 0


def test_whole_bus_drops() -> None:
    robot = Robot(EIGHT)
    detector = started(robot)
    dropped = ["TalonFX-11", "TalonFX-12", "TalonFX-21", "TalonFX-22", "TalonFX-31"]
    for name in dropped:
        robot.device("DriveTrain", name, Dropouts=1, MaxAgeMs=30.0)
    (event,) = detector.update(robot.snapshot(), 101.0)
    assert (event.kind, event.bus, event.device) == ("bus-dropout", "DriveTrain", None)
    assert event.devices == dropped
    assert all(detector.devices["DriveTrain", n].dropouts == 0 for n in dropped)
    assert detector.buses["DriveTrain"].bus_dropouts == 1


def test_just_under_half_is_isolated() -> None:
    # 3 of 8: each dropper sees 2 of 7 others, under half.
    robot = Robot(EIGHT)
    detector = started(robot)
    for name in ("TalonFX-11", "TalonFX-12", "TalonFX-21"):
        robot.device("DriveTrain", name, Dropouts=1)
    events = detector.update(robot.snapshot(), 101.0)
    assert kinds(events) == ["dropout"] * 3


def test_half_of_the_others_is_bus_wide() -> None:
    # 2 of 3: each dropper sees 1 of 2 others, exactly half.
    robot = Robot({"DriveTrain": ["TalonFX-11", "TalonFX-12", "CANcoder-13"]})
    detector = started(robot)
    robot.device("DriveTrain", "TalonFX-11", Dropouts=1)
    robot.device("DriveTrain", "TalonFX-12", Dropouts=1)
    assert kinds(detector.update(robot.snapshot(), 101.0)) == ["bus-dropout"]


def test_lone_device_on_a_bus_is_isolated() -> None:
    robot = Robot({"rio": ["TalonFX-2"]})
    detector = started(robot)
    robot.device("rio", "TalonFX-2", Dropouts=1)
    assert kinds(detector.update(robot.snapshot(), 101.0)) == ["dropout"]


def test_other_buses_are_not_siblings() -> None:
    robot = Robot({"DriveTrain": ["TalonFX-11", "TalonFX-12"], "rio": ["TalonFX-2"]})
    detector = started(robot)
    robot.device("DriveTrain", "TalonFX-11", Dropouts=1)
    robot.device("rio", "TalonFX-2", Dropouts=1)
    events = detector.update(robot.snapshot(), 101.0)
    assert sorted((e.kind, e.bus) for e in events) == [
        ("dropout", "DriveTrain"), ("dropout", "rio")
    ]


def test_unmapped_devices_count_as_siblings() -> None:
    # The detector knows nothing of slots: TalonFX-60 (in no robot file) is a sibling like any.
    robot = Robot({"rio": ["TalonFX-2", "TalonFX-60", "TalonFX-61"]})
    detector = started(robot)
    robot.device("rio", "TalonFX-2", Dropouts=1)
    robot.device("rio", "TalonFX-60", Dropouts=1)
    assert kinds(detector.update(robot.snapshot(), 101.0)) == ["bus-dropout"]


# --- 3.3 restarts and bad values ----------------------------------------------------------


def test_robot_code_restarts() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.device("rio", "TalonFX-8", Dropouts=12, Resets=1)
    detector.update(robot.snapshot(), 101.0)
    robot.device("rio", "TalonFX-8", Dropouts=0, Resets=0)
    events = detector.update(robot.snapshot(), 102.0)
    assert kinds(events) == ["restart"]
    # Fresh baseline: the next rise counts from 0.
    robot.device("rio", "TalonFX-8", Dropouts=1)
    (event,) = detector.update(robot.snapshot(), 103.0)
    assert (event.kind, event.count) == ("dropout", 1)


def test_restart_across_two_updates_is_one_restart() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.device("rio", "TalonFX-8", Dropouts=12).device("rio", "TalonFX-2", Resets=3)
    detector.update(robot.snapshot(), 101.0)
    robot.device("rio", "TalonFX-8", Dropouts=0)
    assert kinds(detector.update(robot.snapshot(), 102.0)) == ["restart"]
    robot.device("rio", "TalonFX-2", Resets=0)
    assert detector.update(robot.snapshot(), 102.2) == []


def test_bus_total_going_down_is_a_restart() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.bus("DriveTrain", BusOffCount=5)
    detector.update(robot.snapshot(), 101.0)
    robot.bus("DriveTrain", BusOffCount=0)
    assert kinds(detector.update(robot.snapshot(), 102.0)) == ["restart"]


def test_restart_hides_other_changes_in_that_update() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.device("rio", "TalonFX-8", Dropouts=12)
    detector.update(robot.snapshot(), 101.0)
    robot.device("rio", "TalonFX-8", Dropouts=0).device("rio", "TalonFX-2", Faults=0b10)
    assert kinds(detector.update(robot.snapshot(), 102.0)) == ["restart"]


def test_wrong_type() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.set("rio/TalonFX-8/Dropouts", "lots", "string")
    robot.device("rio", "TalonFX-2", Dropouts=1)
    events = detector.update(robot.snapshot(), 101.0)
    assert kinds(events) == ["dropout"]
    assert events[0].device == "TalonFX-2"
    assert detector.devices["rio", "TalonFX-8"].notes == {"Dropouts": "wrong type: string"}
    # Fixed: the note goes and the value is a fresh baseline for that field.
    robot.set("rio/TalonFX-8/Dropouts", 5)
    assert detector.update(robot.snapshot(), 102.0) == []
    assert detector.devices["rio", "TalonFX-8"].notes == {}


def test_negative_count() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.device("rio", "TalonFX-8", Resets=-1)
    robot.bus("rio", TxFullCount=-4)
    assert detector.update(robot.snapshot(), 101.0) == []
    assert detector.devices["rio", "TalonFX-8"].notes == {"Resets": "negative count: -1"}
    assert detector.buses["rio"].notes == {"TxFullCount": "negative count: -4"}


def test_bad_top_level_values() -> None:
    robot = Robot(DEVICES)
    detector = started(robot)
    robot.set("Enabled", "yes", "string").device("rio", "TalonFX-8", Dropouts=1)
    (event,) = detector.update(robot.snapshot(), 101.0)
    assert event.enabled is None
    assert detector.notes == {"Enabled": "wrong type: string"}


@pytest.mark.parametrize("seed", range(40))
def test_never_raises_on_fuzzed_values(seed: int) -> None:
    rng = random.Random(seed)
    junk: list[tuple[str, Any]] = [
        ("int", -(2**63)), ("int", 2**63 - 1), ("double", float("nan")), ("double", -1e308),
        ("double", float("inf")), ("string", ""), ("string", "x" * 1000), ("boolean", True),
        ("raw", b"\x00\xff"), ("int[]", [1, 2]), ("double", 3), ("int", None),
        ("boolean", 1), ("string", 7), ("int", 2.5), ("int", True),
    ]
    names = [n for bus, ds in DEVICES.items() for n in contract.topic_names(bus, ds)]
    names += [contract.ROOT + "rio/Bus/Utilization", contract.ROOT + "Version"]
    robot = Robot(DEVICES)
    detector = Detector()
    for step in range(30):
        for _ in range(rng.randint(1, 8)):
            type_string, value = rng.choice(junk)
            robot.values[rng.choice(names)] = (type_string, value)
        snapshot = robot.snapshot()
        if rng.random() < 0.1:
            snapshot = {k: v for k, v in snapshot.items() if rng.random() < 0.5}
        detector.update(snapshot, float(step))
        detector.view(float(step))


# --- versions -----------------------------------------------------------------------------


def test_unknown_contract_version() -> None:
    robot = Robot(DEVICES, version=2)
    detector = Detector()
    assert detector.update(robot.snapshot(), 1.0) == []
    robot.device("rio", "TalonFX-2", Dropouts=5)
    assert detector.update(robot.snapshot(), 2.0) == []
    view = detector.view(2.0)
    assert view["status"] == "unsupported"
    assert view["version"] == 2
    assert view["supported_version"] == contract.CONTRACT_VERSION
    assert view["buses"] == []


def test_nothing_published() -> None:
    detector = Detector()
    assert detector.update({}, 1.0) == []
    assert detector.view(1.0)["status"] == "not-publishing"
