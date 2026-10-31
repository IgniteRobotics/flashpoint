from pathlib import Path

from flashpoint.live.detector import Detector
from flashpoint.live.slots import RobotSlots, load_resolver, resolver_for
from flashpoint.semantics.robot_config import RobotConfig, load_robots
from tests.conftest import REPO_CONFIG
from tests.live.snapshots import Robot


def ignite() -> list[RobotConfig]:
    return load_robots(REPO_CONFIG / "robots")


def robot_2026() -> RobotConfig:
    return next(r for r in ignite() if r.project == "Robot-2026")


def all_2026_devices() -> dict[str, list[str]]:
    robot = robot_2026()
    devices: dict[str, list[str]] = {}
    for slot in robot.slots:
        bus = robot.bus_name_for(slot.bus)
        assert bus is not None
        devices.setdefault(bus, []).append(f"{slot.model}-{slot.can_id}")
    return devices


def test_mapped_device() -> None:
    slots = RobotSlots(robot_2026())
    info = slots.slot_for("DriveTrain", "TalonFX", 11)
    assert info is not None
    assert (info.id, info.role) == ("drive-fl", "front-left drive")
    # Same model and CAN id on the other bus is a different slot (or none).
    assert slots.slot_for("rio", "TalonFX", 11) is None
    assert slots.slot_for("rio", "TalonFX", 10).id == "intake-roller-follower"  # type: ignore[union-attr]
    assert slots.slot_for("DriveTrain", "Pigeon2", 10).id == "gyro"  # type: ignore[union-attr]


def test_unknown_bus_name_is_tried_as_a_hoot_bus() -> None:
    slots = RobotSlots(robot_2026())
    assert slots.slot_for("canivore", "TalonFX", 12).id == "steer-fl"  # type: ignore[union-attr]
    assert slots.slot_for("Elevator", "TalonFX", 99) is None


def test_missing_slot() -> None:
    devices = all_2026_devices()
    devices["DriveTrain"].remove("TalonFX-12")
    robot = Robot(devices)
    detector = Detector(resolver_for(ignite()))
    detector.update(robot.snapshot(), 1.0)
    view = detector.view(1.0)
    missing = [m for bus in view["buses"] for m in bus["missing"]]
    assert [(m["id"], m["bus"], m["model"], m["can_id"]) for m in missing] == [
        ("steer-fl", "DriveTrain", "TalonFX", 12)
    ]
    states = {d["slot"]: d["state"] for bus in view["buses"] for d in bus["devices"]}
    assert len(states) == len(robot_2026().slots) - 1
    assert set(states.values()) == {"ok"}


def test_view_shows_the_slot() -> None:
    robot = Robot({"DriveTrain": ["TalonFX-11", "TalonFX-60"]})
    detector = Detector(resolver_for(ignite()))
    detector.update(robot.snapshot(), 1.0)
    robot.device("DriveTrain", "TalonFX-11", Dropouts=1)
    (event,) = detector.update(robot.snapshot(), 2.0)
    assert event.slot == "drive-fl"
    view = detector.view(2.0)
    assert view["robot"] == "2026-comp"
    assert view["project_found"] is True
    bus = next(b for b in view["buses"] if b["name"] == "DriveTrain")
    by_name = {d["name"]: d for d in bus["devices"]}
    assert (by_name["TalonFX-11"]["slot"], by_name["TalonFX-11"]["state"]) == (
        "drive-fl", "dropped"
    )
    assert (by_name["TalonFX-60"]["slot"], by_name["TalonFX-60"]["state"]) == (None, "unmapped")


def test_unknown_project() -> None:
    robot = Robot({"DriveTrain": ["TalonFX-11"], "rio": ["TalonFX-2"]}, project="Robot-2099")
    detector = Detector(resolver_for(ignite()))
    detector.update(robot.snapshot(), 1.0)
    view = detector.view(1.0)
    assert view["project"] == "Robot-2099"
    assert view["project_found"] is False
    assert view["robot"] is None
    assert {d["state"] for bus in view["buses"] for d in bus["devices"]} == {"unmapped"}
    assert all(bus["missing"] == [] for bus in view["buses"])


def test_latest_season_wins() -> None:
    old = robot_2026().model_copy(update={"robot": "2026-practice", "season": 2025})
    new = robot_2026().model_copy(update={"robot": "2027-comp", "season": 2027})
    resolve = resolver_for([old, robot_2026(), new])
    slots = resolve("Robot-2026")
    assert slots is not None and slots.robot == "2027-comp"
    assert resolve("Robot-2099") is None


def test_load_resolver_from_a_config_root(tmp_path: Path) -> None:
    assert load_resolver(REPO_CONFIG)("Robot-2026").robot == "2026-comp"  # type: ignore[union-attr]
    assert load_resolver(tmp_path)("Robot-2026") is None  # no robots folder
    (tmp_path / "robots").mkdir()
    (tmp_path / "robots" / "broken.toml").write_text("robot = [")
    assert load_resolver(tmp_path)("Robot-2026") is None  # unreadable: nothing maps
