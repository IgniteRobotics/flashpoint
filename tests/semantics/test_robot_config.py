from datetime import date
from pathlib import Path

import pytest

from flashpoint.semantics.robot_config import ConfigError, load_robot, load_robots, select_robot

REPO_CONFIG = Path(__file__).resolve().parents[2] / "config" / "robots"
CANIVORE = "6E9415C3394C485320202050101C18FF"

BASE = """
robot = "r"
season = 2026
project = "Robot-2026"
[[slot]]
id = "a"
bus = "rio"
model = "TalonFX"
can_id = 1
subsystem = "s"
role = "x"
"""


def _write(tmp_path: Path, text: str, name: str = "r.toml") -> Path:
    path = tmp_path / name
    path.write_text(text)
    return path


def test_repo_2026_config_is_valid() -> None:
    robot = load_robot(REPO_CONFIG / "2026-comp.toml")
    assert len(robot.slots) == 23
    assert robot.slot_for("rio", "TalonFX", 10).id == "intake-roller-follower"  # type: ignore[union-attr]
    assert robot.slot_for(CANIVORE, "TalonFX", 11).id == "drive-fl"  # type: ignore[union-attr]
    assert robot.slot_for("rio", "TalonFX", 11) is None
    subsystems = {s.subsystem for s in robot.signals}
    assert {"intake", "shooter", "indexer", "hunter", "drivetrain"} <= subsystems
    cameras = {s.component for s in robot.signals if s.subsystem == "vision"}
    assert cameras == {"front-camera", "left-camera", "right-camera"}
    assert robot.battery_proxy == "gyro"


def test_battery_proxy_is_optional(tmp_path: Path) -> None:
    assert load_robot(_write(tmp_path, BASE)).battery_proxy is None
    declared = BASE.replace('project = "Robot-2026"', 'project = "Robot-2026"\nbattery_proxy = "a"')
    assert load_robot(_write(tmp_path, declared)).battery_proxy == "a"


def test_unknown_battery_proxy_rejected(tmp_path: Path) -> None:
    text = BASE.replace('project = "Robot-2026"', 'project = "Robot-2026"\nbattery_proxy = "pdh"')
    with pytest.raises(ConfigError, match="(?s)r.toml.*'pdh'"):
        load_robot(_write(tmp_path, text))


def test_unknown_key_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="(?s)r.toml.*colour"):
        load_robot(_write(tmp_path, BASE + 'colour = "red"\n'))


def test_duplicate_slot_names_both(tmp_path: Path) -> None:
    dup = (
        BASE
        + '[[slot]]\nid = "b"\nbus = "rio"\nmodel = "TalonFX"\ncan_id = 1\n'
        + 'subsystem = "s"\nrole = "y"\n'
    )
    with pytest.raises(ConfigError, match=r"'a'.*'b'|'b'.*'a'"):
        load_robot(_write(tmp_path, dup))


def test_bad_swap_date_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="swaps"):
        load_robot(_write(tmp_path, BASE + 'swaps = ["not-a-date"]\n'))


def test_swaps_are_dates(tmp_path: Path) -> None:
    robot = load_robot(_write(tmp_path, BASE + 'swaps = ["2026-03-20"]\n'))
    assert robot.slots[0].swaps == [date(2026, 3, 20)]


def test_bus_validation(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="bus"):
        load_robot(_write(tmp_path, BASE.replace('bus = "rio"', 'bus = "can0"')))


def test_select_robot_by_project_and_season(tmp_path: Path) -> None:
    _write(tmp_path, BASE, "a.toml")
    _write(
        tmp_path, BASE.replace("season = 2026", "season = 2025").replace('"r"', '"old"'), "b.toml"
    )
    robots = load_robots(tmp_path)

    assert select_robot(robots, "Robot-2026", "2026").robot == "r"  # type: ignore[union-attr]
    assert select_robot(robots, "Robot-2026", "2025").robot == "old"  # type: ignore[union-attr]
    assert select_robot(robots, "Other", "2026") is None
    assert select_robot(robots, None, "2026").robot == "r"  # type: ignore[union-attr]  # season's only robot
    assert select_robot(robots, None, "2024") is None


BUSES = """
[[bus]]
name = "rio"
hoot = "rio"
[[bus]]
name = "DriveTrain"
hoot = "canivore"
"""


def test_buses_name_the_hoot_bus(tmp_path: Path) -> None:
    robot = load_robot(_write(tmp_path, BASE + BUSES))
    assert robot.bus_name_for("rio") == "rio"
    assert robot.bus_name_for(CANIVORE) == "DriveTrain"
    assert load_robot(_write(tmp_path, BASE)).bus_name_for("rio") is None


def test_bus_with_specific_canivore_id(tmp_path: Path) -> None:
    text = BASE + f'[[bus]]\nname = "DriveTrain"\nhoot = "{CANIVORE}"\n'
    robot = load_robot(_write(tmp_path, text))
    assert robot.bus_name_for(CANIVORE.lower()) == "DriveTrain"
    assert robot.bus_name_for("D4F5AE23394C4853202020500E3418FF") is None


def test_bus_hoot_validated(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="hoot"):
        load_robot(_write(tmp_path, BASE + '[[bus]]\nname = "DriveTrain"\nhoot = "can0"\n'))


def test_duplicate_bus_names_rejected(tmp_path: Path) -> None:
    text = BASE + BUSES + '[[bus]]\nname = "DriveTrain"\nhoot = "rio"\n'
    with pytest.raises(ConfigError, match="DriveTrain"):
        load_robot(_write(tmp_path, text))


TWO_SLOTS = BASE + (
    '[[slot]]\nid = "b"\nbus = "rio"\nmodel = "TalonFX"\ncan_id = 2\nsubsystem = "s"\nrole = "y"\n'
)


def test_repo_2026_sibling_groups() -> None:
    robot = load_robot(REPO_CONFIG / "2026-comp.toml")
    groups = {g.name: g.slots for g in robot.siblings}
    assert groups["drive"] == ["drive-fl", "drive-fr", "drive-bl", "drive-br"]
    assert groups["steer"] == ["steer-fl", "steer-fr", "steer-bl", "steer-br"]
    assert groups["flywheel"] == [
        "flywheel-left-leader",
        "flywheel-right",
        "flywheel-left-follower",
    ]
    assert groups["intake-roller"] == ["intake-roller", "intake-roller-follower"]
    assert groups["intake-extension"] == ["intake-extension", "intake-extension-follower"]
    assert robot.sibling_group("drive-br").name == "drive"  # type: ignore[union-attr]
    assert robot.sibling_group("hood") is None


def test_no_sibling_groups(tmp_path: Path) -> None:
    assert load_robot(_write(tmp_path, BASE)).siblings == []


@pytest.mark.parametrize(
    ("groups", "pattern"),
    [
        (
            '[[siblings]]\nname = "pair"\nslots = ["a", "drive-xx"]\n',
            r"(?s)r\.toml.*pair.*drive-xx",
        ),
        ('[[siblings]]\nname = "solo"\nslots = ["a"]\n', r"(?s)r\.toml.*solo.*two"),
        (
            '[[siblings]]\nname = "g1"\nslots = ["a", "b"]\n'
            '[[siblings]]\nname = "g2"\nslots = ["b", "a"]\n',
            r"(?s)r\.toml.*g2.*('a'|'b')",
        ),
        (
            '[[siblings]]\nname = "g"\nslots = ["a", "b"]\n'
            '[[siblings]]\nname = "g"\nslots = ["a", "b"]\n',
            r"(?s)r\.toml.*duplicate.*'g'",
        ),
        ('[[siblings]]\nname = "g"\nslots = ["a", "a"]\n', r"(?s)r\.toml.*g.*'a'"),
    ],
)
def test_bad_sibling_groups(tmp_path: Path, groups: str, pattern: str) -> None:
    with pytest.raises(ConfigError, match=pattern):
        load_robot(_write(tmp_path, TWO_SLOTS + groups))
