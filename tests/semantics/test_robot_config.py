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
