import json
import tomllib
from pathlib import Path

import pytest

from flashpoint.semantics.legacy_migration import (
    migrate_device_maps,
    migrate_nt_maps,
    reference_entries,
)
from flashpoint.semantics.robot_config import RobotConfig, SeasonConfig, Slot, load_robot
from tests.wpilog_builder import WpilogBuilder

HEADER = "entry,subsystem,assembly,subassembly,component,metric\n"


def test_migrates_device_rows_and_fixes_spelling(tmp_path: Path) -> None:
    rio = tmp_path / "rio.csv"
    rio.write_text(
        HEADER
        + "Phoenix6/TalonFX-6/SupplyCurrent,ELEVATOR,,,LEAD MOTOR,CURRENT\n"
        + "Phoenix6/TalonFX-15/Velocity,CLIMBER,,,MOTOR,VELOCITY\n"
        + "Pheonix6/TalonFX-15/Position,CLIMBER,,,MOTOR,POSITION\n"
    )
    drive = tmp_path / "drive.csv"
    drive.write_text(
        HEADER + "Phoenix6/TalonFX-11/SupplyCurrent,DRIVETRAIN,FRONT_LEFT,DRIVE,MOTOR,CURRENT\n"
    )
    nt = tmp_path / "metrics.csv"
    nt.write_text(HEADER + "climber/getPosition,CLIMBER,,,,POSITION\n")

    toml_text, report = migrate_device_maps(
        robot="2025-comp", season=2025, project="Robot-2025",
        rio_map=rio, canivore_map=drive, nt_maps=[nt],
    )  # fmt: skip

    robot = RobotConfig.model_validate(tomllib.loads(toml_text))
    assert {(s.bus, s.can_id, s.subsystem, s.role) for s in robot.slots} == {
        ("rio", 6, "elevator", "lead motor"),
        ("rio", 15, "climber", "motor"),
        ("canivore", 11, "drivetrain", "front left drive motor"),
    }
    assert any("Pheonix6/TalonFX-15/Position" in line and "corrected" in line for line in report)
    assert any("climber/getPosition" in line and "unmapped" in line for line in report)


# --- NetworkTables maps and the log configuration ----------------------------------------


ROBOT_ROOT = "NT:Robot/m_robotContainer/"
LOG_CONFIG = {
    "metadata_prefix": "MetaData",
    "metrics_prefix": ROBOT_ROOT,
    "preferences_prefix": "NT:/Preferences/",
    "fms_prefix": "NT:/FMSInfo/",
    "photon_prefix": "NT:/photonvision/",
    "camerapub_prefix": "NT:/CameraPublisher/",
}
SLOTS = [
    Slot(id="corraler-motor-2", bus="rio", model="TalonFX", can_id=2, subsystem="corraler",
         role="motor"),
    Slot(id="algae-collector-wrist-motor-4", bus="rio", model="TalonFX", can_id=4,
         subsystem="algae collector", role="wrist motor"),
]  # fmt: skip


@pytest.fixture
def migrated(tmp_path: Path) -> tuple[str, str, list[str]]:
    metrics = tmp_path / "metrics_map.csv"
    metrics.write_text(
        HEADER
        + "corraler/Motor Current,Corraler,,,MOTOR,CURRENT\n"
        + "corraler/m_beambreak_enter/Is Detected,Corraler,,,TOP BEAMBREAK,\n"
        + "collector/Wrist Motor Position,ALGAE COLLECTOR,WRIST,,MOTOR,POSITION\n"
        + "climber/getPosition,CLIMBER,,,,POSITION\n"
        + "climber/Pose History,CLIMBER,,,,POSE\n"
    )
    vision = tmp_path / "vision_map.csv"
    vision.write_text(
        "entry,camera,metric\n"
        "INTAKE/targetPitch,INTAKE,\n"
        "INTAKE/hasTarget,INTAKE,HAS_TARGET\n"
        "INTAKE/targetPose,INTAKE,\n"
    )
    log_config = tmp_path / "config2025.json"
    log_config.write_text(json.dumps(LOG_CONFIG))
    log = (
        WpilogBuilder()
        .start(1, ROBOT_ROOT + "corraler/m_beambreak_enter/Is Detected", "boolean")
        .start(2, ROBOT_ROOT + "climber/getPosition", "double")
        .start(3, ROBOT_ROOT + "climber/Pose History", "double[]")
        .start(4, "NT:/photonvision/INTAKE/targetPitch", "double")
        .start(5, "NT:/photonvision/INTAKE/hasTarget", "boolean")
        .start(6, "NT:/photonvision/INTAKE/targetPose", "struct:Transform3d")
        .start(7, "MetaData/BuildDate", "string")
        .write(tmp_path / "reference.wpilog")
    )
    return migrate_nt_maps(2025, metrics, vision, log_config, reference_entries(log), SLOTS)


def _signals(robot_toml: str) -> dict[str, dict[str, str]]:
    return {s["entry"]: s for s in tomllib.loads(robot_toml)["signal"]}


def test_motor_rows_superseded_by_can_slot(migrated: tuple[str, str, list[str]]) -> None:
    _, signals, report = migrated
    assert "corraler/Motor Current" not in _signals(signals)
    assert any(
        "corraler/Motor Current" in line and "superseded by CAN slot corraler-motor-2" in line
        for line in report
    )
    assert any(
        "collector/Wrist Motor Position" in line
        and "superseded by CAN slot algae-collector-wrist-motor-4" in line
        for line in report
    )


def test_absent_and_non_numeric_entries_reported(migrated: tuple[str, str, list[str]]) -> None:
    _, signals, report = migrated
    assert "climber/Pose History" not in _signals(signals)
    assert any(
        "climber/Pose History" in line and "non-numeric in reference log" in line for line in report
    )
    assert any(
        "INTAKE/targetPose" in line and "non-numeric in reference log" in line for line in report
    )


def test_absent_entry_reported(tmp_path: Path) -> None:
    metrics = tmp_path / "m.csv"
    metrics.write_text(HEADER + "elevator/getPosition,ELEVATOR,,,,POSITION\n")
    vision = tmp_path / "v.csv"
    vision.write_text("entry,camera,metric\n")
    log_config = tmp_path / "c.json"
    log_config.write_text(json.dumps({"metrics_prefix": ROBOT_ROOT}))
    _, signals, report = migrate_nt_maps(2025, metrics, vision, log_config, {}, [])
    assert signals.strip() == ""
    assert any("elevator/getPosition" in line and "not in reference log" in line for line in report)


def test_beam_break_and_vision_rows_become_signals(migrated: tuple[str, str, list[str]]) -> None:
    _, signals, _ = migrated
    by_entry = _signals(signals)
    beam = by_entry["corraler/m_beambreak_enter/Is Detected"]
    assert (beam["root"], beam["subsystem"], beam["component"], beam["metric"]) == (
        "robot", "corraler", "top-beambreak", "is_detected",
    )  # fmt: skip
    position = by_entry["climber/getPosition"]
    assert (position["metric"], "component" in position) == ("position", False)
    pitch = by_entry["INTAKE/targetPitch"]
    assert (pitch["root"], pitch["subsystem"], pitch["component"], pitch["metric"]) == (
        "photonvision", "vision", "intake", "target_pitch",
    )  # fmt: skip
    assert by_entry["INTAKE/hasTarget"]["metric"] == "has_target"


def test_every_log_config_prefix_is_a_root_or_reported(
    migrated: tuple[str, str, list[str]],
) -> None:
    season_toml, _, report = migrated
    season = SeasonConfig.model_validate(tomllib.loads(season_toml))
    assert season.root_names() == {
        "robot": ROBOT_ROOT,
        "photonvision": "NT:/photonvision/",
        "camera_publisher": "NT:/CameraPublisher/",
        "preferences": "NT:/Preferences/",
        "fms": "NT:/FMSInfo/",
    }
    assert any(
        "metadata_prefix" in line and "non-numeric in reference log" in line for line in report
    )


def test_migrated_files_load_together(migrated: tuple[str, str, list[str]], tmp_path: Path) -> None:
    season_toml, signals, _ = migrated
    (tmp_path / "cfg" / "seasons").mkdir(parents=True)
    (tmp_path / "cfg" / "robots").mkdir()
    (tmp_path / "cfg" / "seasons" / "2025.toml").write_text(season_toml)
    robot = tmp_path / "cfg" / "robots" / "r.toml"
    robot.write_text(
        'robot = "r"\nseason = 2025\nproject = "P"\n'
        '[[slot]]\nid = "a"\nbus = "rio"\nmodel = "TalonFX"\ncan_id = 1\n'
        'subsystem = "s"\nrole = "x"\n' + signals
    )
    assert len(load_robot(robot).signals) == 4
