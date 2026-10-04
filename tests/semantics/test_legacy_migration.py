from pathlib import Path

from flashpoint.semantics.legacy_migration import migrate_device_maps
from flashpoint.semantics.robot_config import RobotConfig

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

    import tomllib

    robot = RobotConfig.model_validate(tomllib.loads(toml_text))
    assert {(s.bus, s.can_id, s.subsystem, s.role) for s in robot.slots} == {
        ("rio", 6, "elevator", "lead motor"),
        ("rio", 15, "climber", "motor"),
        ("canivore", 11, "drivetrain", "front left drive motor"),
    }
    assert any("Pheonix6/TalonFX-15/Position" in line and "corrected" in line for line in report)
    assert any("climber/getPosition" in line and "unmapped" in line for line in report)
