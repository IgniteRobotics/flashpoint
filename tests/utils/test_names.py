from __future__ import annotations

import pytest
from pathlib import Path

from utils import names


TOML_CONTENT = """\
[default]
1 = "FL Drive"
2 = "FR Drive"
5 = "Intake"

[GACMP]
5 = "Intake V2"

[GACOL]
5 = "Intake V3"
2 = "Arm Pivot"
"""


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    f = tmp_path / "motor-names.toml"
    f.write_text(TOML_CONTENT)
    return f


def test_load_returns_dict_with_sections(config_file: Path) -> None:
    config = names.load(config_file)
    assert "default" in config
    assert "GACMP" in config
    assert "GACOL" in config


def test_load_default_section_values(config_file: Path) -> None:
    config = names.load(config_file)
    assert config["default"]["1"] == "FL Drive"
    assert config["default"]["5"] == "Intake"


def test_resolve_default_only(config_file: Path) -> None:
    config = names.load(config_file)
    result = names.resolve("UNKNOWN_Q1", config)
    assert result == {
        "TalonFX-1": "FL Drive",
        "TalonFX-2": "FR Drive",
        "TalonFX-5": "Intake",
    }


def test_resolve_competition_overrides_merge_with_default(config_file: Path) -> None:
    config = names.load(config_file)
    result = names.resolve("GACMP_E5", config)
    assert result["TalonFX-1"] == "FL Drive"   # from default
    assert result["TalonFX-2"] == "FR Drive"   # from default
    assert result["TalonFX-5"] == "Intake V2"  # overridden by GACMP


def test_resolve_different_competition(config_file: Path) -> None:
    config = names.load(config_file)
    result = names.resolve("GACOL_E8", config)
    assert result["TalonFX-5"] == "Intake V3"
    assert result["TalonFX-2"] == "Arm Pivot"
    assert result["TalonFX-1"] == "FL Drive"   # from default


def test_resolve_unknown_motor_not_in_result(config_file: Path) -> None:
    config = names.load(config_file)
    result = names.resolve("GACMP_E5", config)
    assert "TalonFX-99" not in result


def test_resolve_empty_config() -> None:
    result = names.resolve("GACMP_Q1", {})
    assert result == {}
