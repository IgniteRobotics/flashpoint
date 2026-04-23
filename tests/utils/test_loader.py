from __future__ import annotations
from pathlib import Path

import pytest

from utils.loader import MOTOR_COL_PATTERN, extract_match_id, find_matches, load_match, get_motor_ids


def test_extract_match_id_with_uuid(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_a1b2c3d4-e29b-41d4-a716-446655440000_20240315.csv"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_fallback_dash(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5-rio.csv"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_fallback_underscore(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_CARNIVORE.csv"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_with_hex32_device_id(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_6E9415C3394C485320202050101C18FF_filtered.csv"
    assert extract_match_id(p) == "GACMP_E5"


def test_find_matches_groups_uuid_and_dash_files(tmp_path: Path) -> None:
    rio = tmp_path / "GACMP_E5-rio.csv"
    carnivore = tmp_path / "GACMP_E5_a1b2c3d4-e29b-41d4-a716-446655440000.csv"
    rio.touch()
    carnivore.touch()
    groups = find_matches([rio, carnivore])
    assert list(groups.keys()) == ["GACMP_E5"]
    assert len(groups["GACMP_E5"]) == 2


def test_find_matches_separates_different_match_ids(tmp_path: Path) -> None:
    q1 = tmp_path / "GACMP_Q1-rio.csv"
    q2 = tmp_path / "GACMP_Q2-rio.csv"
    q1.touch()
    q2.touch()
    groups = find_matches([q1, q2])
    assert len(groups) == 2
    assert "GACMP_Q1" in groups
    assert "GACMP_Q2" in groups


def test_load_match_single_file_returns_dataframe(rio_csv: Path) -> None:
    df = load_match([rio_csv])
    assert "Timestamp" in df.columns
    assert "Phoenix6/TalonFX-1/MotorVoltage" in df.columns
    assert df["Timestamp"].is_monotonic_increasing


def test_load_match_merges_two_canbus_files(rio_csv: Path, carnivore_csv: Path) -> None:
    df = load_match([rio_csv, carnivore_csv])
    assert "Phoenix6/TalonFX-1/MotorVoltage" in df.columns
    assert "Phoenix6/TalonFX-11/MotorVoltage" in df.columns
    assert df["Timestamp"].is_monotonic_increasing


def test_load_match_parses_null_as_nan(rio_csv: Path) -> None:
    df = load_match([rio_csv])
    for col in df.columns:
        assert df[col].dtype != object or df[col].isna().all()


def test_get_motor_ids_returns_sorted_list(rio_csv: Path, carnivore_csv: Path) -> None:
    df = load_match([rio_csv, carnivore_csv])
    ids = get_motor_ids(df)
    nums = [int(mid.split("-")[1]) for mid in ids]
    assert nums == sorted(nums)
    assert "TalonFX-1" in ids
    assert "TalonFX-11" in ids


def test_motor_col_pattern_matches_velocity() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/Velocity")


def test_motor_col_pattern_velocity_captures_motor_id() -> None:
    m = MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-42/Velocity")
    assert m is not None
    assert m.group(1) == "42"


def test_motor_col_pattern_does_not_match_unknown_signal() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/Temperature") is None
