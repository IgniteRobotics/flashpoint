from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from utils.hoot_loader import (
    MOTOR_COL_PATTERN,
    _pivot_records,
    _run_owlet,
    convert_hoot,
    extract_match_id,
    find_matches,
    get_motor_ids,
    merge_dataframes,
)


def test_convert_hoot_returns_cache_path_on_hit(tmp_path: Path) -> None:
    hoot = tmp_path / "GACMP_Q1_rio_2025-01-01_00-00-00.hoot"
    hoot.write_bytes(b"fake hoot content")

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    h = hashlib.sha256(b"fake hoot content").hexdigest()
    cache_csv = cache_dir / f"{h}.csv"
    cache_csv.write_text(
        "Timestamp,Phoenix6/TalonFX-1/MotorVoltage\n0.0,5.0\n"
    )

    result = convert_hoot(hoot, cache_dir)

    assert result == cache_csv


class _FakeStartData:
    def __init__(self, entry_id: int, name: str, type_: str) -> None:
        self.entry = entry_id
        self.name = name
        self.type = type_


class _FakeRecord:
    def __init__(
        self,
        *,
        is_start: bool = False,
        is_finish: bool = False,
        entry: int = 0,
        timestamp: int = 0,
        value: float | None = None,
        start_data: _FakeStartData | None = None,
    ) -> None:
        self._is_start = is_start
        self._is_finish = is_finish
        self.entry = entry
        self.timestamp = timestamp
        self._value = value
        self._start_data = start_data

    def isStart(self) -> bool:
        return self._is_start

    def isFinish(self) -> bool:
        return self._is_finish

    def isSetMetadata(self) -> bool:
        return False

    def isControl(self) -> bool:
        return False

    def getStartData(self) -> _FakeStartData:
        return self._start_data  # type: ignore[return-value]

    def getDouble(self) -> float:
        return self._value  # type: ignore[return-value]


def _make_records(
    entries: list[tuple[int, str, str]],
    data: list[tuple[int, int, float]],
) -> list[_FakeRecord]:
    """entries=(entry_id, name, type_), data=(entry_id, timestamp_us, value)"""
    records: list[_FakeRecord] = [
        _FakeRecord(is_start=True, start_data=_FakeStartData(eid, name, t))
        for eid, name, t in entries
    ]
    records += [
        _FakeRecord(entry=eid, timestamp=ts, value=val)
        for eid, ts, val in data
    ]
    return records


def test_pivot_records_produces_wide_dataframe() -> None:
    records = _make_records(
        entries=[(1, "Phoenix6/TalonFX-11/MotorVoltage", "double")],
        data=[(1, 1_000_000, 5.0), (1, 2_000_000, 6.0)],
    )
    df = _pivot_records(records)
    assert "Timestamp" in df.columns
    assert "Phoenix6/TalonFX-11/MotorVoltage" in df.columns
    assert list(df["Phoenix6/TalonFX-11/MotorVoltage"]) == [5.0, 6.0]
    assert df["Timestamp"].iloc[0] == pytest.approx(1.0)
    assert df["Timestamp"].iloc[1] == pytest.approx(2.0)


def test_pivot_records_filters_non_talon_entries() -> None:
    records = _make_records(
        entries=[
            (1, "Phoenix6/TalonFX-11/MotorVoltage", "double"),
            (2, "DS:enabled", "boolean"),
            (3, "elevator/Leader Motor Voltage", "double"),
        ],
        data=[
            (1, 1_000_000, 5.0),
            (2, 1_000_000, 1.0),
            (3, 1_000_000, 12.0),
        ],
    )
    df = _pivot_records(records)
    assert "DS:enabled" not in df.columns
    assert "elevator/Leader Motor Voltage" not in df.columns
    assert "Phoenix6/TalonFX-11/MotorVoltage" in df.columns


def test_pivot_records_returns_empty_dataframe_when_no_talon_entries() -> None:
    records = _make_records(
        entries=[(1, "DS:enabled", "boolean")],
        data=[(1, 1_000_000, 1.0)],
    )
    df = _pivot_records(records)
    assert df.empty


def test_pivot_records_sorts_by_timestamp() -> None:
    records = _make_records(
        entries=[(1, "Phoenix6/TalonFX-11/MotorVoltage", "double")],
        data=[
            (1, 3_000_000, 3.0),
            (1, 1_000_000, 1.0),
            (1, 2_000_000, 2.0),
        ],
    )
    df = _pivot_records(records)
    assert df["Timestamp"].is_monotonic_increasing


def test_pivot_records_handles_late_start_for_different_entry() -> None:
    # Start for entry 2 arrives after data for entry 1 — real log files do this
    records = [
        _FakeRecord(is_start=True, start_data=_FakeStartData(1, "Phoenix6/TalonFX-1/MotorVoltage", "double")),
        _FakeRecord(entry=1, timestamp=1_000_000, value=5.0),
        _FakeRecord(is_start=True, start_data=_FakeStartData(2, "Phoenix6/TalonFX-2/MotorVoltage", "double")),
        _FakeRecord(entry=2, timestamp=1_000_000, value=3.0),
        _FakeRecord(entry=1, timestamp=2_000_000, value=6.0),
    ]
    df = _pivot_records(records)
    assert "Phoenix6/TalonFX-1/MotorVoltage" in df.columns
    assert "Phoenix6/TalonFX-2/MotorVoltage" in df.columns
    assert df["Phoenix6/TalonFX-1/MotorVoltage"].notna().sum() == 2
    assert df["Phoenix6/TalonFX-2/MotorVoltage"].notna().sum() == 1


def test_pivot_records_silently_drops_data_before_start_record() -> None:
    # Data records that arrive before their Start record are silently dropped
    records = [
        _FakeRecord(entry=1, timestamp=500_000, value=9.9),  # no Start yet — dropped
        _FakeRecord(is_start=True, start_data=_FakeStartData(1, "Phoenix6/TalonFX-1/MotorVoltage", "double")),
        _FakeRecord(entry=1, timestamp=1_000_000, value=5.0),
    ]
    df = _pivot_records(records)
    assert len(df) == 1
    assert df["Phoenix6/TalonFX-1/MotorVoltage"].iloc[0] == 5.0


def test_convert_hoot_converts_on_cache_miss(tmp_path: Path) -> None:
    hoot = tmp_path / "GACMP_Q1_rio_2025-01-01_00-00-00.hoot"
    hoot.write_bytes(b"fake hoot content")
    cache_dir = tmp_path / "cache"

    sample_df = pd.DataFrame({
        "Timestamp": [0.0, 0.02],
        "Phoenix6/TalonFX-1/MotorVoltage": [5.0, 6.0],
    })

    with (
        patch("utils.hoot_loader._run_owlet"),
        patch("utils.hoot_loader._wpilog_to_df", return_value=sample_df),
    ):
        result = convert_hoot(hoot, cache_dir)

    assert result is not None
    assert result.exists()
    df = pd.read_csv(result)
    assert "Phoenix6/TalonFX-1/MotorVoltage" in df.columns
    assert list(df["Timestamp"]) == pytest.approx([0.0, 0.02])


def test_run_owlet_exits_on_owlet_failure(tmp_path: Path) -> None:
    fake_exe = tmp_path / "fake_owlet"
    fake_exe.touch()

    mock_result = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="", stderr="owlet error message"
    )
    with (
        patch("utils.hoot_loader._owlet_path", return_value=fake_exe),
        patch("utils.hoot_loader.subprocess.run", return_value=mock_result),
    ):
        with pytest.raises(SystemExit) as exc_info:
            _run_owlet(tmp_path / "test.hoot", tmp_path / "test.wpilog")
    assert exc_info.value.code == 1


def test_run_owlet_exits_when_executable_not_found(tmp_path: Path) -> None:
    missing_exe = tmp_path / "owlet_does_not_exist"
    # do NOT create the file

    with patch("utils.hoot_loader._owlet_path", return_value=missing_exe):
        with pytest.raises(SystemExit) as exc_info:
            _run_owlet(tmp_path / "test.hoot", tmp_path / "test.wpilog")
    assert exc_info.value.code == 1


def test_convert_hoot_returns_none_when_no_talon_entries(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    hoot = tmp_path / "GACMP_Q1_rio_2025-01-01_00-00-00.hoot"
    hoot.write_bytes(b"fake hoot content")
    cache_dir = tmp_path / "cache"

    with (
        patch("utils.hoot_loader._run_owlet"),
        patch("utils.hoot_loader._wpilog_to_df", return_value=pd.DataFrame()),
    ):
        result = convert_hoot(hoot, cache_dir)

    assert result is None
    captured = capsys.readouterr()
    assert "warning" in captured.err.lower()


def test_convert_hoot_reconverts_corrupt_cache(tmp_path: Path) -> None:
    hoot = tmp_path / "GACMP_Q1_rio_2025-01-01_00-00-00.hoot"
    hoot.write_bytes(b"fake hoot content")

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    h = hashlib.sha256(b"fake hoot content").hexdigest()
    cache_csv = cache_dir / f"{h}.csv"
    # Write invalid UTF-8 bytes that trigger a UnicodeDecodeError on read
    cache_csv.write_bytes(b"\xff\xfe\x00\x00invalid")

    sample_df = pd.DataFrame({
        "Timestamp": [0.0],
        "Phoenix6/TalonFX-1/MotorVoltage": [5.0],
    })

    with (
        patch("utils.hoot_loader._run_owlet"),
        patch("utils.hoot_loader._wpilog_to_df", return_value=sample_df),
    ):
        result = convert_hoot(hoot, cache_dir)

    assert result == cache_csv
    assert result.exists()
    df = pd.read_csv(result)
    assert "Phoenix6/TalonFX-1/MotorVoltage" in df.columns


# --- MOTOR_COL_PATTERN ---

def test_motor_col_pattern_matches_device_temp() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/DeviceTemp")


def test_motor_col_pattern_device_temp_captures_motor_id() -> None:
    m = MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-42/DeviceTemp")
    assert m is not None
    assert m.group(1) == "42"


def test_motor_col_pattern_matches_velocity() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/Velocity")


def test_motor_col_pattern_does_not_match_unknown_signal() -> None:
    assert MOTOR_COL_PATTERN.match("Phoenix6/TalonFX-1/Temperature") is None


# --- extract_match_id ---

def test_extract_match_id_strips_timestamp_suffix(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_Q1_rio_2026-04-23_14-30-00.hoot"
    assert extract_match_id(p) == "GACMP_Q1"


def test_extract_match_id_strips_uuid(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_a1b2c3d4-e29b-41d4-a716-446655440000.hoot"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_strips_hex32(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_6E9415C3394C485320202050101C18FF_filtered.hoot"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_fallback_dash(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5-rio.hoot"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_hex32_with_timestamp(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_6E9415C3394C485320202050101C18FF_2026-04-11_18-32-18.hoot"
    assert extract_match_id(p) == "GACMP_E5"


def test_extract_match_id_rio_with_timestamp(tmp_path: Path) -> None:
    p = tmp_path / "GACMP_E5_rio_2026-04-11_18-32-18.hoot"
    assert extract_match_id(p) == "GACMP_E5"


# --- find_matches ---

def test_find_matches_groups_by_match_id(tmp_path: Path) -> None:
    f1 = tmp_path / "GACMP_E5-rio.hoot"
    f2 = tmp_path / "GACMP_E5_6E9415C3394C485320202050101C18FF.hoot"
    f1.touch()
    f2.touch()
    groups = find_matches([f1, f2])
    assert list(groups.keys()) == ["GACMP_E5"]
    assert len(groups["GACMP_E5"]) == 2


def test_find_matches_separates_different_matches(tmp_path: Path) -> None:
    f1 = tmp_path / "GACMP_Q1-rio.hoot"
    f2 = tmp_path / "GACMP_Q2-rio.hoot"
    f1.touch()
    f2.touch()
    groups = find_matches([f1, f2])
    assert len(groups) == 2


# --- get_motor_ids ---

def test_get_motor_ids_returns_sorted_list() -> None:
    df = pd.DataFrame(columns=[
        "Timestamp",
        "Phoenix6/TalonFX-11/MotorVoltage",
        "Phoenix6/TalonFX-1/MotorVoltage",
    ])
    ids = get_motor_ids(df)
    nums = [int(mid.split("-")[1]) for mid in ids]
    assert nums == sorted(nums)
    assert "TalonFX-1" in ids
    assert "TalonFX-11" in ids


# --- merge_dataframes ---

def test_merge_dataframes_single_df_sorted() -> None:
    df = pd.DataFrame({
        "Timestamp": [0.04, 0.02, 0.0],
        "Phoenix6/TalonFX-1/MotorVoltage": [3.0, 2.0, 1.0],
    })
    result = merge_dataframes([df])
    assert result["Timestamp"].is_monotonic_increasing


def test_merge_dataframes_outer_joins_two_dfs() -> None:
    df1 = pd.DataFrame({
        "Timestamp": [0.0, 0.02],
        "Phoenix6/TalonFX-1/MotorVoltage": [5.0, 6.0],
    })
    df2 = pd.DataFrame({
        "Timestamp": [0.0, 0.02],
        "Phoenix6/TalonFX-11/MotorVoltage": [3.0, 4.0],
    })
    result = merge_dataframes([df1, df2])
    assert "Phoenix6/TalonFX-1/MotorVoltage" in result.columns
    assert "Phoenix6/TalonFX-11/MotorVoltage" in result.columns
    assert result["Timestamp"].is_monotonic_increasing
