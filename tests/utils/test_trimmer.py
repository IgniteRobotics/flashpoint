from __future__ import annotations

import pandas as pd
import pytest

from utils.trimmer import trim_to_match


def _df(timestamps: list[float], voltages: list[float | None]) -> pd.DataFrame:
    return pd.DataFrame({
        "Timestamp": timestamps,
        "Phoenix6/TalonFX-1/MotorVoltage": voltages,
    })


def test_trim_removes_leading_zeros() -> None:
    df = _df([0.0, 0.02, 0.04, 0.06, 0.08], [0.0, 0.0, 5.0, 6.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert len(result) == 2  # rows at 0.04, 0.06


def test_trim_removes_trailing_zeros() -> None:
    df = _df([0.0, 0.02, 0.04, 0.06, 0.08, 0.10], [0.0, 5.0, 6.0, 0.0, 0.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert len(result) == 2  # rows at 0.02, 0.04


def test_trim_resets_timestamp_to_zero() -> None:
    df = _df([10.0, 10.02, 10.04, 10.06], [0.0, 5.0, 6.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert result["Timestamp"].iloc[0] == pytest.approx(0.0)


def test_trim_preserves_relative_spacing() -> None:
    df = _df([10.0, 10.02, 10.04, 10.06], [0.0, 5.0, 6.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert result["Timestamp"].iloc[1] == pytest.approx(0.02)


def test_trim_returns_original_if_no_activity() -> None:
    df = _df([0.0, 0.02, 0.04], [0.0, 0.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert len(result) == 3


def test_trim_treats_null_as_inactive() -> None:
    df = _df([0.0, 0.02, 0.04, 0.06], [None, None, 5.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert len(result) == 1  # only row at 0.04 (last active)


def test_trim_handles_negative_voltage() -> None:
    df = _df([0.0, 0.02, 0.04, 0.06], [0.0, -5.0, -6.0, 0.0])
    result = trim_to_match(df, voltage_threshold=0.5)
    assert len(result) == 2
