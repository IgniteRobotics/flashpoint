from __future__ import annotations

import numpy as np
import pandas as pd

from .hoot_loader import MOTOR_COL_PATTERN


def _voltage_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if MOTOR_COL_PATTERN.match(c) and "MotorVoltage" in c]


def trim_to_match(df: pd.DataFrame, voltage_threshold: float = 0.5) -> pd.DataFrame:
    """Trim DataFrame to the active match window.

    Finds the first and last row where any motor voltage exceeds the threshold.
    Resets Timestamp to start at 0. Nulls are treated as 0 for detection.
    Returns the original DataFrame unchanged if no activity is found.
    """
    vol_cols = _voltage_cols(df)
    if not vol_cols:
        return df

    voltages = df[vol_cols].fillna(0.0)
    active = (voltages.abs() > voltage_threshold).any(axis=1)
    active_indices = np.where(active)[0]

    if len(active_indices) == 0:
        return df

    start_idx = int(active_indices[0])
    end_idx = int(active_indices[-1])

    trimmed = df.iloc[start_idx : end_idx + 1].copy()
    trimmed["Timestamp"] = trimmed["Timestamp"] - trimmed["Timestamp"].iloc[0]
    return trimmed.reset_index(drop=True)
