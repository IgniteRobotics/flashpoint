"""CAN-gap spread floors scale with frame rate (anomaly-detection, mvp-outside-team-logs 4.2)."""

from pathlib import Path
from typing import Any

import numpy as np
import pytest

from flashpoint import config
from flashpoint.anomaly import detect
from flashpoint.anomaly.floors import floors
from tests.anomaly.gold_builder import GoldLake

SETTINGS = detect.load_settings(config.config_root())
DRIVE = ["drive-fl", "drive-fr", "drive-bl", "drive-br"]  # a CANivore sibling group
CAN_GAP = ("isolated_gaps", "missed_per_min")
SHAPES = ("DRIFT", "STEP", "SIBLING")


def _can_gap(rows: list[dict[str, Any]], method: str | None = None) -> list[dict[str, Any]]:
    return [
        r for r in rows
        if r["metric"] in CAN_GAP and (method is None or r["method"] == method)
    ]  # fmt: skip


def _drive_lake(tmp_path: Path, rate: float | None, matches: int = 20) -> GoldLake:
    gold = GoldLake(tmp_path, matches=matches, slots=DRIVE)
    for slot in DRIVE:
        gold.set(slot, "isolated_gaps", [0] * matches)
        gold.set(slot, "missed_per_min", [0.0] * matches)
        gold.set(slot, "frame_rate_hz", [rate] * matches)
    return gold


def test_jitter_on_a_250_hz_canivore(tmp_path: Path) -> None:
    gold = _drive_lake(tmp_path, 250.0)
    gold.set("drive-fl", "isolated_gaps", [0] * 15 + [9] * 5)
    gold.set("drive-fl", "missed_per_min", [0.0] * 15 + [3.7] * 5)
    gold.set("drive-fl", "longest_gap_ms", [0.0] * 15 + [12.0] * 5)
    rows = detect.compute(gold.write(), SETTINGS).rows
    assert [r for r in _can_gap(rows) if r["method"] in SHAPES] == []


def test_the_same_jitter_at_rio_rate_is_still_flagged(tmp_path: Path) -> None:
    """The fixture above is a finding without scaling, so its test is not vacuous."""
    gold = _drive_lake(tmp_path, 100.0)
    gold.set("drive-fl", "isolated_gaps", [0] * 15 + [9] * 5)
    gold.set("drive-fl", "missed_per_min", [0.0] * 15 + [3.7] * 5)
    rows = detect.compute(gold.write(), SETTINGS).rows
    assert [r for r in _can_gap(rows) if r["method"] in SHAPES]


def test_real_dropout_still_found(tmp_path: Path) -> None:
    """1 s silent at 250 Hz in 2.5 enabled minutes: ~249 frames, ~100 missed per minute."""
    gold = _drive_lake(tmp_path, 250.0)
    gold.set("drive-fl", "isolated_gaps", [0] * 15 + [1] * 5)
    gold.set("drive-fl", "missed_per_min", [0.0] * 15 + [99.6] * 5)
    gold.set("drive-fl", "longest_gap_ms", [0.0] * 15 + [1000.0] * 5)
    rows = [
        r for r in detect.compute(gold.write(), SETTINGS).rows if r["subject"] == "ctre:DRIVE-FL"
    ]

    spec = [r for r in rows if r["method"] == "SPEC" and r["metric"] == "longest_gap_ms"]
    assert [r["severity"] for r in spec] == ["HIGH"]
    steps = [r for r in rows if r["method"] == "STEP" and r["metric"] == "missed_per_min"]
    assert [r["onset_match_key"] for r in steps] == [gold.match_key(15)]
    assert "250 frames/s" in steps[0]["why"]
    assert "spread floor was 5 /min, scaled with the rate" in steps[0]["why"]


def _drift_history(gold: GoldLake, slot: str) -> GoldLake:
    """A normal-sensitivity DRIFT with the configured floor: 7 gaps on a steady 0 is 2.8 floors."""
    gold.set(slot, "isolated_gaps", [0] * 15 + [7] * 5)
    return gold


def _shape(rows: list[dict[str, Any]]) -> list[tuple[str, ...]]:
    return sorted(
        (r["subject"], r["method"], r["metric"], r["severity"], r["sensitivity"],
         r["onset_match_key"])
        for r in rows
    )  # fmt: skip


@pytest.mark.parametrize("rate", [105.0, 4.0, 50.0])
def test_rio_and_slow_devices_keep_todays_floor(tmp_path: Path, rate: float) -> None:
    unrated = _drift_history(GoldLake(tmp_path / "a", matches=20, slots=["hood"]), "hood")
    rated = _drift_history(GoldLake(tmp_path / "b", matches=20, slots=["hood"]), "hood")
    rated.set("hood", "frame_rate_hz", [rate] * 20)
    before = detect.compute(unrated.write(), SETTINGS).rows
    after = detect.compute(rated.write(), SETTINGS).rows

    assert _shape(after) == _shape(before)
    drifts = [r for r in after if r["method"] == "DRIFT" and r["metric"] == "isolated_gaps"]
    assert [r["sensitivity"] for r in drifts] == ["normal"]
    assert f"{rate:.0f} frames/s" in drifts[0]["why"]
    assert "as configured (floors scale only above 125 frames/s)" in drifts[0]["why"]


def test_rows_without_a_frame_rate(tmp_path: Path) -> None:
    gold = _drift_history(GoldLake(tmp_path, matches=20, slots=["hood"]), "hood")
    computed = detect.compute(gold.write(), SETTINGS)
    summary = detect.summarize(computed, "fp")

    assert summary["can_rows_without_frame_rate"] == 20
    (drift,) = [
        r for r in computed.rows if r["method"] == "DRIFT" and r["metric"] == "isolated_gaps"
    ]
    assert "No frame rate was recorded" in drift["why"]


def test_floor_uses_the_median_rate_of_the_window_and_the_match() -> None:
    rates = np.array([250.0, 250.0, np.nan, 100.0, 100.0, 100.0])
    got = floors("isolated_gaps", 2.5, rates, window=2)
    assert list(got) == [12.5, 12.5, 2.5, 8.75, 2.5, 2.5]  # type: ignore[arg-type]
    assert floors("temp_max_c", 1.0, rates, window=2) == 1.0
    assert floors("isolated_gaps", 2.5, np.full(3, np.nan), window=2) == 2.5
