"""Detection over synthetic gold: the anomaly-detection spec scenarios."""

import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np

from flashpoint import config
from flashpoint.anomaly import detect
from flashpoint.lake.paths import LakePaths
from tests.anomaly.gold_builder import GoldLake, steady

SETTINGS = detect.load_settings(config.config_root())


def _rows(lake: LakePaths) -> list[dict[str, Any]]:
    return detect.compute(lake, SETTINGS).rows


def test_no_lookahead(tmp_path: Path) -> None:
    rng = np.random.default_rng(11)
    gold = GoldLake(tmp_path / "a", matches=40, slots=["drive-fl", "drive-fr", "drive-bl",
                                                        "drive-br", "hood"])  # fmt: skip
    for k, slot in enumerate(gold.slot_ids()):
        gold.set(slot, "temp_max_c", list(rng.normal(40 + k, 1.5, 40)))
        gold.set(slot, "stator_current_p95", list(rng.normal(30, 3, 40)))
    gold.set("hood", "temp_max_c", steady(45.0, 22) + steady(56.0, 18))
    gold.set("drive-bl", "isolated_gaps", [2] * 25 + [9] * 15)
    full = _rows(gold.write())
    short_gold = GoldLake(tmp_path / "b", matches=40, slots=gold.slots, series=gold.series)
    short = _rows(short_gold.write(upto=30))

    keys = {gold.match_key(i) for i in range(30)}
    done = {r["id"]: r for r in full if r["last_match_key"] in keys}
    by_id = {r["id"]: r for r in short}
    assert done, "the fixture must produce anomalies inside the first 30 matches"
    for anomaly_id, row in done.items():  # finished by match 30: identical
        assert anomaly_id in by_id
        for column in ("onset_match_key", "last_match_key", "evidence", "severity"):
            assert by_id[anomaly_id][column] == row[column], column
    full_ids = {r["id"]: r for r in full}
    for anomaly_id, row in by_id.items():  # still running at match 30: same id and onset
        assert full_ids[anomaly_id]["onset_match_key"] == row["onset_match_key"]


def test_stable_ids_across_runs(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=25, slots=["hood"])
    gold.set("hood", "temp_max_c", steady(45.0, 20) + steady(55.0, 5))
    lake = gold.write()
    assert _rows(lake) == _rows(lake)


def test_building_baseline(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=3, slots=["hood"])
    gold.set("hood", "temp_max_c", [40.0, 50.0, 60.0])
    computed = detect.compute(gold.write(), SETTINGS)
    assert [r for r in computed.rows if r["method"] in ("DRIFT", "STEP")] == []
    assert computed.building == [
        {"unit": "ctre:HOOD", "robot": "2026-comp", "slot_id": "hood", "matches": 3}
    ]


def test_swapped_in_unit(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=30, slots=["hood"])
    gold.set("hood", "temp_max_c", steady(45.0, 20) + steady(55.0, 10))
    gold.swap("hood", 20, "ctre:HOOD-NEW")
    rows = _rows(gold.write())
    new = [r for r in rows if r["subject"] == "ctre:HOOD-NEW" and r["method"] in ("DRIFT", "STEP")]
    assert new == []  # its own history is steady; the old unit's is never used for it
    old = [r for r in rows if r["subject"] == "ctre:HOOD" and r["method"] in ("DRIFT", "STEP")]
    assert old == []


def test_missing_residual(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=25, slots=["hood"])
    gold.set("hood", "residual_p95_a", [None] * 25)
    gold.set("hood", "temp_max_c", steady(45.0, 20) + steady(55.0, 5))
    computed = detect.compute(gold.write(), SETTINGS)
    assert [r for r in computed.rows if r["metric"] == "residual_p95_a"] == []
    assert [r["metric"] for r in computed.rows if r["method"] == "STEP"] == ["temp_max_c"]
    assert computed.frame.missing["residual_p95_a"] == 25


def test_low_alignment_is_judged_and_flagged(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=3, slots=["hood"])
    gold.set("hood", "temp_max_c", [40.0, 66.0, 40.0])
    gold.low_alignment.add(1)
    (row,) = [r for r in _rows(gold.write()) if r["method"] == "SPEC"]
    assert row["low_alignment"] is True


def test_synthetic_q105(tmp_path: Path) -> None:
    """The Johnson matches as the real lake has them: the roller leader reboots while enabled
    after an 820 ms isolated gap in Q105."""
    gold = GoldLake(tmp_path, matches=4, slots=["intake-roller", "intake-extension-follower"])
    gold.keys = ["2026johnson_qm15", "2026johnson_qm53", "2026johnson_qm76", "2026johnson_qm105"]
    gold.set("intake-roller", "reboots", [0, 0, 0, 1])
    gold.set("intake-roller", "reboots_enabled", [0, 0, 0, 1])
    gold.set("intake-roller", "longest_gap_ms", [26.0, 0.0, 59.0, 820.0])
    gold.set("intake-extension-follower", "isolated_gaps", [7, 20, 1, 42])
    rows = {(r["subject"], r["metric"]): r for r in _rows(gold.write())}
    reboot = rows[("ctre:INTAKE-ROLLER", "reboots")]
    gap = rows[("ctre:INTAKE-ROLLER", "longest_gap_ms")]
    assert (reboot["method"], reboot["severity"], reboot["onset_match_key"]) == (
        "SPEC", "HIGH", "2026johnson_qm105",
    )  # fmt: skip
    assert (gap["method"], gap["severity"], gap["actual"]) == ("SPEC", "MED", "820 ms")
    assert gap["onset_match_key"] == "2026johnson_qm105"
    assert ("ctre:INTAKE-EXTENSION-FOLLOWER", "isolated_gaps") not in rows  # building baseline


def test_sibling_in_detection(tmp_path: Path) -> None:
    drive = ["drive-fl", "drive-fr", "drive-bl", "drive-br"]
    gold = GoldLake(tmp_path, matches=20, slots=drive)
    for k, slot in enumerate(drive):
        gold.set(slot, "temp_max_c", steady(40.0, 20, start=k))
    hot = [v + (6.0 if i >= 15 else 0) for i, v in enumerate(steady(40.0, 20, start=2))]
    gold.set("drive-bl", "temp_max_c", hot)
    rows = [r for r in _rows(gold.write()) if r["method"] == "SIBLING"]
    assert {r["subject"] for r in rows} == {"ctre:DRIVE-BL"}
    assert json.loads(rows[0]["evidence"])["peer"] == "median of drive-fl, drive-fr, drive-br"


def _two_events(root: Path, step_at: int) -> GoldLake:
    """10 matches at 2026aaa then 6 at 2026bbb; the hood's max temperature steps 45 -> 55 °C
    at match index `step_at`."""
    keys = [f"2026aaa_qm{i + 1}" for i in range(10)] + [f"2026bbb_qm{i + 1}" for i in range(6)]
    gold = GoldLake(root, matches=16, slots=["hood"], keys=keys)
    gold.set("hood", "temp_max_c", steady(45.0, step_at) + steady(55.0, 16 - step_at))
    return gold


def _steps(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in rows if r["metric"] == "temp_max_c" and r["method"] in ("STEP", "DRIFT")]


def test_step_at_an_event_boundary_is_a_quiet_event_change(tmp_path: Path) -> None:
    found = _steps(_rows(_two_events(tmp_path, 10).write()))
    assert [(r["method"], r["onset_match_key"]) for r in found] == [("STEP", "2026bbb_qm1")]
    step = found[0]
    assert (step["severity"], step["sensitivity"]) == ("LOW", "strict")
    assert "changed between events" in step["title"]
    assert "2026aaa" in step["why"] and "2026bbb" in step["why"]


def test_step_inside_an_event_is_unchanged(tmp_path: Path) -> None:
    found = _steps(_rows(_two_events(tmp_path, 12).write()))
    assert [(r["method"], r["onset_match_key"]) for r in found] == [("STEP", "2026bbb_qm3")]
    assert (found[0]["severity"], found[0]["sensitivity"]) == ("HIGH", "relaxed")
    assert "between events" not in found[0]["title"]


def test_event_boundary_rule_can_be_switched_off(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    shutil.copytree(config.config_root(), config_dir)
    toml = config_dir / "anomaly.toml"
    text = toml.read_text().replace("quiet_event_steps = true", "quiet_event_steps = false")
    toml.write_text(text)
    settings = detect.load_settings(config_dir)
    found = _steps(detect.compute(_two_events(tmp_path / "lake", 10).write(), settings).rows)
    assert [(r["method"], r["onset_match_key"]) for r in found] == [("STEP", "2026bbb_qm1")]
    assert (found[0]["severity"], found[0]["sensitivity"]) == ("HIGH", "relaxed")
