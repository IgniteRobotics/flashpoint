"""Detection without temperature from non-Pro hoots (anomaly-detection, "Partial and malformed
inputs"; mvp-outside-team-logs 6.7)."""

from pathlib import Path

from flashpoint import config
from flashpoint.anomaly import detect
from tests.anomaly.gold_builder import GoldLake, steady

SETTINGS = detect.load_settings(config.config_root())
TEMPERATURE = ("temp_max_c", "temp_mean_c", "temp_rise_c_per_min")
SLOTS = ["drive-fl", "drive-fr", "drive-bl", "drive-br", "hood"]


def test_non_pro_season(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=30, slots=SLOTS, non_pro=set(range(30)))
    for slot in SLOTS:
        for metric in TEMPERATURE:
            gold.set(slot, metric, [None] * 30)
    gold.set("hood", "stator_current_p95", steady(40.0, 20) + steady(70.0, 10))
    computed = detect.compute(gold.write(), SETTINGS)

    assert [r for r in computed.rows if r["metric"] in TEMPERATURE] == []
    assert [r for r in computed.rows if r["metric"] == "stator_current_p95"], "current is judged"
    summary = detect.summarize(computed, "fp")
    # whole-match rows of 5 units x 30 matches, counted once each
    assert summary["temperature_needs_pro"] == {"label": "not logged (needs Phoenix Pro)",
                                                 "rows": 150}  # fmt: skip


def test_licence_lapses_mid_season(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=30, slots=SLOTS, non_pro=set(range(15, 30)))
    for k, slot in enumerate(SLOTS):
        for metric in TEMPERATURE:
            gold.set(slot, metric, steady(40.0 + k, 15) + [None] * 15)
    rows = detect.compute(gold.write(), SETTINGS).rows

    late = {gold.match_key(i) for i in range(15, 30)}
    temperature = [r for r in rows if r["metric"] in TEMPERATURE]
    assert [r for r in temperature if r["onset_match_key"] in late] == []
    assert [
        r for r in temperature if r["method"] == "SIBLING" and r["last_match_key"] in late
    ] == []
    assert (
        detect.summarize(detect.compute(gold.lake, SETTINGS), "fp")["temperature_needs_pro"]["rows"]
        == 75
    )


def test_pro_season_counts_none(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=6, slots=["hood"])
    summary = detect.summarize(detect.compute(gold.write(), SETTINGS), "fp")
    assert summary["temperature_needs_pro"]["rows"] == 0
