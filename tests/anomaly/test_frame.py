from pathlib import Path

import polars as pl

from flashpoint.anomaly.frame import load_frame
from flashpoint.lake.paths import LakePaths
from tests.anomaly.gold_builder import GoldLake


def test_whole_match_rows_joined_and_ordered(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=6, slots=["drive-fl", "hood"])
    gold.set("hood", "isolated_gaps", [0, 1, 2, 3, 4, 5])
    gold.set("hood", "temp_max_c", [50.0, 51.0, 52.0, 53.0, 54.0, 55.0])
    gold.keys = [f"2026syn_qm{n}" for n in (10, 2, 33, 4, 5, 6)]  # sorts wrongly as text
    frame = load_frame(gold.write())

    hood = frame.rows.filter(pl.col("slot_id") == "hood")
    assert hood["match_key"].to_list() == gold.keys
    assert hood["temp_max_c"].to_list() == [50.0, 51.0, 52.0, 53.0, 54.0, 55.0]
    assert hood["isolated_gaps"].to_list() == [0, 1, 2, 3, 4, 5]
    assert frame.rows.height == 12  # the auto rows are not read
    assert frame.matches["2026-comp"] == gold.keys
    assert frame.skipped == {}


def test_missing_values_stay_null_per_metric(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=3, slots=["drive-fl"])
    gold.set("drive-fl", "residual_p95_a", [None, None, None])
    frame = load_frame(gold.write())
    assert frame.rows["residual_p95_a"].null_count() == 3
    assert frame.rows["temp_max_c"].null_count() == 0
    assert frame.missing["residual_p95_a"] == 3


def test_can_health_without_features_and_features_without_can(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=3, slots=["drive-fl"])
    gold.no_can.add(1)
    lake = gold.write()
    frame = load_frame(lake)
    assert frame.rows["isolated_gaps"].to_list() == [2, None, 2]
    assert frame.rows["temp_max_c"].to_list() == [40.0, 40.0, 40.0]


def test_skipped_rows_counted_by_reason(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=3, slots=["drive-fl"])
    lake = gold.write()
    # a second session for the same match (a duplicate log): the fuller session wins
    extra = lake.root / "gold" / "match_features" / "season=2026" / "session_id=dup" / "p.parquet"
    extra.parent.mkdir(parents=True)
    source = lake.root / "gold" / "match_features" / "season=2026" / "session_id=2026syn-s000"
    pl.read_parquet(source / "part-0.parquet").filter(pl.col("phase") == "match").with_columns(
        session_id=pl.lit("dup"), unit_id=pl.lit(None, pl.String)
    ).write_parquet(extra)
    # a session with no start time (no unit_usage row)
    lonely = lake.root / "gold" / "match_features" / "season=2026" / "session_id=lonely"
    lonely.mkdir()
    pl.read_parquet(source / "part-0.parquet").with_columns(
        session_id=pl.lit("lonely"), match_key=pl.lit("2026syn_qm99")
    ).write_parquet(lonely / "p.parquet")

    frame = load_frame(lake)
    assert frame.rows.height == 3
    assert frame.skipped == {"duplicate-session": 1, "no-start-time": 1}


def test_low_alignment_is_carried(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=3, slots=["drive-fl"])
    gold.low_alignment.add(1)
    frame = load_frame(gold.write())
    assert frame.rows["alignment"].to_list() == ["high", "low", "high"]


def test_empty_lake(tmp_path: Path) -> None:
    frame = load_frame(LakePaths(tmp_path))
    assert frame.rows.is_empty() and frame.matches == {} and frame.skipped == {}
