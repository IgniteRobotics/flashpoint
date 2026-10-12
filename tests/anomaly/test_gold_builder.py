from pathlib import Path

import duckdb

from tests.anomaly.gold_builder import GoldLake, steady


def _read(root: Path, table: str, column: str, slot: str) -> list[tuple[str, str, object]]:
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT f.match_key, f.unit_id, f.{column} FROM read_parquet(?, union_by_name = true) f"
        " JOIN read_parquet(?) u USING (session_id, slot_id)"
        " WHERE f.phase = 'match' AND f.slot_id = ? ORDER BY u.session_start",
        [str(root / "gold" / table / "*" / "*" / "*.parquet"),
         str(root / "gold" / "unit_usage" / "*" / "*" / "*.parquet"), slot],
    ).fetchall()  # fmt: skip
    return [(k, u, v) for k, u, v in rows]


def test_round_trip_in_match_order(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=12, slots=["drive-fl", "intake-roller"])
    gold.set("drive-fl", "temp_max_c", steady(45.0, 12))
    gold.set("intake-roller", "isolated_gaps", list(range(12)))
    gold.swap("drive-fl", 8, "ctre:NEW")
    gold.absent.add(("intake-roller", 3))
    gold.low_alignment.add(5)
    gold.write()

    temps = _read(tmp_path, "match_features", "temp_max_c", "drive-fl")
    assert [k for k, _, _ in temps] == [f"2026syn_qm{i + 1}" for i in range(12)]
    assert [v for _, _, v in temps] == steady(45.0, 12)
    assert [u for _, u, _ in temps] == ["ctre:DRIVE-FL"] * 8 + ["ctre:NEW"] * 4
    gaps = _read(tmp_path, "can_health", "isolated_gaps", "intake-roller")
    assert [v for _, _, v in gaps] == [i for i in range(12) if i != 3]


def test_partial_write_and_defaults(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path, matches=10, slots=["hood"])
    gold.write(upto=4)
    temps = _read(tmp_path, "match_features", "temp_max_c", "hood")
    assert [v for _, _, v in temps] == [40.0] * 4
