"""Temperature status in gold (mvp-outside-team-logs 6.2; lifetime-trends, match-reports)."""

from pathlib import Path
from typing import Any

import duckdb
import pytest

from flashpoint.lake.paths import LakePaths
from flashpoint.semantics import gold
from flashpoint.semantics.silver import silver_dir
from flashpoint.semantics.temperature import pro_by_bus, temp_status
from tests.semantics.test_gold import FRAMING, ROBOT, SESSION, _lake, _rows


@pytest.mark.parametrize(
    ("model", "pro", "has", "status"),
    [
        ("TalonFX", True, True, "logged"),
        ("TalonFX", False, True, "logged"),
        ("TalonFX", False, False, "needs-pro"),
        ("TalonFX", True, False, "missing"),
        ("TalonFX", None, False, "missing"),
        ("CANcoder", False, False, "not-tracked"),
        ("Pigeon2", True, False, "not-tracked"),
        ("CANrange", False, False, "needs-pro"),  # unknown to the list: temperature-capable
        (None, False, False, "needs-pro"),
    ],
)
def test_temp_status(model: str | None, pro: bool | None, has: bool, status: str) -> None:
    assert temp_status(model, pro, has) == status


def test_pro_by_bus() -> None:
    hoots = [("rio", 0), ("rio", 0), ("can", 1), ("can", 0), ("x", None), ("y", None), ("y", 0)]
    assert pro_by_bus(hoots) == {"rio": False, "can": True, "x": None, "y": False}


def _with_temperature(lake: LakePaths, slot: str) -> None:
    import polars as pl

    path = silver_dir(lake) / "season=2026" / "session_id=s1" / "part-1.parquet"
    pl.DataFrame(_rows(slot, "temp_c", [(t / 2, 40.0 + t / 10) for t in range(70)])).write_parquet(
        path
    )


def _statuses(lake: LakePaths, slot_pro: dict[str, bool | None]) -> dict[str, set[Any]]:
    rows = gold.session_features(
        duckdb.connect(), lake, SESSION, FRAMING, ROBOT, "high", "", slot_pro=slot_pro
    )
    out: dict[str, set[Any]] = {}
    for row in rows:
        out.setdefault(row["slot_id"], set()).add(row["temp_status"])
    return out


def test_all_non_pro_lake(tmp_path: Path) -> None:
    lake = _lake(tmp_path)
    assert _statuses(lake, {"drive-fl": False, "drive-fr": False}) == {
        "drive-fl": {"needs-pro"},
        "drive-fr": {"needs-pro"},
    }


def test_mixed_bus_lake(tmp_path: Path) -> None:
    lake = _lake(tmp_path)
    _with_temperature(lake, "drive-fr")
    assert _statuses(lake, {"drive-fl": True, "drive-fr": True}) == {
        "drive-fl": {"missing"},
        "drive-fr": {"logged"},
    }
    assert _statuses(lake, {"drive-fl": False, "drive-fr": True})["drive-fl"] == {"needs-pro"}


def test_no_pro_flag_is_missing(tmp_path: Path) -> None:
    assert _statuses(_lake(tmp_path), {}) == {"drive-fl": {"missing"}, "drive-fr": {"missing"}}


@pytest.mark.corpus
def test_corpus_q7_statuses(derived_corpus_lake: LakePaths) -> None:
    def statuses(table: str) -> dict[str, set[str]]:
        glob = (gold.gold_dir(derived_corpus_lake, table) / "*" / "*" / "*.parquet").as_posix()
        rel = duckdb.sql(
            f"SELECT slot_id, temp_status FROM read_parquet('{glob}', hive_partitioning = false)"
            " WHERE match_key = '2026gacmp_qm7'"
        )
        assert rel is not None
        rows = rel.fetchall()
        out: dict[str, set[str]] = {}
        for slot, status in rows:
            out.setdefault(slot, set()).add(status)
        return out

    models = {s.id: s.model for s in ROBOT.slots}
    can = statuses("can_health")
    assert can["gyro"] == {"not-tracked"}
    assert all(can[s] == {"not-tracked"} for s in can if models[s] == "CANcoder")
    assert all(can[s] == {"logged"} for s in can if models[s] == "TalonFX")
    features = statuses("match_features")
    assert features and all(v == {"logged"} for v in features.values())
