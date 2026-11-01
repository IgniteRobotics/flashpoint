import hashlib
import sqlite3
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from flashpoint.lake import bronze, raw
from flashpoint.lake.ledger import Ledger, Stage
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.query import connect


@pytest.fixture
def lake(tmp_path: Path) -> LakePaths:
    paths = LakePaths(tmp_path / "lake")
    paths.ensure()
    return paths


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --- ledger ------------------------------------------------------------------------------


def test_register_new_file_then_duplicate_under_alias(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    assert ledger.register("abc", "wpilog", 10, Path("/x/a.wpilog")) is True
    assert ledger.register("abc", "wpilog", 10, Path("/y/b.wpilog")) is False
    assert sorted(ledger.aliases("abc")) == ["/x/a.wpilog", "/y/b.wpilog"]
    assert ledger.stage("abc") == Stage.RECEIVED


def test_success_is_terminal_for_same_pipeline_version(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    ledger.register("abc", "wpilog", 10, Path("a.wpilog"))
    ledger.set_stage("abc", Stage.SUCCESS, pipeline_version=1)
    assert ledger.needs_processing("abc", pipeline_version=1) is False
    assert ledger.needs_processing("abc", pipeline_version=2) is True


def test_quarantine_records_reason(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    ledger.register("abc", "hoot", 0, Path("e.hoot"))
    ledger.quarantine("abc", "empty-file", pipeline_version=1)
    assert ledger.stage("abc") == Stage.QUARANTINED
    assert ledger.reason("abc") == "empty-file"


def test_interrupted_file_is_reprocessed(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    ledger.register("abc", "wpilog", 10, Path("a.wpilog"))
    ledger.set_stage("abc", Stage.RECEIVED, pipeline_version=1)
    reopened = Ledger(lake.ledger)
    assert reopened.needs_processing("abc", pipeline_version=1) is True


def test_failed_records_reason_and_counts_attempts(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    ledger.register("abc", "hoot", 10, Path("a.hoot"))
    assert ledger.fail("abc", "owlet-failed", pipeline_version=1) == 1
    assert ledger.fail("abc", "owlet-timeout", pipeline_version=1) == 2
    assert ledger.stage("abc") == Stage.FAILED
    assert ledger.reason("abc") == "owlet-timeout"
    assert ledger.attempts("abc") == 2


def test_success_resets_attempts(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    ledger.register("abc", "hoot", 10, Path("a.hoot"))
    ledger.fail("abc", "owlet-failed", pipeline_version=1)
    ledger.set_stage("abc", Stage.SUCCESS, pipeline_version=1)
    assert ledger.attempts("abc") == 0


def test_pipeline_version_change_restarts_attempts(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    ledger.register("abc", "hoot", 10, Path("a.hoot"))
    ledger.fail("abc", "owlet-failed", pipeline_version=1)
    ledger.fail("abc", "owlet-failed", pipeline_version=1)
    assert ledger.fail("abc", "owlet-failed", pipeline_version=2) == 1


def test_failed_is_retried_and_quarantined_is_final(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    for sha in ("f", "q", "s"):
        ledger.register(sha, "hoot", 10, Path(f"{sha}.hoot"))
    ledger.fail("f", "owlet-download-failed", pipeline_version=1)
    ledger.quarantine("q", "empty-file", pipeline_version=1)
    ledger.set_stage("s", Stage.SUCCESS, pipeline_version=1)
    assert ledger.needs_processing("f", pipeline_version=1) is True
    assert ledger.needs_processing("q", pipeline_version=1) is False
    assert ledger.needs_processing("s", pipeline_version=1) is False


def test_files_snapshot_carries_attempts(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    ledger.register("abc", "hoot", 10, Path("a.hoot"))
    ledger.fail("abc", "owlet-failed", pipeline_version=1)
    ledger.export_snapshots(lake.meta)
    rows = duckdb.sql(f"select stage, attempts from '{lake.meta / 'files.parquet'}'").fetchall()
    assert rows == [("failed", 1)]


_OLD_FILES = """
CREATE TABLE files (
    sha256 TEXT PRIMARY KEY, kind TEXT NOT NULL, size INTEGER NOT NULL,
    stage TEXT NOT NULL, reason TEXT, warnings TEXT, pipeline_version INTEGER,
    first_seen TEXT NOT NULL, updated TEXT NOT NULL
);
"""


def _old_lake(lake: LakePaths) -> None:
    db = sqlite3.connect(lake.ledger)
    db.executescript(_OLD_FILES)
    for sha, reason in [
        ("dl", "owlet-download-failed"),
        ("crash", "owlet-failed"),
        ("oops", "internal-error:OSError"),
        ("bad", "invalid-header"),
    ]:
        db.execute(
            "INSERT INTO files VALUES (?, 'hoot', 1, 'quarantined', ?, NULL, 1, 't', 't')",
            (sha, reason),
        )
    db.commit()
    db.close()


def test_old_lake_gains_attempts_and_machine_quarantines_become_failed(lake: LakePaths) -> None:
    _old_lake(lake)
    ledger = Ledger(lake.ledger)
    rows = {r["sha256"]: (r["stage"], r["attempts"]) for r in ledger.files()}
    assert rows == {
        "dl": ("failed", 0),
        "crash": ("failed", 0),
        "oops": ("failed", 0),
        "bad": ("quarantined", 0),
    }
    assert ledger.reason("dl") == "owlet-download-failed"


def test_opening_an_old_lake_twice_is_a_noop(lake: LakePaths) -> None:
    _old_lake(lake)
    Ledger(lake.ledger).close()
    first = Ledger(lake.ledger).files()
    second = Ledger(lake.ledger).files()
    assert first == second


def test_reopening_keeps_a_quarantine_after_three_failures(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    ledger.register("abc", "hoot", 10, Path("a.hoot"))
    ledger.quarantine("abc", "owlet-failed", pipeline_version=1)
    ledger.close()
    assert Ledger(lake.ledger).stage("abc") == Stage.QUARANTINED


def test_ledger_exports_parquet_snapshots(lake: LakePaths) -> None:
    ledger = Ledger(lake.ledger)
    ledger.register("abc", "wpilog", 10, Path("a.wpilog"))
    ledger.export_snapshots(lake.meta)
    assert (lake.meta / "files.parquet").is_file()
    rows = duckdb.sql(f"select sha256 from '{lake.meta / 'files.parquet'}'").fetchall()
    assert rows == [("abc",)]


# --- raw ---------------------------------------------------------------------------------


def test_raw_store_is_content_addressed_and_verified(lake: LakePaths, tmp_path: Path) -> None:
    data = b"hello log"
    src = tmp_path / "in.wpilog"
    src.write_bytes(data)
    stored = raw.store(lake, src, _sha(data), ".wpilog")

    assert stored == lake.raw / _sha(data)[:2] / f"{_sha(data)}.wpilog"
    assert stored.read_bytes() == data
    assert raw.store(lake, src, _sha(data), ".wpilog") == stored  # idempotent


def test_raw_store_rejects_hash_mismatch(lake: LakePaths, tmp_path: Path) -> None:
    src = tmp_path / "in.wpilog"
    src.write_bytes(b"data")
    with pytest.raises(ValueError, match="hash"):
        raw.store(lake, src, "0" * 64, ".wpilog")


# --- bronze ------------------------------------------------------------------------------


def _table(n: int = 4) -> pa.Table:
    return pa.table(
        {
            "signal": pa.array(["b", "a", "b", "a"][:n]).dictionary_encode(),
            "type": pa.array(["double"] * n).dictionary_encode(),
            "ts_us": pa.array([3, 2, 1, 4][:n], pa.int64()),
            "v_f64": pa.array([1.0, 2.0, 3.0, 4.0][:n]),
            "v_i64": pa.nulls(n, pa.int64()),
            "v_bool": pa.nulls(n, pa.bool_()),
            "v_str": pa.nulls(n, pa.large_string()),
            "v_bytes": pa.nulls(n, pa.large_binary()),
        }
    )


def test_bronze_write_is_sorted_and_partitioned(lake: LakePaths) -> None:
    path = bronze.write(lake, _table(), season="2026", log_id="abc", run_id="r1")
    assert path == lake.bronze / "season=2026" / "log_id=abc"
    on_disk = pq.read_table(path / "part-0.parquet").select(["signal", "ts_us"])
    rows = list(
        zip(on_disk.column("signal").to_pylist(), on_disk.column("ts_us").to_pylist(), strict=True)
    )
    assert rows == [("a", 2), ("a", 4), ("b", 1), ("b", 3)]
    assert connect(lake).sql("select count(*) from samples").fetchone() == (4,)


def test_bronze_rewrite_replaces_atomically(lake: LakePaths) -> None:
    bronze.write(lake, _table(), season="2026", log_id="abc", run_id="r1")
    bronze.write(lake, _table(2), season="2026", log_id="abc", run_id="r2")
    con = connect(lake)
    assert con.sql("select count(*) from samples").fetchone() == (2,)


def test_staging_leftovers_never_visible_and_are_cleaned(lake: LakePaths) -> None:
    stale = lake.staging / "dead-run" / "log_id=zzz"
    stale.mkdir(parents=True)
    (stale / "part-0.parquet").write_bytes(b"partial")
    bronze.write(lake, _table(), season="2026", log_id="abc", run_id="r1")
    con = connect(lake)
    assert con.sql("select distinct log_id from samples").fetchall() == [("abc",)]
    bronze.clean_staging(lake)
    assert not any(lake.staging.iterdir())


def test_query_reads_only_needed_partition(lake: LakePaths) -> None:
    bronze.write(lake, _table(), season="2026", log_id="abc", run_id="r1")
    bronze.write(lake, _table(), season="2025", log_id="def", run_id="r1")
    con = connect(lake)
    result = con.sql("select count(*) from samples where log_id = 'def'").fetchone()
    assert result == (4,)
    assert (lake.bronze / "season=2025" / "log_id=def").is_dir()
