from collections.abc import Iterator
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from flashpoint.acquire.pulls import MAX_ATTEMPTS, PullKey, PullLedger, PullStatus
from flashpoint.lake.ledger import INCOMPLETE_READ, Ledger, Stage
from flashpoint.lake.paths import LakePaths

SHA = "a" * 64


@pytest.fixture
def paths(tmp_path: Path) -> LakePaths:
    lake = LakePaths(tmp_path / "lake")
    lake.ensure()
    return lake


@pytest.fixture
def pulls(paths: LakePaths) -> Iterator[PullLedger]:
    ledger = PullLedger(paths.ledger)
    yield ledger
    ledger.close()


def _key(path: str = "/home/lvuser/logs/a.wpilog", size: int = 100, mtime_ns: int = 5) -> PullKey:
    return PullKey("robot", "10.68.29.2", path, size, mtime_ns)


def test_unknown_file_is_not_pulled_and_should_be(pulls: PullLedger) -> None:
    assert not pulls.is_pulled(_key())
    assert pulls.should_pull(_key())


@pytest.mark.parametrize("status", [PullStatus.VERIFIED, PullStatus.SIZE_VERIFIED])
def test_success_is_deduped(pulls: PullLedger, status: PullStatus) -> None:
    pulls.record_success(_key(), SHA, status, include_active=False)
    assert pulls.is_pulled(_key())
    assert not pulls.should_pull(_key())
    row = pulls.get(_key())
    assert row is not None
    assert (row["sha256"], row["status"], row["include_active"]) == (SHA, status, 0)
    assert row["pulled_at"] is not None


def test_changed_size_or_mtime_is_a_new_entry(pulls: PullLedger) -> None:
    pulls.record_success(_key(), SHA, PullStatus.VERIFIED, include_active=True)
    assert pulls.should_pull(_key(size=200))
    assert pulls.should_pull(_key(mtime_ns=6))
    pulls.record_success(_key(size=200), "b" * 64, PullStatus.VERIFIED, include_active=False)
    rows = pulls.query("SELECT * FROM pulls ORDER BY size")
    assert [r["include_active"] for r in rows] == [1, 0]  # partial entry kept


def test_upsert_keeps_one_row_per_key(pulls: PullLedger) -> None:
    pulls.record_failure(_key(), "hash mismatch")
    pulls.record_success(_key(), SHA, PullStatus.VERIFIED, include_active=False)
    rows = pulls.query("SELECT * FROM pulls")
    assert len(rows) == 1
    assert rows[0]["status"] == PullStatus.VERIFIED
    assert rows[0]["reason"] is None
    assert rows[0]["attempts"] == 1


def test_failures_count_attempts_then_fail_after_three(pulls: PullLedger) -> None:
    assert MAX_ATTEMPTS == 3
    assert pulls.record_failure(_key(), "hash mismatch") is PullStatus.PENDING
    assert pulls.should_pull(_key())
    assert pulls.record_failure(_key(), "hash mismatch") is PullStatus.PENDING
    assert pulls.record_failure(_key(), "size mismatch") is PullStatus.FAILED
    row = pulls.get(_key())
    assert row is not None
    assert (row["status"], row["attempts"], row["reason"]) == ("failed", 3, "size mismatch")
    assert not pulls.is_pulled(_key())
    assert not pulls.should_pull(_key())  # no further automatic retries


def test_failed_lists_only_failed_rows(pulls: PullLedger) -> None:
    for _ in range(3):
        pulls.record_failure(_key("/x/bad.hoot"), "timeout")
    pulls.record_failure(_key("/x/flaky.hoot"), "timeout")
    pulls.record_success(_key("/x/ok.hoot"), SHA, PullStatus.VERIFIED, include_active=False)
    failed = pulls.failed()
    assert [(r["source_id"], r["remote_path"], r["reason"]) for r in failed] == [
        ("10.68.29.2", "/x/bad.hoot", "timeout")
    ]


def test_same_path_on_different_source_is_independent(pulls: PullLedger) -> None:
    pulls.record_success(_key(), SHA, PullStatus.VERIFIED, include_active=False)
    other = PullKey("volume", "VOL-UUID", _key().remote_path, 100, 5)
    assert pulls.should_pull(other)


def test_parquet_snapshot_includes_pulls(pulls: PullLedger, paths: LakePaths) -> None:
    pulls.record_success(_key(), SHA, PullStatus.SIZE_VERIFIED, include_active=True)
    ledger = Ledger(paths.ledger)
    try:
        ledger.export_snapshots(paths.meta)
    finally:
        ledger.close()
    table = pq.read_table(paths.meta / "pulls.parquet")
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["status"] == "size-verified"
    assert row["size"] == 100
    assert row["mtime_ns"] == 5
    assert row["include_active"] == 1


def test_busy_timeout_is_30s_on_both_connections(pulls: PullLedger, paths: LakePaths) -> None:
    assert pulls.query("PRAGMA busy_timeout") == [{"timeout": 30000}]
    ledger = Ledger(paths.ledger)
    try:
        assert ledger.query("PRAGMA busy_timeout") == [{"timeout": 30000}]
    finally:
        ledger.close()


def test_pulls_and_ledger_share_a_database_without_blocking(
    pulls: PullLedger, paths: LakePaths
) -> None:
    ledger = Ledger(paths.ledger)
    try:
        ledger.register(SHA, "wpilog", 100, Path("/inbox/a.wpilog"))
        pulls.record_success(_key(), SHA, PullStatus.VERIFIED, include_active=False)
        assert ledger.stage(SHA) is Stage.RECEIVED
    finally:
        ledger.close()


def test_add_warning_appends_in_comma_format(paths: LakePaths) -> None:
    ledger = Ledger(paths.ledger)
    try:
        ledger.register(SHA, "hoot", 100, Path("/inbox/a.hoot"))
        ledger.set_stage(SHA, Stage.SUCCESS, 2, warnings="short-coverage")
        ledger.add_warning(SHA, "incomplete-read")
        ledger.add_warning(SHA, "incomplete-read")  # idempotent
        assert ledger.files()[0]["warnings"] == "short-coverage,incomplete-read"
    finally:
        ledger.close()


def test_add_warning_on_file_without_warnings(paths: LakePaths) -> None:
    ledger = Ledger(paths.ledger)
    try:
        ledger.register(SHA, "wpilog", 100, Path("/inbox/a.wpilog"))
        ledger.add_warning(SHA, "incomplete-read")
        assert ledger.files()[0]["warnings"] == "incomplete-read"
        ledger.add_warning("f" * 64, "incomplete-read")  # unknown sha: no-op
    finally:
        ledger.close()


def test_lake_paths_inbox_and_status(paths: LakePaths) -> None:
    assert paths.inbox == paths.root / "inbox"
    assert paths.status == paths.root / "meta" / "acquire-status.json"


@pytest.mark.parametrize("warnings", [None, "short-coverage", "incomplete-read"])
def test_set_stage_keeps_an_incomplete_read_marker(paths: LakePaths, warnings: str | None) -> None:
    ledger = Ledger(paths.ledger)
    try:
        ledger.register(SHA, "wpilog", 100, Path("/inbox/a.wpilog"))
        ledger.set_stage(SHA, Stage.SUCCESS, 2)
        ledger.add_warning(SHA, INCOMPLETE_READ)
        ledger.execute("UPDATE files SET pipeline_version = NULL")  # restore, then rebuild
        ledger.set_stage(SHA, Stage.BRONZE, 3)
        ledger.set_stage(SHA, Stage.SUCCESS, 3, warnings=warnings)
        kept = (ledger.files()[0]["warnings"] or "").split(",")
        assert kept.count(INCOMPLETE_READ) == 1
        assert warnings is None or warnings in kept
    finally:
        ledger.close()


def test_set_stage_without_a_marker_still_replaces_warnings(paths: LakePaths) -> None:
    ledger = Ledger(paths.ledger)
    try:
        ledger.register(SHA, "hoot", 100, Path("/inbox/a.hoot"))
        ledger.set_stage(SHA, Stage.SUCCESS, 2, warnings="short-coverage")
        ledger.set_stage(SHA, Stage.SUCCESS, 3)
        assert ledger.files()[0]["warnings"] is None
    finally:
        ledger.close()


def _via(host: str) -> PullKey:
    return PullKey("robot", host, "/home/lvuser/logs/a.wpilog", 100, 5)


@pytest.mark.parametrize("status", [PullStatus.VERIFIED, PullStatus.SIZE_VERIFIED])
def test_a_robot_file_pulled_via_one_host_is_pulled_via_every_host(
    pulls: PullLedger, status: PullStatus
) -> None:
    pulls.record_success(_via("10.68.29.2"), SHA, status, include_active=False)
    for other in ("roborio-6829-frc.local", "172.22.11.2"):
        assert pulls.is_pulled(_via(other))
        assert not pulls.should_pull(_via(other))
    # The serving host stays recorded.
    rows = pulls.query("SELECT source_id FROM pulls")
    assert rows == [{"source_id": "10.68.29.2"}]


def test_a_robot_file_failed_via_one_host_is_not_retried_via_another(pulls: PullLedger) -> None:
    for _ in range(MAX_ATTEMPTS):
        pulls.record_failure(_via("10.68.29.2"), "hash-mismatch")
    assert not pulls.should_pull(_via("172.22.11.2"))
    assert not pulls.is_pulled(_via("172.22.11.2"))


def test_volume_files_are_still_keyed_by_volume(pulls: PullLedger) -> None:
    a = PullKey("volume", "UUID-A", "logs/a.wpilog", 100, 5)
    b = PullKey("volume", "UUID-B", "logs/a.wpilog", 100, 5)
    pulls.record_success(a, SHA, PullStatus.VERIFIED, include_active=False)
    assert pulls.is_pulled(a)
    assert not pulls.is_pulled(b) and pulls.should_pull(b)
    # A robot file at the same path is not a volume file.
    assert pulls.should_pull(PullKey("robot", "UUID-A", "logs/a.wpilog", 100, 5))
