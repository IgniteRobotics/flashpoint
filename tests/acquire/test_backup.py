"""Backup and restore through rclone (spec: lake-backup), against a fake `rclone` on PATH."""

import hashlib
import json
import logging
import sqlite3
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config as flashpoint_config
from flashpoint.acquire.backup import BackupError, BackupSchedule, run_backup
from flashpoint.acquire.config import AcquireConfig, BackupConfig
from flashpoint.acquire.cycle import CycleResult, run_cycle
from flashpoint.acquire.watch import StatusTracker, run_acquire
from flashpoint.cli import main
from flashpoint.lake.ledger import Ledger, Stage
from flashpoint.lake.paths import LakePaths
from tests.acquire.helpers import ledger_rows, synthetic_wpilog, tree

FAKE = Path(__file__).with_name("fake_rclone.py")
NO_ROBOT = "127.0.0.1:1"  # connection refused at once
T0 = datetime(2026, 10, 4, 10, 0, tzinfo=UTC)


class FakeRclone:
    def __init__(self, log: Path) -> None:
        self.log = log

    def calls(self) -> list[list[str]]:
        if not self.log.is_file():
            return []
        lines = self.log.read_text(encoding="utf-8").splitlines()
        return [json.loads(line)["argv"] for line in lines]


@pytest.fixture
def fake_rclone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeRclone:
    """`rclone` on PATH (a POSIX script or a Windows .cmd) that runs fake_rclone.py."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    if sys.platform == "win32":
        (bin_dir / "rclone.cmd").write_text(f'@"{sys.executable}" "{FAKE}" %*\r\n')
    else:
        script = bin_dir / "rclone"
        script.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{FAKE}" "$@"\n')
        script.chmod(0o755)
    monkeypatch.setenv("PATH", str(bin_dir))
    log = tmp_path / "rclone-calls.jsonl"
    monkeypatch.setenv("FAKE_RCLONE_LOG", str(log))
    return FakeRclone(log)


@pytest.fixture
def config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / "config"
    directory.mkdir()
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(directory))
    monkeypatch.setenv("FLASHPOINT_CACHE", str(tmp_path / "cache"))
    return directory


@pytest.fixture
def lake(tmp_path: Path) -> LakePaths:
    return LakePaths(tmp_path / "lake")


@pytest.fixture
def remote(tmp_path: Path) -> Path:
    return tmp_path / "remote"


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _seed_lake(lake: LakePaths, count: int) -> dict[str, str]:
    """`count` raw files registered as ingested at the current pipeline version; name -> hash."""
    lake.ensure()
    ledger = Ledger(lake.ledger)
    hashes: dict[str, str] = {}
    try:
        for i in range(count):
            data = f"log {i}".encode()
            sha = _sha(data)
            path = lake.raw / sha[:2] / f"{sha}.wpilog"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            ledger.register(sha, "wpilog", len(data), path)
            ledger.set_stage(sha, Stage.SUCCESS, flashpoint_config.PIPELINE_VERSION)
            hashes[path.relative_to(lake.raw).as_posix()] = sha
    finally:
        ledger.close()
    (lake.meta / "files.parquet").write_bytes(b"parquet snapshot")
    return hashes


def _hashes(root: Path) -> dict[str, str]:
    return {name: entry[2] for name, entry in tree(root).items()}


def _write_config(config_dir: Path, remote: Path | None) -> None:
    lines = ["[backup]"] + ([f"remote = {json.dumps(str(remote))}"] if remote else [])
    (config_dir / "acquire.toml").write_text("\n".join(lines) + "\n", encoding="utf-8")


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now


def _schedule(
    lake: LakePaths, remote: Path | None, clock: Callable[[], datetime] = lambda: T0
) -> tuple[BackupSchedule, StatusTracker]:
    tracker = StatusTracker(lake.status)
    settings = BackupConfig(remote=None if remote is None else str(remote))
    return BackupSchedule(settings, lake, tracker, threading.Event(), now=clock), tracker


def _changed() -> CycleResult:
    return CycleResult(started=T0.isoformat(), raw_changed=True, ledger_changed=True)


# --- backup: 7.1 -----------------------------------------------------------------------------


def test_first_backup_copies_raw_with_immutable_and_a_meta_snapshot(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path
) -> None:
    hashes = _seed_lake(lake, 15)
    torn = lake.raw / "ff" / f"{'f' * 64}.wpilog.part"  # raw.store interrupted mid-copy
    torn.parent.mkdir(parents=True, exist_ok=True)
    torn.write_bytes(b"half a log")

    run_backup(lake, str(remote))

    assert _hashes(remote / "raw") == hashes  # the .part is not uploaded
    calls = fake_rclone.calls()
    raw_copy = ["copy", "--immutable", "--exclude", "*.part", str(lake.raw), f"{remote}/raw"]
    assert raw_copy in calls
    for argv in calls:  # raw is only ever added to: no sync, delete or move touches it
        if any(a.endswith("raw") for a in argv):
            assert argv[0] == "copy" and "--immutable" in argv
    latest = remote / "meta" / "latest"
    assert (latest / "files.parquet").read_bytes() == b"parquet snapshot"
    assert ledger_rows(latest / "flashpoint.sqlite", "SELECT count(*) AS n FROM files") == [
        {"n": 15}
    ]
    assert ["sync", str(lake.tmp / "meta-snapshot"), f"{remote}/meta/latest"] in calls


def test_raw_file_missing_locally_stays_on_the_remote(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path
) -> None:
    hashes = _seed_lake(lake, 2)
    old = remote / "raw" / "ab" / "old.wpilog"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"only on the remote")

    run_backup(lake, str(remote))

    assert old.read_bytes() == b"only on the remote"
    assert _hashes(remote / "raw") == hashes | {"ab/old.wpilog": _sha(b"only on the remote")}


def test_meta_snapshot_opens_while_a_writer_holds_a_transaction(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path
) -> None:
    _seed_lake(lake, 1)
    writer = sqlite3.connect(lake.ledger, isolation_level=None)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "INSERT INTO files (sha256, kind, size, stage, first_seen, updated)"
            " VALUES ('uncommitted', 'wpilog', 1, 'received', 'x', 'x')"
        )

        run_backup(lake, str(remote))

        snapshot = remote / "meta" / "latest" / "flashpoint.sqlite"
        db = sqlite3.connect(snapshot)
        try:
            assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            assert db.execute("PRAGMA journal_mode").fetchone() == ("delete",)
            assert db.execute("SELECT count(*) FROM files").fetchone() == (1,)
        finally:
            db.close()
        assert sorted(p.name for p in snapshot.parent.iterdir()) == [
            "files.parquet",
            "flashpoint.sqlite",
        ]  # a self-contained file: no -wal or -shm
    finally:
        writer.execute("ROLLBACK")
        writer.close()


def test_backup_command_without_a_remote_exits_0_and_says_so(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _seed_lake(lake, 1)

    assert main(["backup", "--lake", str(lake.root)]) == 0

    assert "backup not configured" in capsys.readouterr().out
    assert fake_rclone.calls() == []


def test_watch_without_a_remote_skips_backup_and_logs_it_once(
    fake_rclone: FakeRclone, lake: LakePaths, caplog: pytest.LogCaptureFixture
) -> None:
    schedule, tracker = _schedule(lake, None)
    caplog.set_level(logging.INFO)

    schedule(_changed())
    schedule(_changed())

    assert fake_rclone.calls() == []
    assert [r.message for r in caplog.records].count("backup not configured") == 1
    assert tracker.backup["last_time"] is None


def test_backup_command_runs_now_ignoring_the_interval(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    hashes = _seed_lake(lake, 3)
    _write_config(config_dir, remote)
    schedule, tracker = _schedule(lake, remote, lambda: datetime.now(UTC))
    schedule(_changed())  # a backup just ran
    tracker.write()
    (remote / "raw").mkdir(exist_ok=True)
    for path in (remote / "raw").rglob("*"):
        if path.is_file():
            path.unlink()

    assert main(["backup", "--lake", str(lake.root)]) == 0

    assert _hashes(remote / "raw") == hashes
    assert "backed up" in capsys.readouterr().out


def test_backup_command_reports_an_rclone_failure(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _seed_lake(lake, 1)
    _write_config(config_dir, remote)
    monkeypatch.setenv("FAKE_RCLONE_FAIL", "1")

    assert main(["backup", "--lake", str(lake.root)]) == 1

    assert "fake failure" in capsys.readouterr().err


def test_nothing_changed_means_no_backup(fake_rclone: FakeRclone, lake: LakePaths) -> None:
    _seed_lake(lake, 1)
    schedule, tracker = _schedule(lake, lake.root.parent / "remote")

    schedule(CycleResult(started=T0.isoformat()))

    assert fake_rclone.calls() == []
    assert tracker.backup["last_time"] is None


def test_interval_is_respected_and_survives_a_restart(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path
) -> None:
    _seed_lake(lake, 1)
    clock = Clock()
    schedule, tracker = _schedule(lake, remote, clock)

    schedule(_changed())
    first = len(fake_rclone.calls())
    assert first > 0
    assert tracker.backup["last_time"] == T0.isoformat()
    assert tracker.backup["result"].startswith("ok")

    clock.now = T0 + timedelta(minutes=5)
    schedule(_changed())  # within 15 min: deferred, still owed
    assert len(fake_rclone.calls()) == first
    assert tracker.backup["pending"] is True

    tracker.record(CycleResult(started=clock.now.isoformat()))
    tracker.write()
    schedule, tracker = _schedule(lake, remote, clock)  # the watch restarts
    clock.now = T0 + timedelta(minutes=14)
    schedule(CycleResult(started=clock.now.isoformat()))
    assert len(fake_rclone.calls()) == first

    clock.now = T0 + timedelta(minutes=15)
    schedule(CycleResult(started=clock.now.isoformat()))  # nothing new, but a change is owed
    assert len(fake_rclone.calls()) == 2 * first
    assert tracker.backup["last_time"] == clock.now.isoformat()
    assert tracker.backup["pending"] is False


def test_rclone_failure_is_recorded_and_retried_after_the_interval(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_lake(lake, 1)
    monkeypatch.setenv("FAKE_RCLONE_FAIL", "1")
    clock = Clock()
    schedule, tracker = _schedule(lake, remote, clock)

    schedule(_changed())  # never raises

    assert tracker.backup["result"].startswith("error")
    assert "fake failure" in tracker.backup["result"]
    assert tracker.backup["pending"] is True
    monkeypatch.delenv("FAKE_RCLONE_FAIL")
    clock.now = T0 + timedelta(minutes=15)
    schedule(CycleResult(started=clock.now.isoformat()))
    assert tracker.backup["result"].startswith("ok")


def test_backup_without_a_ledger_refuses_rather_than_emptying_the_remote(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path
) -> None:
    with pytest.raises(BackupError, match="no ledger"):
        run_backup(lake, str(remote))
    assert fake_rclone.calls() == []


def test_missing_rclone_is_a_status_error_and_the_cycle_still_ingests(
    config_dir: Path, lake: LakePaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty = tmp_path / "empty-path"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    drop = lake.inbox / "manual" / "FRC_20260404_120000.wpilog"
    drop.parent.mkdir(parents=True)
    drop.write_bytes(synthetic_wpilog())
    config = AcquireConfig(
        hosts=[NO_ROBOT], removable_media=False, settle_s=0,
        backup=BackupConfig(remote=str(tmp_path / "remote")),
    )  # fmt: skip
    stop = threading.Event()
    results: list[CycleResult] = []

    def one_cycle(*args: Any, **kwargs: Any) -> CycleResult:
        results.append(run_cycle(*args, **kwargs))
        stop.set()
        return results[-1]

    assert run_acquire(config, lake, watch=True, stop=stop, cycle=one_cycle) == 0

    assert results[0].raw_changed and not results[0].errors
    assert not drop.exists()  # ingested, then cleared from the inbox
    stages = ledger_rows(lake.ledger, "SELECT stage FROM files")
    assert stages == [{"stage": "success"}]
    status = json.loads(lake.status.read_text())
    assert status["backup"]["result"].startswith("error")
    assert "rclone not found" in status["backup"]["result"]
    assert status["backup"]["pending"] is True


# --- restore: 7.3 ----------------------------------------------------------------------------


def test_restore_refuses_a_lake_with_a_ledger_and_leaves_it_unchanged(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _seed_lake(lake, 2)
    _write_config(config_dir, remote)
    before = tree(lake.root)

    assert main(["restore", "--lake", str(lake.root)]) != 0

    assert "--force" in capsys.readouterr().err
    assert tree(lake.root) == before
    assert fake_rclone.calls() == []


def test_restore_copies_raw_and_meta_and_resets_pipeline_version(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    hashes = _seed_lake(lake, 4)
    run_backup(lake, str(remote))
    _write_config(config_dir, remote)
    restored = LakePaths(tmp_path / "restored")

    assert main(["restore", "--lake", str(restored.root)]) == 0

    out = capsys.readouterr().out
    assert "run: flashpoint rebuild" in out
    assert _hashes(restored.raw) == hashes
    rows = ledger_rows(restored.ledger, "SELECT sha256, pipeline_version FROM files")
    assert sorted(r["sha256"] for r in rows) == sorted(hashes.values())  # type: ignore[type-var]
    assert {r["pipeline_version"] for r in rows} == {None}
    ledger = Ledger(restored.ledger)
    try:
        assert all(
            ledger.needs_processing(sha, flashpoint_config.PIPELINE_VERSION)
            for sha in hashes.values()
        )
    finally:
        ledger.close()
    assert (restored.meta / "files.parquet").read_bytes() == b"parquet snapshot"
    assert not (restored.tmp / "restore-meta").exists()


def test_restore_force_replaces_the_ledger_and_its_stale_wal(
    fake_rclone: FakeRclone, config_dir: Path, lake: LakePaths, remote: Path, tmp_path: Path
) -> None:
    hashes = _seed_lake(lake, 2)
    run_backup(lake, str(remote))
    other = LakePaths(tmp_path / "other")
    _seed_lake(other, 3)
    # A WAL left behind by a crashed writer: its pages would be replayed onto the restored file.
    writer = sqlite3.connect(other.ledger, isolation_level=None)
    writer.execute("PRAGMA wal_autocheckpoint=0")
    writer.execute("UPDATE files SET size = 99")
    wal = other.ledger.with_name(other.ledger.name + "-wal")
    stale = wal.read_bytes()
    writer.close()
    wal.write_bytes(stale)
    _write_config(config_dir, None)

    code = main(["restore", "--force", "--remote", str(remote), "--lake", str(other.root)])

    assert code == 0
    rows = ledger_rows(other.ledger, "SELECT sha256, size FROM files")
    assert sorted(r["sha256"] for r in rows) == sorted(hashes.values())  # type: ignore[type-var]
    assert {r["size"] for r in rows} == {len(b"log 0")}


def test_restore_without_a_remote_is_a_usage_error(
    fake_rclone: FakeRclone,
    config_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["restore", "--lake", str(tmp_path / "new")]) == 2
    assert "backup.remote" in capsys.readouterr().err
    assert fake_rclone.calls() == []


# --- review round 1 ----------------------------------------------------------------------------


def test_naive_last_time_in_the_status_is_treated_as_due(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path
) -> None:
    _seed_lake(lake, 1)
    schedule, tracker = _schedule(lake, remote)
    tracker.backup = {"last_time": "2026-10-04T10:00:00", "result": "ok", "pending": True}

    schedule(CycleResult(started=T0.isoformat()))  # must not raise TypeError every cycle

    assert fake_rclone.calls()
    assert tracker.backup["result"].startswith("ok")


def test_backup_command_with_a_mistyped_lake_creates_nothing(
    fake_rclone: FakeRclone,
    config_dir: Path,
    remote: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_config(config_dir, remote)
    typo = tmp_path / "lakee"

    assert main(["backup", "--lake", str(typo)]) == 1

    assert "no ledger at" in capsys.readouterr().err
    assert not typo.exists()
    assert fake_rclone.calls() == []


def test_backup_command_records_its_outcome_in_the_status(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_lake(lake, 1)
    _write_config(config_dir, remote)
    tracker = StatusTracker(lake.status)
    tracker.backup = {"last_time": None, "result": None, "pending": True}
    tracker.record(CycleResult(started=T0.isoformat(), errors=["inbox: boom"]))
    tracker.write()

    monkeypatch.setenv("FAKE_RCLONE_FAIL", "1")
    assert main(["backup", "--lake", str(lake.root)]) == 1
    failed = json.loads(lake.status.read_text())["backup"]
    assert failed["result"].startswith("error") and failed["pending"] is True

    monkeypatch.delenv("FAKE_RCLONE_FAIL")
    assert main(["backup", "--lake", str(lake.root)]) == 0

    status = json.loads(lake.status.read_text())
    assert status["errors"] == ["inbox: boom"]  # the rest of the status is kept
    assert status["backup"]["result"].startswith("ok") and status["backup"]["pending"] is False
    last = datetime.fromisoformat(status["backup"]["last_time"])
    assert abs(datetime.now(UTC) - last) < timedelta(minutes=1)
    assert StatusTracker(lake.status).backup == status["backup"]  # the watch interval sees it


def test_backup_command_reports_a_corrupt_ledger_without_a_traceback(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    lake.meta.mkdir(parents=True)
    lake.ledger.write_bytes(b"not a database" * 100)
    _write_config(config_dir, remote)

    assert main(["backup", "--lake", str(lake.root)]) == 1

    assert "backup failed:" in capsys.readouterr().err


def test_restore_reports_a_corrupt_snapshot_without_a_traceback(
    fake_rclone: FakeRclone,
    config_dir: Path,
    remote: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    latest = remote / "meta" / "latest"
    latest.mkdir(parents=True)
    (latest / "flashpoint.sqlite").write_bytes(b"not a database" * 100)
    (remote / "raw").mkdir()

    code = main(["restore", "--remote", str(remote), "--lake", str(tmp_path / "new")])

    assert code == 1
    assert "restore failed:" in capsys.readouterr().err


def test_restore_does_not_bring_over_the_source_watch_status(
    fake_rclone: FakeRclone, config_dir: Path, lake: LakePaths, remote: Path, tmp_path: Path
) -> None:
    _seed_lake(lake, 1)
    tracker = StatusTracker(lake.status)
    tracker.backup = {"last_time": T0.isoformat(), "result": "ok", "pending": True}
    tracker.record(CycleResult(started=T0.isoformat(), derive_pending=True))
    tracker.write()
    run_backup(lake, str(remote))
    assert (remote / "meta" / "latest" / lake.status.name).is_file()  # still backed up
    restored = LakePaths(tmp_path / "restored")

    assert main(["restore", "--remote", str(remote), "--lake", str(restored.root)]) == 0

    assert not restored.status.exists()
    assert not (restored.tmp / "restore-meta").exists()
