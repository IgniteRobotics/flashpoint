"""Backup and restore through rclone (spec: lake-backup), against a fake `rclone` on PATH."""

import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from flashpoint import cli
from flashpoint import config as flashpoint_config
from flashpoint.acquire.backup import (
    RCLONE_LIMITS,
    BackupError,
    BackupSchedule,
    config_files,
    config_hash,
    restore,
    run_backup,
    snapshot_config,
)
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
    lake: LakePaths,
    remote: Path | None,
    clock: Callable[[], datetime] = lambda: T0,
    config_root: Path | None = None,
) -> tuple[BackupSchedule, StatusTracker]:
    tracker = StatusTracker(lake.status)
    settings = BackupConfig(remote=None if remote is None else str(remote))
    schedule = BackupSchedule(
        settings, lake, tracker, threading.Event(), now=clock, config_root=config_root
    )
    return schedule, tracker


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
    raw_copy = [
        "copy", *RCLONE_LIMITS, "--immutable", "--exclude", "*.part", str(lake.raw),
        f"{remote}/raw",
    ]  # fmt: skip
    assert raw_copy in calls
    for argv in calls:  # raw is only ever added to: no sync, delete or move touches it
        if any(a.endswith("raw") for a in argv):
            assert argv[0] == "copy" and "--immutable" in argv
    latest = remote / "meta" / "latest"
    assert (latest / "files.parquet").read_bytes() == b"parquet snapshot"
    assert ledger_rows(latest / "flashpoint.sqlite", "SELECT count(*) AS n FROM files") == [
        {"n": 15}
    ]
    (sync,) = [argv for argv in calls if argv[0] == "sync"]
    assert sync[-2:] == [str(lake.tmp / "meta-snapshot"), f"{remote}/meta/latest"]


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

    status = json.loads(restored.status.read_text())  # a fresh status: only the backup record
    assert set(status) == {"version", "backup"}
    assert status["backup"]["result"].startswith("ok: restored from")
    assert not (restored.tmp / "restore-meta").exists()


# --- bounds and metadata history ---------------------------------------------------------------


def _value(argv: list[str], flag: str) -> str | None:
    return argv[argv.index(flag) + 1] if flag in argv else None


def test_every_rclone_call_is_bounded_and_only_the_watch_has_a_max_duration(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path
) -> None:
    _seed_lake(lake, 2)
    run_backup(lake, str(remote))  # `flashpoint backup`: a first upload may take long
    manual = fake_rclone.calls()
    schedule, _tracker = _schedule(lake, remote)
    schedule(_changed())
    watch = fake_rclone.calls()[len(manual) :]
    restore(LakePaths(lake.root.parent / "restored"), str(remote))
    restored = fake_rclone.calls()[len(manual) + len(watch) :]

    bounds = {
        "--contimeout": "10s", "--timeout": "60s", "--retries": "1", "--low-level-retries": "2",
    }  # fmt: skip
    for argv in manual + watch + restored:
        assert {flag: _value(argv, flag) for flag in bounds} == bounds
    assert [_value(argv, "--max-duration") for argv in watch] == ["4m", "4m"]
    assert [_value(argv, "--max-duration") for argv in manual + restored] == [None] * 4


def test_meta_sync_keeps_replaced_metadata_in_a_timestamped_history(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path
) -> None:
    _seed_lake(lake, 2)
    first = remote / "meta" / "latest" / "flashpoint.sqlite"
    run_backup(lake, str(remote), now=lambda: T0)
    original = first.read_bytes()

    lake.ledger.unlink()  # a wrong, empty lake backed up over the good one
    Ledger(lake.ledger).close()
    (lake.meta / "files.parquet").unlink()
    run_backup(lake, str(remote), now=lambda: T0 + timedelta(minutes=15))

    (sync, _) = [argv for argv in fake_rclone.calls() if argv[0] == "sync"]
    assert _value(sync, "--backup-dir") == f"{remote}/meta/history/20261004T100000Z"
    assert re.fullmatch(r"\d{8}T\d{6}Z", Path(str(_value(sync, "--backup-dir"))).name)
    kept = remote / "meta" / "history" / "20261004T101500Z"
    assert (kept / "flashpoint.sqlite").read_bytes() == original
    assert (kept / "files.parquet").read_bytes() == b"parquet snapshot"
    assert ledger_rows(kept / "flashpoint.sqlite", "SELECT count(*) AS n FROM files") == [{"n": 2}]


def test_watch_backup_cut_short_still_backs_up_meta_and_stays_owed(
    fake_rclone: FakeRclone,
    lake: LakePaths,
    remote: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_lake(lake, 2)
    monkeypatch.setenv("FAKE_RCLONE_DURATION_EXCEEDED", "1")
    schedule, tracker = _schedule(lake, remote)
    schedule(_changed())

    assert (remote / "meta" / "latest" / "flashpoint.sqlite").is_file()
    assert tracker.backup["pending"] is True
    assert "next backup" in tracker.backup["result"]


def test_backup_command_succeeds_even_when_the_status_cannot_be_updated(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    hashes = _seed_lake(lake, 1)
    _write_config(config_dir, remote)

    def unwritable(_path: Path, _backup: dict[str, Any]) -> None:
        raise PermissionError(13, "Permission denied", "acquire-status.json")

    monkeypatch.setattr(cli, "record_backup", unwritable)

    assert main(["backup", "--lake", str(lake.root)]) == 0

    out, err = capsys.readouterr()
    assert "backed up" in out and "backup failed" not in out + err
    assert "status" in err and "Permission denied" in err
    assert _hashes(remote / "raw") == hashes

    monkeypatch.setenv("FAKE_RCLONE_FAIL", "1")  # a failed backup still names the rclone error
    assert main(["backup", "--lake", str(lake.root)]) == 1
    err = capsys.readouterr().err
    assert "backup failed" in err and "fake failure" in err


# --- configuration (spec: config-backup) -------------------------------------------------------

CONFIG_FILES = {
    "acquire.toml": "[backup]\n",
    "robots/2026-comp.toml": 'robot = "comp"\n',
    "seasons/2026.toml": "season = 2026\n",
    "units/units-seed.csv": "serial,model\n",
}
NOT_CONFIG = {
    "env": "SECRET=1\n",
    ".backups/robots/2026-comp.toml.1": "old\n",
    "robots/.2026-comp.toml.abc.tmp": "half\n",
    "notes.txt": "hi\n",
    "robots/old/2025-comp.toml": "old robot\n",
    "units/notes.toml": "not a unit file\n",
}


def _write_files(root: Path, files: dict[str, str]) -> None:
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def _texts(root: Path) -> dict[str, str]:
    return {
        p.relative_to(root).as_posix(): p.read_text(encoding="utf-8")
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


@pytest.fixture
def team_config(tmp_path: Path) -> Path:
    root = tmp_path / "team-config"
    _write_files(root, CONFIG_FILES | NOT_CONFIG)
    return root


def test_config_snapshot_copies_only_configuration_files(team_config: Path, tmp_path: Path) -> None:
    target = tmp_path / "snap"

    copied = snapshot_config(team_config, target)

    assert copied == sorted(CONFIG_FILES)
    assert _texts(target) == CONFIG_FILES
    assert config_files(team_config) == sorted(CONFIG_FILES)


def test_config_snapshot_of_a_missing_or_empty_root_is_empty(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    _write_files(tmp_path / "junk", NOT_CONFIG)
    for root in (tmp_path / "missing", tmp_path / "empty", tmp_path / "junk"):
        assert snapshot_config(root, tmp_path / "snap") == []
        assert not any((tmp_path / "snap").rglob("*"))


def test_config_hash_follows_contents_and_names_only(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    _write_files(a, CONFIG_FILES)
    _write_files(b, dict(reversed(CONFIG_FILES.items())) | NOT_CONFIG)  # other order, extra junk
    assert config_hash(a) == config_hash(b)
    assert config_hash(tmp_path / "missing") == config_hash(tmp_path / "missing-too")
    assert config_hash(a) != config_hash(tmp_path / "missing")

    (b / "robots" / "2026-comp.toml").write_text('robot = "comP"\n', encoding="utf-8")
    assert config_hash(a) != config_hash(b)
    _write_files(b, {"robots/2026-comp.toml": 'robot = "comp"\n'})
    assert config_hash(a) == config_hash(b)
    (b / "robots" / "2026-comp.toml").rename(b / "robots" / "2026-practice.toml")
    assert config_hash(a) != config_hash(b)


def test_backup_syncs_configuration_with_history_after_raw_and_meta(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, team_config: Path
) -> None:
    _seed_lake(lake, 2)

    summary = run_backup(lake, str(remote), config_root=team_config, now=lambda: T0)

    assert _texts(remote / "config" / "latest") == CONFIG_FILES
    assert "configuration" in summary
    calls = fake_rclone.calls()
    assert [argv[-1] for argv in calls] == [
        f"{remote}/raw",
        f"{remote}/meta/latest",
        f"{remote}/config/latest",
    ]
    config_sync = calls[-1]
    assert config_sync[0] == "sync"
    assert config_sync[-2] == str(lake.tmp / "config-snapshot")
    assert _value(config_sync, "--backup-dir") == f"{remote}/config/history/20261004T100000Z"


def test_edited_and_deleted_config_files_are_kept_in_history(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, team_config: Path
) -> None:
    _seed_lake(lake, 1)
    run_backup(lake, str(remote), config_root=team_config, now=lambda: T0)
    (team_config / "robots" / "2026-comp.toml").write_text('robot = "edited"\n', encoding="utf-8")
    (team_config / "units" / "units-seed.csv").unlink()

    run_backup(lake, str(remote), config_root=team_config, now=lambda: T0 + timedelta(hours=1))

    latest = _texts(remote / "config" / "latest")
    assert latest["robots/2026-comp.toml"] == 'robot = "edited"\n'
    assert "units/units-seed.csv" not in latest
    kept = _texts(remote / "config" / "history" / "20261004T110000Z")
    assert kept == {
        "robots/2026-comp.toml": 'robot = "comp"\n',
        "units/units-seed.csv": "serial,model\n",
    }


def test_backup_without_configuration_files_still_backs_up_and_says_so(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, tmp_path: Path
) -> None:
    hashes = _seed_lake(lake, 1)

    summary = run_backup(lake, str(remote), config_root=tmp_path / "missing")

    assert _hashes(remote / "raw") == hashes
    assert "no configuration files found" in summary
    assert not (remote / "config").exists()
    assert all(not argv[-1].endswith("config/latest") for argv in fake_rclone.calls())


def test_config_sync_failure_fails_the_backup_after_metadata(
    fake_rclone: FakeRclone,
    lake: LakePaths,
    remote: Path,
    team_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_lake(lake, 1)
    monkeypatch.setenv("FAKE_RCLONE_FAIL_ON", "config/latest")

    with pytest.raises(BackupError, match="fake failure"):
        run_backup(lake, str(remote), config_root=team_config)

    assert (remote / "meta" / "latest" / "flashpoint.sqlite").is_file()


def test_watch_backup_cut_short_still_backs_up_configuration(
    fake_rclone: FakeRclone,
    lake: LakePaths,
    remote: Path,
    team_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_lake(lake, 1)
    monkeypatch.setenv("FAKE_RCLONE_DURATION_EXCEEDED", "1")
    schedule, tracker = _schedule(lake, remote, config_root=team_config)

    schedule(_changed())

    assert _texts(remote / "config" / "latest") == CONFIG_FILES
    assert tracker.backup["pending"] is True  # raw is still owed
    config_sync = fake_rclone.calls()[-1]
    assert _value(config_sync, "--max-duration") == "4m"


def test_backup_command_backs_up_the_config_root_and_records_its_hash(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _seed_lake(lake, 1)
    _write_files(config_dir, {k: v for k, v in CONFIG_FILES.items() if k != "acquire.toml"})
    _write_config(config_dir, remote)

    assert main(["backup", "--lake", str(lake.root)]) == 0

    assert "configuration" in capsys.readouterr().out
    assert _texts(remote / "config" / "latest") == _texts(config_dir)
    status = json.loads(lake.status.read_text())
    assert status["backup"]["config_sha"] == config_hash(config_dir)


def test_config_change_alone_makes_a_backup_due(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, team_config: Path
) -> None:
    _seed_lake(lake, 1)
    clock = Clock()
    schedule, tracker = _schedule(lake, remote, clock, config_root=team_config)
    schedule(_changed())
    assert tracker.backup["config_sha"] == config_hash(team_config)
    first = len(fake_rclone.calls())

    clock.now = T0 + timedelta(minutes=20)
    schedule(CycleResult(started=clock.now.isoformat()))  # quiet day, config unchanged
    assert len(fake_rclone.calls()) == first

    (team_config / "robots" / "2026-comp.toml").write_text('robot = "saved"\n', encoding="utf-8")
    clock.now = T0 + timedelta(minutes=21)
    schedule(CycleResult(started=clock.now.isoformat()))  # no new logs, a robot saved

    assert len(fake_rclone.calls()) == 2 * first
    latest = remote / "config" / "latest" / "robots" / "2026-comp.toml"
    assert latest.read_text(encoding="utf-8") == 'robot = "saved"\n'
    assert tracker.backup["config_sha"] == config_hash(team_config)
    assert tracker.backup["pending"] is False


def test_config_change_waits_for_the_interval(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, team_config: Path
) -> None:
    _seed_lake(lake, 1)
    clock = Clock()
    schedule, tracker = _schedule(lake, remote, clock, config_root=team_config)
    schedule(_changed())
    first = len(fake_rclone.calls())

    (team_config / "acquire.toml").write_text("[backup]\n# edited\n", encoding="utf-8")
    clock.now = T0 + timedelta(minutes=5)
    schedule(CycleResult(started=clock.now.isoformat()))

    assert len(fake_rclone.calls()) == first
    assert tracker.backup["pending"] is True


def test_config_hash_survives_a_watch_restart(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, team_config: Path
) -> None:
    _seed_lake(lake, 1)
    clock = Clock()
    schedule, tracker = _schedule(lake, remote, clock, config_root=team_config)
    schedule(_changed())
    tracker.record(CycleResult(started=T0.isoformat()))
    tracker.write()
    first = len(fake_rclone.calls())

    schedule, tracker = _schedule(lake, remote, clock, config_root=team_config)
    clock.now = T0 + timedelta(hours=1)
    schedule(CycleResult(started=clock.now.isoformat()))

    assert len(fake_rclone.calls()) == first  # same config, nothing new: no backup


def test_status_without_a_config_hash_owes_a_backup(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, team_config: Path
) -> None:
    _seed_lake(lake, 1)
    schedule, tracker = _schedule(lake, remote, config_root=team_config)
    tracker.backup = {"last_time": "2026-10-01T00:00:00+00:00", "result": "ok", "pending": False}

    schedule(CycleResult(started=T0.isoformat()))  # status from before configuration backup

    assert _texts(remote / "config" / "latest") == CONFIG_FILES
    assert tracker.backup["config_sha"] == config_hash(team_config)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions")
def test_unreadable_config_folder_is_a_backup_error_and_the_cycle_goes_on(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, team_config: Path
) -> None:
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("root reads everything")
    _seed_lake(lake, 1)
    robots = team_config / "robots"
    robots.chmod(0)
    try:
        schedule, tracker = _schedule(lake, remote, config_root=team_config)
        schedule(_changed())  # never raises
    finally:
        robots.chmod(0o755)

    assert tracker.backup["result"].startswith("error")
    assert tracker.backup["pending"] is True
    assert (remote / "meta" / "latest" / "flashpoint.sqlite").is_file()


def _backed_up(lake: LakePaths, remote: Path, team_config: Path) -> None:
    _seed_lake(lake, 2)
    run_backup(lake, str(remote), config_root=team_config)


def test_restore_copies_configuration_into_an_empty_root(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, team_config: Path, tmp_path: Path
) -> None:
    _backed_up(lake, remote, team_config)
    (remote / "config" / "latest" / "notes.txt").write_text("not config\n")  # left on the remote
    for root in (tmp_path / "new-root", tmp_path / "empty-root"):
        if root.name == "empty-root":
            root.mkdir()
        target = LakePaths(tmp_path / f"lake-{root.name}")

        result = restore(target, str(remote), config_root=root)

        assert _texts(root) == CONFIG_FILES
        assert result.config == "restored"
        assert result.config_path == root
        assert not (target.root / "restored-config").exists()
        assert not (target.tmp / "restore-config").exists()


def test_restore_keeps_existing_configuration_and_puts_the_copy_beside_the_lake(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    team_config: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _backed_up(lake, remote, team_config)
    _write_config(config_dir, remote)
    _write_files(config_dir, {"robots/mine.toml": "mine\n"})
    before = _texts(config_dir)
    restored = LakePaths(tmp_path / "restored")

    assert main(["restore", "--lake", str(restored.root)]) == 0

    assert _texts(config_dir) == before
    copy = restored.root / "restored-config"
    assert _texts(copy) == CONFIG_FILES
    assert str(copy) in capsys.readouterr().out


def test_forced_restore_still_keeps_configuration(
    fake_rclone: FakeRclone, config_dir: Path, lake: LakePaths, remote: Path, team_config: Path
) -> None:
    _backed_up(lake, remote, team_config)
    _write_config(config_dir, remote)
    _write_files(config_dir, {"robots/2026-comp.toml": "local edit\n"})
    before = _texts(config_dir)

    assert main(["restore", "--force", "--lake", str(lake.root)]) == 0

    assert _texts(config_dir) == before
    assert _texts(lake.root / "restored-config") == CONFIG_FILES


def test_restore_from_a_backup_without_configuration(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    hashes = _seed_lake(lake, 2)
    run_backup(lake, str(remote))  # made before configuration was backed up
    root = tmp_path / "scratch-config"
    restored = LakePaths(tmp_path / "restored")

    result = restore(restored, str(remote), config_root=root)

    assert result.config == "none" and result.files == 2
    assert _hashes(restored.raw) == hashes
    assert not root.exists()
    assert not (restored.root / "restored-config").exists()
    _write_config(config_dir, remote)
    other = LakePaths(tmp_path / "other")
    assert main(["restore", "--lake", str(other.root)]) == 0
    assert "no configuration" in capsys.readouterr().out


def test_second_restore_replaces_the_restored_config_copy(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path, team_config: Path, tmp_path: Path
) -> None:
    _backed_up(lake, remote, team_config)
    root = tmp_path / "root"
    _write_files(root, {"acquire.toml": "local\n"})
    target = LakePaths(tmp_path / "target")
    restore(target, str(remote), config_root=root)
    (target.root / "restored-config" / "stale.toml").write_text("stale\n")
    (team_config / "units" / "units-seed.csv").unlink()
    run_backup(lake, str(remote), config_root=team_config)

    result = restore(target, str(remote), force=True, config_root=root)

    assert result.config == "kept"
    expected = {k: v for k, v in CONFIG_FILES.items() if k != "units/units-seed.csv"}
    assert _texts(target.root / "restored-config") == expected
    assert _texts(root) == {"acquire.toml": "local\n"}


# --- last successful backup (spec: backup-status) ----------------------------------------------


def test_watch_success_records_last_ok_and_a_failure_keeps_it(
    fake_rclone: FakeRclone,
    lake: LakePaths,
    remote: Path,
    team_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_lake(lake, 1)
    clock = Clock()
    schedule, tracker = _schedule(lake, remote, clock, config_root=team_config)
    schedule(_changed())
    assert tracker.backup["last_ok"] == T0.isoformat()
    sha = tracker.backup["config_sha"]

    monkeypatch.setenv("FAKE_RCLONE_FAIL", "1")
    (team_config / "acquire.toml").write_text("[backup]\n# edited\n", encoding="utf-8")
    clock.now = T0 + timedelta(hours=1)
    schedule(_changed())

    assert tracker.backup["result"].startswith("error")
    assert tracker.backup["last_time"] == clock.now.isoformat()
    assert tracker.backup["last_ok"] == T0.isoformat()  # the last success is kept
    assert tracker.backup["config_sha"] == sha


def test_backup_command_failure_keeps_the_last_success(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_lake(lake, 1)
    _write_config(config_dir, remote)
    assert main(["backup", "--lake", str(lake.root)]) == 0
    first = json.loads(lake.status.read_text())["backup"]
    assert first["last_ok"] == first["last_time"]
    assert first["config_sha"] == config_hash(config_dir)

    monkeypatch.setenv("FAKE_RCLONE_FAIL", "1")
    assert main(["backup", "--lake", str(lake.root)]) == 1

    failed = json.loads(lake.status.read_text())["backup"]
    assert failed["result"].startswith("error")
    assert (failed["last_ok"], failed["config_sha"]) == (first["last_ok"], first["config_sha"])


def test_old_status_without_last_ok_reads_an_ok_attempt_as_the_last_success(
    lake: LakePaths,
) -> None:
    lake.meta.mkdir(parents=True)
    for result, expected in (("ok: backed up", "2026-10-01T00:00:00+00:00"), ("error: x", None)):
        status = {"version": 1, "backup": {
            "last_time": "2026-10-01T00:00:00+00:00", "result": result, "pending": False,
        }}  # fmt: skip
        lake.status.write_text(json.dumps(status), encoding="utf-8")
        assert StatusTracker(lake.status).backup["last_ok"] == expected


def test_last_ok_survives_a_watch_restart(
    fake_rclone: FakeRclone, lake: LakePaths, remote: Path
) -> None:
    _seed_lake(lake, 1)
    schedule, tracker = _schedule(lake, remote)
    schedule(_changed())
    tracker.record(CycleResult(started=T0.isoformat()))
    tracker.write()

    assert StatusTracker(lake.status).backup["last_ok"] == T0.isoformat()


def test_restore_records_the_restored_backup_as_the_last_success(
    fake_rclone: FakeRclone,
    config_dir: Path,
    lake: LakePaths,
    remote: Path,
    team_config: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_lake(lake, 2)
    tracker = StatusTracker(lake.status)
    tracker.record(CycleResult(started=T0.isoformat(), derive_pending=True))
    tracker.write()
    run_backup(lake, str(remote), config_root=team_config)
    for name, existing in (("empty", {}), ("kept", {"robots/mine.toml": "mine\n"})):
        root = tmp_path / f"config-{name}"
        root.mkdir()
        _write_files(root, existing)
        monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
        restored = LakePaths(tmp_path / f"restored-{name}")
        before = datetime.now(UTC)

        assert main(["restore", "--remote", str(remote), "--lake", str(restored.root)]) == 0

        status = json.loads(restored.status.read_text())
        backup = status["backup"]
        assert datetime.fromisoformat(backup["last_ok"]) >= before.replace(microsecond=0)
        assert backup["config_sha"] == config_hash(root)  # what is in the root now
        assert backup["pending"] is False
        assert "derive" not in status or not status["derive"].get("pending")
