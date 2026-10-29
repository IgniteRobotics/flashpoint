"""When a backup is overdue (spec: backup-status), from the ledger, the configuration and the
status file's backup slot. No rclone: the rule only reads recorded state."""

import json
import os
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from flashpoint.acquire.backup import backup_status, config_hash
from flashpoint.acquire.config import BackupConfig
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths

NOW = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)
CONFIGURED = BackupConfig(remote="gdrive:flashpoint")


@pytest.fixture
def lake(tmp_path: Path) -> LakePaths:
    lake = LakePaths(tmp_path / "lake")
    lake.ensure()
    Ledger(lake.ledger).close()
    return lake


@pytest.fixture
def root(tmp_path: Path) -> Path:
    root = tmp_path / "config"
    (root / "robots").mkdir(parents=True)
    _write(root / "acquire.toml", "[backup]\n", NOW - timedelta(days=30))
    _write(root / "robots" / "comp.toml", 'robot = "comp"\n', NOW - timedelta(days=30))
    return root


def _write(path: Path, text: str, mtime: datetime) -> None:
    path.write_text(text, encoding="utf-8")
    os.utime(path, (mtime.timestamp(), mtime.timestamp()))


def _logs(lake: LakePaths, *ages: timedelta) -> None:
    ledger = Ledger(lake.ledger)
    try:
        for i, age in enumerate(ages):
            sha = f"{i:064x}"
            ledger.register(sha, "wpilog", 1, lake.raw / sha)
            seen = (NOW - age).isoformat(timespec="seconds")
            ledger.execute("UPDATE files SET first_seen = ? WHERE sha256 = ?", (seen, sha))
    finally:
        ledger.close()


def _backed_up(
    lake: LakePaths,
    age: timedelta | None,
    root: Path | None = None,
    error: str | None = None,
    micro: bool = True,
) -> None:
    """A status slot whose last success was `age` ago (None: never), optionally a later failure."""
    ok = (
        None
        if age is None
        else (NOW - age).isoformat(timespec="microseconds" if micro else "seconds")
    )
    slot = {
        "last_time": ok, "result": "ok: backed up", "pending": False, "last_ok": ok,
        "config_sha": config_hash(root) if root is not None else None,
    }  # fmt: skip
    if error is not None:
        slot |= {"last_time": (NOW - timedelta(hours=1)).isoformat(), "result": f"error: {error}"}
    lake.status.write_text(json.dumps({"version": 1, "backup": slot}), encoding="utf-8")


def test_imported_by_hand_with_no_backup_is_overdue(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(hours=30), timedelta(hours=29))

    status = backup_status(lake, root, CONFIGURED, NOW)

    assert status.overdue and status.level == "problem"
    assert status.logs_waiting == 2
    assert status.waiting_since == NOW - timedelta(hours=30)
    assert "2 logs" in status.message and "never" in status.message


def test_recent_logs_only_are_not_overdue(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(days=5), timedelta(hours=2))
    _backed_up(lake, timedelta(days=3), root)

    status = backup_status(lake, root, CONFIGURED, NOW)

    assert not status.overdue and status.level == "ok"
    assert status.logs_waiting == 1
    assert "3 days ago" in status.message


def test_configuration_edited_after_the_last_backup_is_overdue(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(days=5))
    _backed_up(lake, timedelta(days=3), root)
    _write(root / "robots" / "comp.toml", 'robot = "edited"\n', NOW - timedelta(days=2))

    status = backup_status(lake, root, CONFIGURED, NOW)

    assert status.overdue and status.config_waiting and status.logs_waiting == 0
    assert status.waiting_since == NOW - timedelta(days=2)
    assert "configuration" in status.message


def test_deleted_configuration_file_waits_from_the_last_backup(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(days=5))
    _backed_up(lake, timedelta(days=3), root)
    (root / "robots" / "comp.toml").unlink()

    status = backup_status(lake, root, CONFIGURED, NOW)

    assert status.config_waiting and status.waiting_since == NOW - timedelta(days=3)


def test_nothing_changed_for_a_week_is_ok(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(days=8))
    _backed_up(lake, timedelta(days=7), root)

    status = backup_status(lake, root, CONFIGURED, NOW)

    assert (status.level, status.overdue, status.logs_waiting) == ("ok", False, 0)
    assert not status.config_waiting


def test_microsecond_and_second_timestamps_compare_as_times(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(days=2))  # seen at exactly NOW - 2 days, in seconds
    _backed_up(lake, timedelta(days=2) - timedelta(microseconds=500), root)  # just after it

    assert backup_status(lake, root, CONFIGURED, NOW).logs_waiting == 0


def test_clock_behind_the_records_gives_no_wait(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(hours=-5))  # received "in the future"

    status = backup_status(lake, root, CONFIGURED, NOW)

    assert status.logs_waiting == 1 and not status.overdue


def test_failing_but_not_overdue_is_a_warning(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(hours=3))
    _backed_up(lake, timedelta(hours=5), root, error="rclone copy exited 1: connection refused")

    status = backup_status(lake, root, CONFIGURED, NOW)

    assert status.level == "warning" and not status.overdue
    assert "connection refused" in status.message


def test_offline_for_two_days_names_count_wait_and_error(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(days=2), timedelta(days=1), timedelta(hours=1))
    _backed_up(lake, timedelta(days=3), root, error="rclone copy exited 1: dial tcp: no route")

    status = backup_status(lake, root, CONFIGURED, NOW)

    assert status.level == "problem"
    assert "3 logs" in status.message and "2 days" in status.message
    assert "no route" in status.message


def test_long_error_is_shortened_to_its_last_line(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(days=2))
    _backed_up(lake, timedelta(days=3), root, error="line one\n" * 50 + "the real cause")

    message = backup_status(lake, root, CONFIGURED, NOW).message

    assert "the real cause" in message and "line one" not in message


def test_not_configured_with_logs_is_a_warning(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(days=40))

    status = backup_status(lake, root, BackupConfig(), NOW)

    assert status.level == "warning" and not status.configured and not status.overdue
    assert "only on this machine" in status.message


def test_empty_lake_has_nothing_to_back_up(lake: LakePaths, root: Path, tmp_path: Path) -> None:
    for settings in (CONFIGURED, BackupConfig()):
        status = backup_status(lake, root, settings, NOW)
        assert (status.level, status.overdue) == ("ok", False)
        assert "nothing to back up" in status.message
    missing = LakePaths(tmp_path / "no-lake")
    assert backup_status(missing, root, CONFIGURED, NOW).level == "ok"


def test_overdue_limit_comes_from_the_settings(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(hours=30))
    _backed_up(lake, timedelta(days=3), root)

    assert backup_status(lake, root, BackupConfig(remote="r:", overdue_h=48), NOW).level == "ok"
    assert backup_status(lake, root, BackupConfig(remote="r:", overdue_h=24), NOW).overdue


def test_unreadable_ledger_is_a_warning_not_an_exception(lake: LakePaths, root: Path) -> None:
    lake.ledger.write_bytes(b"not a database" * 100)

    status = backup_status(lake, root, CONFIGURED, NOW)

    assert status.level == "warning" and "can't read" in status.message


def test_status_file_from_before_last_ok(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(days=5))
    old = {"last_time": (NOW - timedelta(days=4)).isoformat(), "result": "ok: x", "pending": False}
    lake.status.write_text(json.dumps({"version": 1, "backup": old}), encoding="utf-8")

    status = backup_status(lake, None, CONFIGURED, NOW)

    assert status.logs_waiting == 0 and status.last_ok == NOW - timedelta(days=4)


def test_fast_enough_for_every_navigation(lake: LakePaths, root: Path) -> None:
    _logs(lake, *[timedelta(minutes=i) for i in range(3000)])
    _backed_up(lake, timedelta(days=1), root)

    started = time.perf_counter()
    backup_status(lake, root, CONFIGURED, NOW)

    assert time.perf_counter() - started < 0.05


def test_never_writes_the_ledger(lake: LakePaths, root: Path) -> None:
    _logs(lake, timedelta(days=1))
    before = lake.ledger.read_bytes()
    db = sqlite3.connect(lake.ledger)
    db.execute("BEGIN EXCLUSIVE")  # a writer holds the lake
    try:
        status = backup_status(lake, root, CONFIGURED, NOW)
    finally:
        db.rollback()
        db.close()
    assert status.level in ("warning", "problem")
    assert lake.ledger.read_bytes() == before


def test_new_team_first_log_an_hour_ago_is_not_overdue(lake: LakePaths, root: Path) -> None:
    """Configuration written a month ago waits only from the first log: no backup could run
    before the lake held one."""
    _logs(lake, timedelta(hours=1))

    status = backup_status(lake, root, CONFIGURED, NOW)

    assert status.config_waiting and not status.overdue
    assert status.waiting_since == NOW - timedelta(hours=1)
