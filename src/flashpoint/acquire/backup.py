"""Backup and restore of the lake's irreplaceable parts through rclone.

Raw goes up with `rclone copy --immutable`, so the remote copy of raw is append-only: nothing
is ever deleted there or overwritten. Metadata goes up as a snapshot folder,
`<lake>/tmp/meta-snapshot/`, synced to `<remote>/meta/latest`:

- `flashpoint.sqlite` from SQLite's online backup API, a transactionally consistent image even
  while ingest or the watch is writing, switched to a self-contained rollback-journal file;
- the `meta/*.parquet` snapshots that ingest already exported, copied as files (each is
  replaced atomically, so a copy is never torn, though it can trail the sqlite image; restore
  is followed by a rebuild, which exports them again). Exporting them here would load pyarrow
  into the watch process, which must stay within its RSS budget;
- the acquire status file.

Bronze, silver and gold are not backed up: they rebuild from raw. Flashpoint never reads rclone
credentials; the remote is a name from the user's rclone configuration plus a path.
"""

import logging
import shutil
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from flashpoint.acquire.config import BackupConfig
from flashpoint.acquire.cycle import CycleResult, run_subprocess
from flashpoint.lake.ledger import BUSY_TIMEOUT_MS, Ledger
from flashpoint.lake.paths import LakePaths

if TYPE_CHECKING:
    from flashpoint.acquire.watch import StatusTracker

log = logging.getLogger(__name__)

RCLONE = "rclone"
SNAPSHOT_DIR = "meta-snapshot"
RESTORE_DIR = "restore-meta"
PART_PATTERN = "*.part"
ERROR_CHARS = 1000
_SQLITE_SIDECARS = ("-wal", "-shm", "-journal")


class BackupError(RuntimeError):
    """A backup or restore step failed (rclone missing or exiting non-zero, nothing to copy)."""


class RestoreRefusedError(BackupError):
    """The target lake already has a ledger and `force` was not given."""


def remote_path(remote: str, sub: str) -> str:
    """`gdrive:flashpoint` + `raw` -> `gdrive:flashpoint/raw`; a bare `gdrive:` gets no slash."""
    remote = remote.rstrip("/\\")
    return f"{remote}{sub}" if remote.endswith(":") else f"{remote}/{sub}"


def _rclone(args: list[str], stop: threading.Event) -> None:
    binary = shutil.which(RCLONE)
    if binary is None:
        raise BackupError("rclone not found on PATH")
    code, output = run_subprocess([binary, *args], stop)
    if stop.is_set():
        raise BackupError(f"rclone {args[0]} stopped")
    if code != 0:
        raise BackupError(f"rclone {args[0]} exited {code}: {output.strip()[-ERROR_CHARS:]}")


def snapshot_meta(lake: LakePaths) -> Path:
    """Write `<lake>/tmp/meta-snapshot/` (sqlite image, Parquet snapshots, status); its path."""
    if not lake.ledger.is_file():
        raise BackupError(f"no ledger at {lake.ledger}; nothing to back up")
    target = lake.tmp / SNAPSHOT_DIR
    shutil.rmtree(target, ignore_errors=True)
    target.mkdir(parents=True)
    image_path = target / lake.ledger.name
    source = sqlite3.connect(lake.ledger, timeout=BUSY_TIMEOUT_MS / 1000)
    try:
        image = sqlite3.connect(image_path)
        try:
            source.backup(image)  # one step: a consistent read of the whole database
            image.execute("PRAGMA journal_mode=DELETE")  # no -wal file to carry along
        finally:
            image.close()
    finally:
        source.close()
    for suffix in _SQLITE_SIDECARS:  # left by the image's brief WAL phase; it is complete now
        image_path.with_name(image_path.name + suffix).unlink(missing_ok=True)
    for path in sorted(lake.meta.glob("*.parquet")):
        shutil.copyfile(path, target / path.name)
    if lake.status.is_file():
        shutil.copyfile(lake.status, target / lake.status.name)
    return target


def run_backup(lake: LakePaths, remote: str, *, stop: threading.Event | None = None) -> str:
    """Copy raw (append-only) and a metadata snapshot to `remote`; a one-line summary."""
    stop = stop or threading.Event()
    snapshot = snapshot_meta(lake)
    if lake.raw.is_dir():
        # raw.store writes `<sha>.<ext>.part` first; a torn one must never become immutable.
        raw_args = ["copy", "--immutable", "--exclude", PART_PATTERN]
        _rclone([*raw_args, str(lake.raw), remote_path(remote, "raw")], stop)
    _rclone(["sync", str(snapshot), remote_path(remote, "meta/latest")], stop)
    return f"backed up raw and metadata to {remote}"


def restore(
    lake: LakePaths, remote: str, *, force: bool = False, stop: threading.Event | None = None
) -> int:
    """Copy raw and the latest metadata snapshot from `remote` into `lake`; files in the ledger.

    Every file's pipeline version is cleared, so `flashpoint rebuild` reprocesses all of raw
    into bronze and derives silver and gold again.
    """
    if lake.ledger.exists() and not force:
        raise RestoreRefusedError(
            f"{lake.ledger} already exists; restore into a new lake, or pass --force"
        )
    stop = stop or threading.Event()
    staging = lake.tmp / RESTORE_DIR
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    _rclone(["copy", remote_path(remote, "meta/latest"), str(staging)], stop)
    if not (staging / lake.ledger.name).is_file():
        raise BackupError(f"no metadata snapshot at {remote_path(remote, 'meta/latest')}")
    lake.raw.mkdir(parents=True, exist_ok=True)
    _rclone(["copy", remote_path(remote, "raw"), str(lake.raw)], stop)
    lake.meta.mkdir(parents=True, exist_ok=True)
    for suffix in _SQLITE_SIDECARS:  # a stale WAL would be replayed onto the restored file
        lake.ledger.with_name(lake.ledger.name + suffix).unlink(missing_ok=True)
    for path in sorted(staging.iterdir()):
        if path.name == lake.status.name:  # another machine's watch state; this lake starts fresh
            path.unlink()
            continue
        path.replace(lake.meta / path.name)
    staging.rmdir()
    ledger = Ledger(lake.ledger)
    try:
        ledger.execute("UPDATE files SET pipeline_version = NULL")
        return len(ledger.files())
    finally:
        ledger.close()


class BackupSchedule:
    """The watch's backup step (`run_cycle(backup=...)`).

    A cycle that changed raw or the ledger makes a backup owed; an owed backup runs once
    `interval_min` has passed since the last attempt. The outcome and whether one is still owed
    live in the status file's backup slot, so both survive a restart. A failure is recorded
    there and retried after the interval; it never stops the cycle.
    """

    def __init__(
        self,
        settings: BackupConfig,
        lake: LakePaths,
        tracker: "StatusTracker",
        stop: threading.Event,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.settings = settings
        self.lake = lake
        self.tracker = tracker
        self.stop = stop
        self.now = now
        self._said_unconfigured = False

    def __call__(self, result: CycleResult) -> None:
        remote = self.settings.remote
        if not remote:
            if not self._said_unconfigured:
                log.info("backup not configured")
                self._said_unconfigured = True
            return
        state: dict[str, Any] = dict(self.tracker.backup)
        pending = bool(state.get("pending")) or result.raw_changed or result.ledger_changed
        state["pending"] = pending
        self.tracker.backup = state
        now = self.now()
        if not pending or not self._due(state.get("last_time"), now):
            return
        try:
            summary = run_backup(self.lake, remote, stop=self.stop)
        except (BackupError, OSError, sqlite3.Error) as exc:
            log.warning("backup failed: %s", exc)
            outcome, pending = f"error: {exc}", True
        else:
            log.info("%s", summary)
            outcome, pending = f"ok: {summary}", False
        self.tracker.backup = {"last_time": now.isoformat(), "result": outcome, "pending": pending}

    def _due(self, last_time: Any, now: datetime) -> bool:
        try:
            last = datetime.fromisoformat(str(last_time))
        except ValueError:
            return True  # never, or unreadable
        try:
            elapsed = now - last
        except TypeError:
            return True  # a naive timestamp (hand-edited status): treat as due
        return elapsed < timedelta(0) or elapsed >= timedelta(minutes=self.settings.interval_min)
