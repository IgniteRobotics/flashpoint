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

The sync moves every file it replaces or deletes in `meta/latest` into
`<remote>/meta/history/<UTC time>/`, so a wrong or empty lake can never destroy the only remote
copy of the metadata. Nothing prunes the history; delete old folders by hand.

Every rclone call is bounded (connect and I/O timeouts, one retry), and the watch's backup also
stops after `--max-duration 4m` so the cycle never waits long on a slow link. The raw copy is
incremental: what did not fit resumes at the next backup, and the backup stays owed.

The team's configuration files (spec: config-backup) go up the same way, as a snapshot synced to
`<remote>/config/latest` with history under `<remote>/config/history/<UTC time>/`. Only files
on an allow-list are copied (`CONFIG_PATTERNS`), never the service's `env` file or hidden files.
Restore copies them into an empty configuration folder, and otherwise beside the lake in
`restored-config/`: configuration that is already there is never overwritten.

Bronze, silver and gold are not backed up: they rebuild from raw. Flashpoint never reads rclone
credentials; the remote is a name from the user's rclone configuration plus a path.
"""

import hashlib
import logging
import shutil
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass
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
# Bounds for every call: a dead link fails in seconds instead of rclone's minutes of retries.
RCLONE_LIMITS = (
    "--contimeout", "10s", "--timeout", "60s", "--retries", "1", "--low-level-retries", "2",
)  # fmt: skip
WATCH_MAX_DURATION = "4m"  # the watch's backup runs inside the acquisition cycle
EXIT_DURATION_EXCEEDED = 10  # rclone's exit code when --max-duration stopped it
EXIT_DIRECTORY_NOT_FOUND = 3  # rclone's exit code for a missing source folder
HISTORY_DIR = "meta/history"
SNAPSHOT_DIR = "meta-snapshot"
RESTORE_DIR = "restore-meta"
CONFIG_LATEST = "config/latest"
CONFIG_HISTORY_DIR = "config/history"
CONFIG_SNAPSHOT_DIR = "config-snapshot"
CONFIG_RESTORE_DIR = "restore-config"
RESTORED_CONFIG_DIR = "restored-config"  # beside the lake's layers, when the root has files
# Configuration files: folder under the root -> file suffix. Nothing else is backed up.
CONFIG_FOLDERS = {"": ".toml", "seasons": ".toml", "robots": ".toml", "units": ".csv"}
PART_PATTERN = "*.part"
ERROR_CHARS = 1000
_SQLITE_SIDECARS = ("-wal", "-shm", "-journal")


class BackupError(RuntimeError):
    """A backup or restore step failed (rclone missing or exiting non-zero, nothing to copy)."""


class RestoreRefusedError(BackupError):
    """The target lake already has a ledger and `force` was not given."""


@dataclass(frozen=True)
class RestoreResult:
    """What `restore` brought back: files in the ledger, and where the configuration went.

    `config` is "restored" (into the empty configuration root), "kept" (the root already had
    configuration; the copy is at `config_path`, beside the lake), "none" (the backup held no
    configuration) or "skipped" (no configuration root was given)."""

    files: int
    config: str = "skipped"
    config_path: Path | None = None


def remote_path(remote: str, sub: str) -> str:
    """`gdrive:flashpoint` + `raw` -> `gdrive:flashpoint/raw`; a bare `gdrive:` gets no slash."""
    remote = remote.rstrip("/\\")
    return f"{remote}{sub}" if remote.endswith(":") else f"{remote}/{sub}"


def _rclone(args: list[str], stop: threading.Event, *, allowed: tuple[int, ...] = ()) -> int:
    """Run `rclone <args>` with `RCLONE_LIMITS`; its exit code, 0 or one of `allowed`."""
    binary = shutil.which(RCLONE)
    if binary is None:
        raise BackupError("rclone not found on PATH")
    code, output = run_subprocess([binary, *args[:1], *RCLONE_LIMITS, *args[1:]], stop)
    if stop.is_set():
        raise BackupError(f"rclone {args[0]} stopped")
    if code is None or (code != 0 and code not in allowed):
        raise BackupError(f"rclone {args[0]} exited {code}: {output.strip()[-ERROR_CHARS:]}")
    return code


def config_files(root: Path) -> list[str]:
    """The configuration files under `root` (relative POSIX paths, sorted); [] if it's missing.

    An unreadable folder raises `OSError` rather than being skipped: a backup that silently
    left out `robots/` would remove the robots from the remote's latest copy."""
    names: list[str] = []
    for folder, suffix in CONFIG_FOLDERS.items():
        directory = root / folder
        if not directory.is_dir():
            continue
        names += [
            path.relative_to(root).as_posix()
            for path in directory.iterdir()
            if path.suffix == suffix and not path.name.startswith(".") and path.is_file()
        ]
    return sorted(names)


def config_hash(root: Path) -> str:
    """SHA-256 over the configuration files' names and contents, in name order."""
    digest = hashlib.sha256()
    for name in config_files(root):
        data = (root / name).read_bytes()
        digest.update(f"{name}\0{len(data)}\0".encode())
        digest.update(data)
    return digest.hexdigest()


def snapshot_config(root: Path, target: Path) -> list[str]:
    """Copy the configuration files under `root` into a fresh `target`; their names."""
    shutil.rmtree(target, ignore_errors=True)
    target.mkdir(parents=True)
    names = config_files(root)
    for name in names:
        destination = target / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / name, destination)
    return names


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


def run_backup(
    lake: LakePaths,
    remote: str,
    *,
    stop: threading.Event | None = None,
    max_duration: str | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    config_root: Path | None = None,
) -> str:
    """Copy raw (append-only), a metadata snapshot and, given `config_root`, the configuration
    files to `remote`; a one-line summary.

    With `max_duration`, each rclone call stops after it. A raw copy cut short still backs up
    the metadata and configuration, then raises `BackupError` (the rest of raw goes up next
    time). Configuration goes last, so a failure there never costs a log backup.
    """
    stop = stop or threading.Event()
    limit = ["--max-duration", max_duration] if max_duration else []
    stamp = now().strftime("%Y%m%dT%H%M%SZ")
    snapshot = snapshot_meta(lake)
    raw_done = True
    if lake.raw.is_dir():
        # raw.store writes `<sha>.<ext>.part` first; a torn one must never become immutable.
        raw_args = ["copy", "--immutable", "--exclude", PART_PATTERN, *limit]
        code = _rclone(
            [*raw_args, str(lake.raw), remote_path(remote, "raw")],
            stop,
            allowed=(EXIT_DURATION_EXCEEDED,) if max_duration else (),
        )
        raw_done = code == 0
    history = remote_path(remote, f"{HISTORY_DIR}/{stamp}")
    meta_args = ["sync", "--backup-dir", history, *limit]
    _rclone([*meta_args, str(snapshot), remote_path(remote, "meta/latest")], stop)
    backed_up, note = "raw and metadata", ""
    if config_root is not None:
        config_snapshot = lake.tmp / CONFIG_SNAPSHOT_DIR
        if snapshot_config(config_root, config_snapshot):
            config_history = remote_path(remote, f"{CONFIG_HISTORY_DIR}/{stamp}")
            config_args = ["sync", "--backup-dir", config_history, *limit]
            _rclone([*config_args, str(config_snapshot), remote_path(remote, CONFIG_LATEST)], stop)
            backed_up = "raw, metadata and configuration"
        else:  # an empty sync would empty the remote's latest copy: leave it as it is
            note = f"; no configuration files found in {config_root}"
    if not raw_done:
        raise BackupError(
            f"raw backup stopped after {max_duration} (metadata is backed up); the rest of raw"
            " goes up at the next backup"
        )
    return f"backed up {backed_up} to {remote}{note}"


def restore(
    lake: LakePaths,
    remote: str,
    *,
    force: bool = False,
    stop: threading.Event | None = None,
    config_root: Path | None = None,
) -> RestoreResult:
    """Copy raw and the latest metadata snapshot from `remote` into `lake`, and, given
    `config_root`, the backed-up configuration (`restore_config`).

    Every file's pipeline version is cleared, so `flashpoint rebuild` reprocesses all of raw
    into bronze and derives silver and gold again. `force` replaces the lake's ledger; it never
    lets configuration be overwritten.
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
        files = len(ledger.files())
    finally:
        ledger.close()
    if config_root is None:
        return RestoreResult(files)
    config, config_path = restore_config(lake, remote, config_root, stop)
    return RestoreResult(files, config, config_path)


def restore_config(
    lake: LakePaths, remote: str, config_root: Path, stop: threading.Event
) -> tuple[str, Path | None]:
    """Bring the backed-up configuration files back without overwriting any.

    Into `config_root` when it holds no configuration files, otherwise into
    `<lake>/restored-config/` (replaced each time); see `RestoreResult.config`.
    """
    staging = lake.tmp / CONFIG_RESTORE_DIR
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    try:
        code = _rclone(
            ["copy", remote_path(remote, CONFIG_LATEST), str(staging)],
            stop,
            allowed=(EXIT_DIRECTORY_NOT_FOUND,),  # a backup from before configuration was in it
        )
        names = [] if code == EXIT_DIRECTORY_NOT_FOUND else config_files(staging)
        if not names:
            return "none", None
        if config_files(config_root):
            target, outcome = lake.root / RESTORED_CONFIG_DIR, "kept"
            shutil.rmtree(target, ignore_errors=True)
        else:
            target, outcome = config_root, "restored"
        for name in names:
            destination = target / name
            if destination.exists():  # never overwrite, whatever the folder holds
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            (staging / name).replace(destination)
        return outcome, target
    finally:
        shutil.rmtree(staging, ignore_errors=True)


class BackupSchedule:
    """The watch's backup step (`run_cycle(backup=...)`).

    A cycle that changed raw or the ledger makes a backup owed, and so does configuration that
    differs from the last successful backup's (`config_sha`, given a `config_root`); an owed
    backup runs once `interval_min` has passed since the last attempt. The outcome, whether one
    is still owed and the configuration hash live in the status file's backup slot, so they
    survive a restart. A failure is recorded there and retried after the interval; it never
    stops the cycle.
    """

    def __init__(
        self,
        settings: BackupConfig,
        lake: LakePaths,
        tracker: "StatusTracker",
        stop: threading.Event,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        config_root: Path | None = None,
    ) -> None:
        self.settings = settings
        self.config_root = config_root
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
        config_sha = self._config_sha()
        config_changed = self.config_root is not None and config_sha != state.get("config_sha")
        pending = (
            bool(state.get("pending"))
            or result.raw_changed
            or result.ledger_changed
            or config_changed
        )
        state["pending"] = pending
        self.tracker.backup = state
        now = self.now()
        if not pending or not self._due(state.get("last_time"), now):
            return
        try:
            summary = run_backup(
                self.lake,
                remote,
                stop=self.stop,
                max_duration=WATCH_MAX_DURATION,
                now=self.now,
                config_root=self.config_root,
            )
        except (BackupError, OSError, sqlite3.Error) as exc:
            log.warning("backup failed: %s", exc)
            outcome, pending, backed_up_sha = f"error: {exc}", True, state.get("config_sha")
        else:
            log.info("%s", summary)
            outcome, pending, backed_up_sha = f"ok: {summary}", False, config_sha
        self.tracker.backup = {
            "last_time": now.isoformat(), "result": outcome, "pending": pending,
            "config_sha": backed_up_sha,
        }  # fmt: skip

    def _config_sha(self) -> str | None:
        """The configuration's hash now; None without a root or when it can't be read (the
        backup then runs and records the error)."""
        if self.config_root is None:
            return None
        try:
            return config_hash(self.config_root)
        except OSError:
            return None

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
