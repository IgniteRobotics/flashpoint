"""The acquire loop: one lock per lake, signals as a stop flag, cycles, and the status file.

`run_acquire` takes `<lake>/meta/acquire.lock` (released by the OS when the holder dies, which
is the stale-lock takeover), runs one cycle or repeats them every `poll_s` measured start to
start, and writes `<lake>/meta/acquire-status.json` atomically after every cycle. A dry run
takes no lock and writes nothing under the lake: it reads a copy of the pull ledger.
"""

import contextlib
import json
import logging
import os
import shutil
import signal
import socket
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType, TracebackType
from typing import Any, Self, TextIO

from flashpoint.acquire import volumes
from flashpoint.acquire.backup import BackupSchedule
from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.cycle import Connect, CycleResult, SourceStatus, run_cycle
from flashpoint.acquire.pulls import PullLedger
from flashpoint.acquire.removable import RemovableMedia
from flashpoint.acquire.robot import RobotClient
from flashpoint.lake.paths import LakePaths

log = logging.getLogger(__name__)

LOCK_FILE = "acquire.lock"
STATUS_VERSION = 1
EXIT_ERRORS = 1  # a one-shot cycle had a step or source error
EXIT_LOCKED = 3
EXIT_INTERRUPTED = 130
DERIVE_ERROR_CHARS = 2000
_WINDOWS_LOCK_OFFSET = 1 << 20  # lock a byte past the holder text so others can still read it

Cycle = Callable[..., CycleResult]


def _connect(config: AcquireConfig) -> RobotClient | None:
    return RobotClient.connect(config)


# --- lock ------------------------------------------------------------------------------------


class LockHeldError(RuntimeError):
    """Another acquire holds the lake's lock; the message names it."""


class AcquireLock:
    """An exclusive, non-blocking lock on `<lake>/meta/acquire.lock`.

    The file holds the holder's pid, host and start time for the error message. The OS drops
    the lock when the holder exits or is killed; no pid liveness check is made.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            _lock(fd)
        except OSError:
            os.close(fd)
            raise LockHeldError(
                f"another acquire is running on this lake ({self._holder()}); lock: {self.path}"
            ) from None
        holder = {
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "started": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, json.dumps(holder).encode())
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        with contextlib.suppress(OSError):
            _unlock(self._fd)
        os.close(self._fd)
        self._fd = None

    def _holder(self) -> str:
        try:
            info = json.loads(self.path.read_text(encoding="utf-8"))
            return f"pid {info['pid']} on {info['host']} since {info['started']}"
        except (OSError, ValueError, KeyError, TypeError):
            return "holder unknown"

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()


if sys.platform == "win32":
    import msvcrt

    def _lock(fd: int) -> None:
        os.lseek(fd, _WINDOWS_LOCK_OFFSET, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)

    def _unlock(fd: int) -> None:
        os.lseek(fd, _WINDOWS_LOCK_OFFSET, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _lock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)


# --- signals ---------------------------------------------------------------------------------


@contextlib.contextmanager
def stop_on_signals(stop: threading.Event) -> Iterator[None]:
    """SIGINT, SIGTERM (and SIGBREAK on Windows) set `stop`; later ones are only logged.

    Ingest and derive run in their own process groups, so a terminal's Ctrl-C reaches only
    this process; the cycle ends them when it sees the flag. Main thread only.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def handler(signum: int, _frame: FrameType | None) -> None:
        name = signal.Signals(signum).name
        if stop.is_set():
            log.info("received %s: already stopping", name)
            return
        log.info("received %s: stopping", name)
        stop.set()

    signums = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        signums.append(signal.SIGBREAK)
    previous = {signum: signal.signal(signum, handler) for signum in signums}
    try:
        yield
    finally:
        for signum, old in previous.items():
            signal.signal(signum, old)


# --- status ----------------------------------------------------------------------------------


class StatusTracker:
    """Builds `acquire-status.json` from each cycle and writes it atomically.

    A low-space warning stays active until a later reading of the same robot recovers, also
    across restarts (it is read back from the previous file), as does the backup slot.
    `backup` is filled by the backup step: `{"last_time": iso | None, "result": str | None,
    "pending": bool}` (the last attempt, its outcome, and whether a backup is still owed).
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._low_space: list[dict[str, Any]] = []
        self.backup: dict[str, Any] = {"last_time": None, "result": None, "pending": False}
        self._derive_error: str | None = None
        self.derive_pending = False  # owed by an earlier run
        previous = read_status(path)
        if previous is not None and previous.get("version") == STATUS_VERSION:
            try:
                self._restore(previous)
            except (AttributeError, KeyError, TypeError):
                log.warning("ignoring malformed acquire status %s", path)
        self.status: dict[str, Any] = {}

    def _restore(self, previous: dict[str, Any]) -> None:
        low_space = [
            {"kind": "low-space", "host": str(w["host"]), "root": str(w["root"]),
             "free_bytes": int(w["free_bytes"])}
            for w in previous.get("warnings") or []
            if w.get("kind") == "low-space"
        ]  # fmt: skip
        backup = previous.get("backup") or {}
        derive = previous.get("derive") or {}
        error = derive.get("error")
        self._low_space = low_space
        self.backup = {
            "last_time": backup.get("last_time"),
            "result": backup.get("result"),
            "pending": bool(backup.get("pending")),
        }
        self._derive_error = None if error is None else str(error)
        self.derive_pending = bool(derive.get("pending"))

    def record(self, result: CycleResult) -> dict[str, Any]:
        # A warning clears only when that host and root got a fresh reading.
        read = {(r.host, r.root) for r in result.space_readings}
        read |= {(w.host, w.root) for w in result.low_space}
        self._low_space = [w for w in self._low_space if (w["host"], w["root"]) not in read] + [
            {"kind": "low-space", "host": w.host, "root": w.root, "free_bytes": w.free_bytes}
            for w in result.low_space
        ]
        derive = result.derive
        if derive is not None and derive.returncode is not None:
            self._derive_error = (
                None
                if derive.returncode == 0
                else f"exit {derive.returncode}: {derive.output[-DERIVE_ERROR_CHARS:]}"
            )
        elif not result.derive_pending:
            self._derive_error = None
        ingest = result.ingest
        self.status = {
            "version": STATUS_VERSION,
            "last_cycle": {
                "started": result.started,
                "finished": result.finished,
                "stopped": result.stopped,
            },
            "sources": [
                {
                    "kind": s.kind,
                    "id": s.id,
                    "label": s.label,
                    "status": s.status.value,
                    "error": s.error,
                    "transfers": _count(t.status.value for t in s.transfers),
                    "unstable": s.unstable,
                    "skipped_active": s.skipped_active,
                }
                for s in result.sources
            ],
            "warnings": self._low_space
            + [{"kind": "size-verified", "host": h} for h in result.size_verified_hosts],
            "failed_files": [
                {
                    "source_kind": row["source_kind"],
                    "source": row["source_id"],
                    "path": row["remote_path"],
                    "reason": row["reason"],
                    "attempts": row["attempts"],
                }
                for row in result.failed_pulls
            ],
            "inbox": _count(f.outcome.value for f in result.inbox),
            "ingest": None
            if ingest is None
            else {
                "returncode": ingest.returncode,
                "crashed": ingest.crashed,
                "stopped": ingest.stopped,
            },
            "derive": {"pending": result.derive_pending, "error": self._derive_error},
            "errors": list(result.errors),
            "backup": self.backup,
        }
        return self.status

    def write(self) -> bool:
        """Write the status atomically; an unwritable file is logged, never raised."""
        try:
            write_json_atomic(self.path, self.status)
        except OSError as exc:
            log.error("cannot write acquire status %s: %s", self.path, exc)
            return False
        return True


def _count(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    """A temp file in the same directory, then `os.replace` (atomic on all three OSes)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        tmp.replace(path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise


def read_status(path: Path) -> dict[str, Any] | None:
    """The last written status; None when there is none or it cannot be read."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _source_name(kind: str, name: str | None) -> str:
    # An unreachable robot or a failed volume detection has no name.
    return f"{kind} {name}" if name else kind


class StatusFormatError(ValueError):
    """The status file is from another version or not in the expected shape."""


def _dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(v, dict) for v in value):
        raise StatusFormatError(f"expected a list of objects, got {value!r:.60}")
    return value


def _dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise StatusFormatError(f"expected an object, got {value!r:.60}")
    return value


def describe_status(status: dict[str, Any]) -> list[str]:
    """Lines for `flashpoint doctor`; `StatusFormatError` for an unknown or malformed file."""
    if status.get("version") != STATUS_VERSION:
        raise StatusFormatError(
            f"status version {status.get('version')!r}, expected {STATUS_VERSION}"
        )
    cycle = _dict(status.get("last_cycle"))
    stopped = " (stopped)" if cycle.get("stopped") else ""
    lines = [f"last cycle {cycle.get('finished') or cycle.get('started') or '?'}{stopped}"]
    for s in _dicts(status.get("sources", [])):
        name = _source_name(str(s.get("kind", "?")), s.get("label") or s.get("id"))
        error = f" ({s['error']})" if s.get("error") else ""
        moved = ", ".join(f"{n} {k}" for k, n in sorted(_dict(s.get("transfers")).items()))
        lines.append(f"{name}: {s.get('status', '?')}{error}{f'; {moved}' if moved else ''}")
    for w in _dicts(status.get("warnings", [])):
        if w.get("kind") == "low-space":
            free = w.get("free_bytes", "?")
            lines.append(f"low-space {w.get('host', '?')} {w.get('root', '?')}: {free} bytes free")
        else:
            lines.append(f"{w.get('kind', '?')} {w.get('host', '')}".rstrip())
    for f in _dicts(status.get("failed_files", [])):
        where = f"{f.get('source_kind', '?')} {f.get('source', '?')} {f.get('path', '?')}"
        lines.append(f"failed {where}: {f.get('reason')}")
    derive = _dict(status.get("derive"))
    if derive.get("error"):
        lines.append(f"derive error: {derive['error']}")
    elif derive.get("pending"):
        lines.append("derive pending")
    errors = status.get("errors", [])
    lines += [f"error: {e}" for e in (errors if isinstance(errors, list) else [errors])]
    backup = _dict(status.get("backup"))
    if backup.get("last_time"):
        lines.append(f"last backup {backup['last_time']}: {backup.get('result')}")
    else:
        lines.append("last backup never")
    return lines


# --- the loop --------------------------------------------------------------------------------


def run_acquire(
    config: AcquireConfig,
    lake: LakePaths,
    *,
    watch: bool = False,
    include_active: bool = False,
    dry_run: bool = False,
    stop: threading.Event | None = None,
    cycle: Cycle = run_cycle,
    connect: Connect = _connect,
    detect: Callable[[], list[volumes.Volume]] = volumes.detect,
    out: TextIO | None = None,
) -> int:
    """`flashpoint acquire`: one cycle, a watch until stopped, or a dry run; the exit code.

    Without `stop`, SIGINT/SIGTERM set an internal one. One-shot returns 0 for a normal cycle
    (also with no robot in range), `EXIT_ERRORS` when a step or source failed, and
    `EXIT_INTERRUPTED` when stopped; the watch returns 0 when stopped.
    """
    own_stop = stop is None
    stop = stop or threading.Event()
    with stop_on_signals(stop) if own_stop else contextlib.nullcontext():
        if dry_run:
            return _dry_run(config, lake, stop, include_active, cycle, connect, detect, out)
        lock = AcquireLock(lake.meta / LOCK_FILE)
        try:
            lock.acquire()
        except LockHeldError as exc:
            print(exc, file=sys.stderr)
            return EXIT_LOCKED
        try:
            return _loop(config, lake, stop, watch, include_active, cycle, connect, detect)
        finally:
            lock.release()


def _loop(
    config: AcquireConfig,
    lake: LakePaths,
    stop: threading.Event,
    watch: bool,
    include_active: bool,
    cycle: Cycle,
    connect: Connect,
    detect: Callable[[], list[volumes.Volume]],
) -> int:
    def sleep(seconds: float) -> None:  # settle waits end early on a stop
        stop.wait(seconds)

    pulls = PullLedger(lake.ledger)
    media = RemovableMedia.from_config(config, pulls, lake.inbox, sleep=sleep, detect=detect)
    tracker = StatusTracker(lake.status)
    derive_pending = tracker.derive_pending
    # Backup belongs to the watch; a one-shot acquire leaves it to `flashpoint backup`.
    backup = BackupSchedule(config.backup, lake, tracker, stop) if watch else None
    try:
        while True:
            started = time.monotonic()
            result = cycle(
                config, lake, pulls, media, stop=stop, include_active=include_active,
                sleep=sleep, connect=connect, derive_pending=derive_pending, backup=backup,
            )  # fmt: skip
            derive_pending = result.derive_pending
            tracker.record(result)
            tracker.write()
            _log_summary(result)
            if not watch or stop.is_set():
                break
            # Start to start: a long cycle (a big pull) is followed by the next one at once.
            stop.wait(max(0.0, started + config.poll_s - time.monotonic()))
            if stop.is_set():
                break
    finally:
        pulls.close()
    if watch:
        return 0
    if result.stopped:
        return EXIT_INTERRUPTED
    failed = result.errors or any(s.status == SourceStatus.ERROR for s in result.sources)
    return EXIT_ERRORS if failed or (result.ingest and result.ingest.crashed) else 0


def _log_summary(result: CycleResult) -> None:
    moved = _count(t.status.value for s in result.sources for t in s.transfers)
    inbox = _count(f.outcome.value for f in result.inbox)
    sources = ", ".join(
        f"{_source_name(s.kind, s.label or s.id)} {s.status.value}" for s in result.sources
    )
    log.info(
        "cycle done%s: %s; transfers %s; inbox %s",
        " (stopped)" if result.stopped else "",
        sources or "no sources",
        moved or "none",
        inbox or "empty",
    )


def _dry_run(
    config: AcquireConfig,
    lake: LakePaths,
    stop: threading.Event,
    include_active: bool,
    cycle: Cycle,
    connect: Connect,
    detect: Callable[[], list[volumes.Volume]],
    out: TextIO | None,
) -> int:
    """List what would be pulled; nothing under the lake is created or modified.

    The pull ledger is read from a copy, because opening it in place would create the file on
    a fresh lake (or the pulls table on an old one) and touch SQLite's shared-memory file.
    """
    out = out or sys.stdout

    def sleep(seconds: float) -> None:
        stop.wait(seconds)

    with tempfile.TemporaryDirectory(prefix="flashpoint-dry-run-") as scratch:
        copy = Path(scratch) / lake.ledger.name
        for suffix in ("", "-wal"):
            source = lake.ledger.with_name(lake.ledger.name + suffix)
            if source.is_file():
                shutil.copyfile(source, copy.with_name(copy.name + suffix))
        pulls = PullLedger(copy)
        try:
            media = RemovableMedia.from_config(
                config, pulls, lake.inbox, sleep=sleep, detect=detect
            )
            result = cycle(
                config, lake, pulls, media, stop=stop, include_active=include_active,
                dry_run=True, sleep=sleep, connect=connect,
            )  # fmt: skip
        finally:
            pulls.close()
    total = 0
    for s in result.sources:
        name = _source_name(s.kind, s.label or s.id)
        error = f" ({s.error})" if s.error else ""
        print(f"{name}: {s.status.value}{error}, {len(s.planned)} to copy", file=out)
        for planned in s.planned:
            print(f"  {planned.source}  {planned.path}  {planned.size} bytes", file=out)
            total += planned.size
    count = sum(len(s.planned) for s in result.sources)
    print(f"dry run: {count} file(s), {total} bytes would be copied; nothing changed", file=out)
    return EXIT_INTERRUPTED if result.stopped else 0
