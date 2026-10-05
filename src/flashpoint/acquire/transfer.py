"""Stable-file selection and verified copy into the inbox.

Selection (robot): skip active files (still being written) and files that changed across one
settle wait. Copy (all sources): stream to `<final>.part` while hashing, check size then hash,
then `os.replace` to the final name. A failed check never leaves a final file or a `.part`.
"""

import contextlib
import hashlib
import logging
import posixpath
import re
import threading
from collections.abc import Callable, Hashable, Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from functools import partial
from pathlib import Path
from typing import Protocol, TypeVar

import paramiko

from flashpoint.acquire.pulls import ROBOT_KIND, PullKey, PullLedger, PullStatus
from flashpoint.acquire.robot import RemoteFile, RobotClient

log = logging.getLogger(__name__)

WPILOG_SUFFIX = ".wpilog"
HOOT_SUFFIX = ".hoot"
PART_SUFFIX = ".part"
CHUNK_BYTES = 1024 * 1024  # the stop flag is checked between chunks
ROBOT_ERRORS: tuple[type[BaseException], ...] = (OSError, EOFError, paramiko.SSHException)
_UNSAFE_DIR_CHARS = re.compile(r'[<>:"/\\|?*]')  # not allowed in a Windows directory name
_UNSAFE_PART_CHARS = ("\\", ":")  # a Windows separator; an NTFS alternate data stream

F = TypeVar("F", bound=Hashable)


def active_files(
    files: Iterable[RemoteFile], dir_mtime_ns: Callable[[str], int]
) -> frozenset[RemoteFile]:
    """Robot files still being written: the newest `.wpilog` in each directory, and every
    `.hoot` in each root's newest hoot session directory (by directory mtime).

    A new file only appears when the robot code restarts, so the newest one is the open one.
    mtimes are compared, never read as dates (the rio clock is unreliable). Ties count as
    active, which errs towards waiting.
    """
    wpilogs: dict[tuple[str, str], list[RemoteFile]] = {}
    hoots: dict[str, dict[str, list[RemoteFile]]] = {}
    for f in files:
        directory = posixpath.dirname(f.relpath)
        suffix = posixpath.splitext(f.relpath)[1].lower()
        if suffix == WPILOG_SUFFIX:
            wpilogs.setdefault((f.root, directory), []).append(f)
        elif suffix == HOOT_SUFFIX:
            hoots.setdefault(f.root, {}).setdefault(directory, []).append(f)
    active: set[RemoteFile] = set()
    for group in wpilogs.values():
        newest = max(f.mtime_ns for f in group)
        active.update(f for f in group if f.mtime_ns == newest)
    for root, sessions in hoots.items():
        mtimes = {d: dir_mtime_ns(posixpath.join(root, d) if d else root) for d in sessions}
        newest = max(mtimes.values())
        for directory, group in sessions.items():
            if mtimes[directory] == newest:
                active.update(group)
    return frozenset(active)


def settle(
    candidates: Sequence[F],
    relist: Callable[[], Iterable[F]],
    *,
    settle_s: float,
    sleep: Callable[[float], None],
) -> list[F]:
    """Candidates unchanged across one settle wait (equal entries carry equal size and mtime).

    One wait per source per cycle; no candidates means no wait.
    """
    if not candidates:
        return []
    sleep(settle_s)
    second = set(relist())
    return [f for f in candidates if f in second]


@dataclass(frozen=True)
class RobotSelection:
    pull: list[RemoteFile]  # to transfer this cycle, in listing order
    active: frozenset[RemoteFile]  # members of `pull` taken while active (--include-active)
    skipped_active: list[RemoteFile]
    unstable: list[RemoteFile]


def robot_key(host: str, f: RemoteFile) -> PullKey:
    # `host` is recorded; the ledger dedupes robot files across hosts.
    return PullKey(ROBOT_KIND, host, f.remote_path, f.size, f.mtime_ns)


def select_robot_files(
    client: RobotClient,
    ledger: PullLedger,
    *,
    include_active: bool,
    settle_s: float,
    sleep: Callable[[float], None],
) -> RobotSelection:
    """List the robot, drop pulled/failed files, apply the active rule and the settle check.

    With `include_active`, active files are taken at once (no settle wait) and flagged.
    """
    first = client.list_logs()
    active = active_files(first, lambda path: client.stat(path)[1])
    todo = [f for f in first if ledger.should_pull(robot_key(client.host, f))]
    active_todo = [f for f in todo if f in active]
    candidates = [f for f in todo if f not in active]
    stable = set(settle(candidates, client.list_logs, settle_s=settle_s, sleep=sleep))
    taken = stable | (set(active_todo) if include_active else set())
    return RobotSelection(
        pull=[f for f in todo if f in taken],
        active=frozenset(active_todo) if include_active else frozenset(),
        skipped_active=[] if include_active else active_todo,
        unstable=[f for f in candidates if f not in stable],
    )


# --- verified copy (shared by robot and volume sources) ------------------------------------


class TransferStatus(StrEnum):
    VERIFIED = "verified"
    SIZE_VERIFIED = "size-verified"
    RETRY = "retry"  # attempt failed; retried next cycle
    FAILED = "failed"  # third failed attempt; no automatic retry
    STOPPED = "stopped"  # stop flag; not an attempt
    SKIPPED = "skipped"  # not attempted (already pulled, failed, inbox occupied, bad path)


@dataclass(frozen=True)
class TransferResult:
    source_path: str
    dest: Path
    bytes_copied: int
    status: TransferStatus
    reason: str | None = None
    source_lost: bool = False  # the source stopped answering; skip its remaining files
    sha256: str | None = None  # of the inbox copy, on success


class ByteSource(Protocol):
    def read(self, size: int, /) -> bytes: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class CopyJob:
    """One file to copy into the inbox.

    `open_source` opens the source for reading. `expected_sha256` is called after the copy:
    the robot's `sha256sum`, or a re-hash of a volume file; None means size-verified.
    `source_errors` are the exceptions that mean the read failed (anything else propagates);
    FileNotFoundError means the file vanished, which fails the attempt but not the source.
    """

    key: PullKey
    dest: Path
    size: int
    open_source: Callable[[], ByteSource]
    expected_sha256: Callable[[], str | None]
    include_active: bool = False
    source_errors: tuple[type[BaseException], ...] = (OSError, EOFError)


class _StopRequestedError(Exception):
    pass


class _AttemptFailedError(Exception):
    def __init__(self, reason: str, source_lost: bool) -> None:
        super().__init__(reason)
        self.reason = reason
        self.source_lost = source_lost


def inbox_path(inbox: Path, source: str, relpath: str) -> Path:
    """`inbox/<source>/<relpath>`; `relpath` is POSIX and must stay inside the source folder."""
    parts = relpath.split("/")
    if relpath.startswith("/") or any(
        p in ("", ".", "..") or any(c in p for c in _UNSAFE_PART_CHARS) for p in parts
    ):
        raise ValueError(f"unsafe relative path: {relpath!r}")
    return inbox.joinpath(_UNSAFE_DIR_CHARS.sub("_", source), *parts)


def part_path(dest: Path) -> Path:
    return dest.with_name(dest.name + PART_SUFFIX)


def sweep_parts(inbox: Path) -> int:
    """Remove `.part` files a killed run left in the inbox; how many. Hold the acquire lock."""
    removed = 0
    for path in sorted(inbox.rglob("*" + PART_SUFFIX)) if inbox.is_dir() else []:
        try:
            if path.is_file() and not path.is_symlink():
                path.unlink()
                removed += 1
        except OSError as exc:
            log.warning("cannot remove stale %s: %s", path, exc)
    if removed:
        log.info("removed %d stale partial file(s) from the inbox", removed)
    return removed


def verified_copy(job: CopyJob, ledger: PullLedger, stop: threading.Event) -> TransferResult:
    """Copy to `<dest>.part` while hashing, verify size then hash, `os.replace` to `dest`.

    On any failure the `.part` is removed and an attempt is recorded (the third makes the pull
    `failed`). That includes a local write error (a name Windows refuses, a full disk): it fails
    this file, not the source. A stop removes the `.part` without counting an attempt. An
    existing `dest` is never overwritten.
    """

    def result(
        status: TransferStatus,
        copied: int = 0,
        reason: str | None = None,
        source_lost: bool = False,
        sha256: str | None = None,
    ) -> TransferResult:
        return TransferResult(
            job.key.remote_path, job.dest, copied, status, reason, source_lost, sha256
        )

    if stop.is_set():
        return result(TransferStatus.STOPPED)
    if not ledger.should_pull(job.key):
        return result(TransferStatus.SKIPPED, reason="not-pending")
    if job.dest.exists() or job.dest.is_symlink():
        return result(TransferStatus.SKIPPED, reason="inbox-occupied")
    part = part_path(job.dest)
    copied = 0
    moved = False
    try:
        try:
            part.parent.mkdir(parents=True, exist_ok=True)
            copied, digest = _stream(job, part, stop)
        except OSError as exc:  # source errors are already _AttemptFailedError
            raise _AttemptFailedError(f"local-write-error: {exc}", source_lost=False) from exc
        if copied != job.size:
            raise _AttemptFailedError("size-mismatch", source_lost=False)
        if stop.is_set():  # the hash can take minutes on the rio, or re-read a whole stick file
            raise _StopRequestedError
        try:
            expected = job.expected_sha256()
        except FileNotFoundError as exc:  # rotated away after the copy; the source is fine
            raise _AttemptFailedError("vanished", source_lost=False) from exc
        except job.source_errors as exc:
            raise _AttemptFailedError(f"hash-error: {exc}", source_lost=True) from exc
        if expected is not None and expected.lower() != digest:
            raise _AttemptFailedError("hash-mismatch", source_lost=False)
        status = TransferStatus.VERIFIED if expected else TransferStatus.SIZE_VERIFIED
        try:
            part.replace(job.dest)  # os.replace: atomic within one directory
        except OSError as exc:
            raise _AttemptFailedError(f"local-write-error: {exc}", source_lost=False) from exc
        moved = True
        ledger.record_success(job.key, digest, PullStatus(status), job.include_active)
        return result(status, copied, sha256=digest)
    except _StopRequestedError:
        return result(TransferStatus.STOPPED)
    except _AttemptFailedError as failure:
        pulled = ledger.record_failure(job.key, failure.reason)
        status = TransferStatus.FAILED if pulled == PullStatus.FAILED else TransferStatus.RETRY
        log.warning("%s: %s (%s)", job.key.remote_path, failure.reason, status)
        return result(status, copied, reason=failure.reason, source_lost=failure.source_lost)
    finally:
        if not moved:
            try:
                part.unlink(missing_ok=True)
            except OSError as exc:  # an unusable name; swept at the next start
                log.warning("cannot remove %s: %s", part, exc)


def _stream(job: CopyJob, part: Path, stop: threading.Event) -> tuple[int, str]:
    """Read exactly up to `job.size` bytes into `part` in 1 MiB chunks; (bytes, sha256)."""
    try:
        source = job.open_source()
    except FileNotFoundError as exc:
        raise _AttemptFailedError("vanished", source_lost=False) from exc
    except job.source_errors as exc:
        raise _AttemptFailedError(f"read-error: {exc}", source_lost=True) from exc
    digest = hashlib.sha256()
    copied = 0
    try:
        with part.open("wb") as out:
            while copied < job.size:
                if copied and stop.is_set():
                    raise _StopRequestedError
                try:
                    chunk = source.read(min(CHUNK_BYTES, job.size - copied))
                except job.source_errors as exc:
                    raise _AttemptFailedError(f"read-error: {exc}", source_lost=True) from exc
                if not chunk:
                    break
                digest.update(chunk)
                out.write(chunk)
                copied += len(chunk)
    finally:
        with contextlib.suppress(*job.source_errors):
            source.close()
    return copied, digest.hexdigest()


# --- robot pulls ---------------------------------------------------------------------------


class _RobotHash:
    """`expected_sha256` for robot jobs; warns once per cycle when `sha256sum` is missing."""

    def __init__(self, client: RobotClient) -> None:
        self._client = client
        self._missing = False

    def expected(self, f: RemoteFile, active: bool) -> str | None:
        if active and self._client.stat(f.remote_path) != (f.size, f.mtime_ns):
            # Still being written: the robot cannot hash the prefix we read.
            log.info("%s grew during the pull; accepting it on size", f.remote_path)
            return None
        if self._missing:
            return None
        try:
            digest = self._client.remote_sha256(f.remote_path)
        except OSError:
            self._client.stat(f.remote_path)  # FileNotFoundError if the file is gone
            raise
        if digest is None:
            self._missing = True
            log.warning(
                "robot %s has no sha256sum; files are accepted on size (size-verified)",
                self._client.host,
            )
        return digest


def pull_robot(
    client: RobotClient,
    ledger: PullLedger,
    inbox: Path,
    selection: RobotSelection,
    *,
    stop: threading.Event,
) -> list[TransferResult]:
    """Pull the selected files into `inbox/<host>/<relpath>`, one result per attempted file.

    Stops early on the stop flag or when the robot stops answering (the rest wait for the next
    cycle, without counting an attempt). Never writes to the robot.
    """
    hasher = _RobotHash(client)
    results: list[TransferResult] = []
    for f in selection.pull:
        try:
            dest = inbox_path(inbox, client.host, f.relpath)
        except ValueError:
            log.warning("robot %s: skipping unsafe path %r", client.host, f.relpath)
            results.append(
                TransferResult(f.remote_path, inbox, 0, TransferStatus.SKIPPED, "bad-path")
            )
            continue
        active = f in selection.active
        job = CopyJob(
            key=robot_key(client.host, f),
            dest=dest,
            size=f.size,
            open_source=partial(client.open_remote, f.remote_path, f.size),
            expected_sha256=partial(hasher.expected, f, active),
            include_active=active,
            source_errors=ROBOT_ERRORS,
        )
        result = verified_copy(job, ledger, stop)
        results.append(result)
        if result.status == TransferStatus.STOPPED or result.source_lost:
            break
    return results
