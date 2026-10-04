"""One acquisition cycle: robot -> volumes -> ingest -> clear the inbox -> derive -> backup.

A failure in one source never stops the others; every outcome lands in the `CycleResult`,
which the watch loop writes as the status. Ingest (`--no-derive`) and derive run as separate
subprocesses, each in its own process group, so the watch process stays small and a stop ends
everything they started. Derive is retried on later cycles until it succeeds
(`derive_pending`), so an inbox file can be cleared once ingested: raw storage holds it. The
backup step is a hook called last.
"""

import contextlib
import dataclasses
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum, StrEnum
from pathlib import Path
from typing import Any

from flashpoint import config as flashpoint_config
from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.pulls import PullLedger, PullStatus
from flashpoint.acquire.removable import RemovableMedia, VolumeSelection, volume_source
from flashpoint.acquire.robot import LowSpaceWarning, RobotClient
from flashpoint.acquire.transfer import (
    ROBOT_ERRORS,
    TransferResult,
    TransferStatus,
    pull_robot,
    select_robot_files,
    settle,
)
from flashpoint.lake.ledger import Ledger, Stage
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.raw import file_sha256

log = logging.getLogger(__name__)

INCOMPLETE_READ = "incomplete-read"
LOG_SUFFIXES = (".wpilog", ".hoot")  # what `flashpoint ingest` picks up (`.part` never matches)
INGEST_BATCH = 200  # explicit paths per ingest run; Windows command lines stop at 32 KiB
STOP_POLL_S = 0.2
TERMINATE_GRACE_S = 5.0
OUTPUT_TAIL_CHARS = 4000
_DONE_STAGES = (Stage.SUCCESS, Stage.QUARANTINED)
_LEDGER_UNTOUCHED = (TransferStatus.SKIPPED, TransferStatus.STOPPED)


class SourceStatus(StrEnum):
    OK = "ok"
    UNREACHABLE = "unreachable"  # no robot answered
    ERROR = "error"  # the step raised; `error` says what
    STOPPED = "stopped"


class InboxOutcome(StrEnum):
    SUCCESS = "success"  # ingested this cycle; removed
    QUARANTINED = "quarantined"  # quarantined this cycle; removed
    SKIPPED = "skipped"  # already in the ledger; removed
    KEPT = "kept"  # ingest crashed or left it unfinished; retried next cycle
    UNSTABLE = "unstable"  # a manual drop still changing; not ingested


@dataclass(frozen=True)
class PlannedFile:
    """A dry-run entry: what would be pulled or copied."""

    source: str  # robot host, or `usb-<label>`
    path: str  # remote path, or the path relative to the volume root
    size: int


@dataclass
class SourceOutcome:
    kind: str  # "robot" or "volume"
    id: str  # robot host, or volume UUID ("" when volume detection itself failed)
    label: str  # robot host, or volume label
    status: SourceStatus = SourceStatus.OK
    error: str | None = None
    transfers: list[TransferResult] = field(default_factory=list)
    planned: list[PlannedFile] = field(default_factory=list)  # dry run only
    unstable: int = 0  # changed during the settle wait; next cycle
    skipped_active: int = 0  # robot files still being written (no --include-active)


@dataclass(frozen=True)
class InboxFile:
    path: Path
    sha256: str
    origin: str  # "pull" (its hash is a recorded pull) or "manual" (dropped by hand)
    outcome: InboxOutcome


@dataclass
class IngestOutcome:
    returncode: int | None  # None when it could not start or was stopped
    crashed: bool  # non-zero exit with no ledger change: everything kept
    stopped: bool
    ledger_changed: bool
    output: str  # the tail of ingest's output


@dataclass
class DeriveOutcome:
    returncode: int | None  # None when it could not start or was stopped
    output: str  # the tail of derive's output


@dataclass
class CycleResult:
    started: str
    finished: str = ""
    dry_run: bool = False
    stopped: bool = False
    sources: list[SourceOutcome] = field(default_factory=list)
    low_space: list[LowSpaceWarning] = field(default_factory=list)
    size_verified_hosts: list[str] = field(default_factory=list)
    inbox: list[InboxFile] = field(default_factory=list)
    ingest: IngestOutcome | None = None  # None: nothing to ingest, stopped, or dry run
    derive: DeriveOutcome | None = None  # None: no derive owed, stopped, or dry run
    derive_pending: bool = False  # derive still owed: pass it to the next run_cycle
    failed_pulls: list[dict[str, Any]] = field(default_factory=list)
    raw_changed: bool = False  # new files reached raw storage (for backup)
    ledger_changed: bool = False  # pulls, files or warnings changed (for backup)
    errors: list[str] = field(default_factory=list)  # non-source steps that raised

    def as_dict(self) -> dict[str, Any]:
        """JSON-ready: paths as strings, enums as their values."""
        converted = _jsonable(dataclasses.asdict(self))
        assert isinstance(converted, dict)
        return converted


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return value


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def ingest_command(paths: Sequence[Path], lake: LakePaths) -> list[str]:
    return [
        sys.executable, "-m", "flashpoint", "ingest", *map(str, paths), "--lake", str(lake.root),
        "--no-derive",
    ]  # fmt: skip


def derive_command(lake: LakePaths) -> list[str]:
    return [sys.executable, "-m", "flashpoint", "derive", "--lake", str(lake.root)]


IngestCommand = Callable[[Sequence[Path], LakePaths], list[str]]
DeriveCommand = Callable[[LakePaths], list[str]]
Connect = Callable[[AcquireConfig], RobotClient | None]


def _connect(config: AcquireConfig) -> RobotClient | None:
    return RobotClient.connect(config)


def run_cycle(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    media: RemovableMedia,
    *,
    stop: threading.Event,
    include_active: bool = False,
    dry_run: bool = False,
    sleep: Callable[[float], None] = time.sleep,
    connect: Connect = _connect,
    ingest_command: IngestCommand = ingest_command,
    derive_command: DeriveCommand = derive_command,
    derive_pending: bool = False,
    backup: Callable[["CycleResult"], None] | None = None,
) -> CycleResult:
    """Run one cycle. A set `stop` skips every remaining step.

    `dry_run` connects and lists, then reports `planned` files per source; it transfers,
    ingests, clears, derives and backs up nothing. `derive_pending` is the previous cycle's
    `result.derive_pending`: derive runs when it is set or when ingest changed the ledger, and
    stays pending until it exits 0. `backup` is called last with the result (not on a dry run
    or a stop); g7 wires the rclone backup in here.
    """
    result = CycleResult(started=_now(), dry_run=dry_run, derive_pending=derive_pending)
    result.sources.append(
        _robot_step(config, pulls, lake, result, stop, include_active, dry_run, sleep, connect)
    )
    if not stop.is_set():
        result.sources += _volume_step(media, result, stop, dry_run)
    if not dry_run and not stop.is_set():
        try:
            _inbox_step(config, lake, pulls, result, stop, sleep, ingest_command)
        except Exception as exc:  # noqa: BLE001 - the cycle must still report
            log.exception("inbox ingest failed")
            result.errors.append(f"inbox: {_describe(exc)}")
    if not dry_run and result.derive_pending and not stop.is_set():
        try:
            _derive_step(lake, result, stop, derive_command)
        except Exception as exc:  # noqa: BLE001
            log.exception("derive step failed")
            result.errors.append(f"derive: {_describe(exc)}")
    if not dry_run:
        result.failed_pulls = pulls.failed()
        result.ledger_changed = result.ledger_changed or any(
            t.status not in _LEDGER_UNTOUCHED for s in result.sources for t in s.transfers
        )
    if backup is not None and not dry_run and not stop.is_set():
        try:
            backup(result)
        except Exception as exc:  # noqa: BLE001
            log.exception("backup step failed")
            result.errors.append(f"backup: {_describe(exc)}")
    result.stopped = stop.is_set()
    result.finished = _now()
    return result


# --- sources -------------------------------------------------------------------------------


def _robot_step(
    config: AcquireConfig,
    pulls: PullLedger,
    lake: LakePaths,
    result: CycleResult,
    stop: threading.Event,
    include_active: bool,
    dry_run: bool,
    sleep: Callable[[float], None],
    connect: Connect,
) -> SourceOutcome:
    outcome = SourceOutcome("robot", "", "")
    try:
        client = connect(config)
        if client is None:
            outcome.status = SourceStatus.UNREACHABLE
            return outcome
        with client:
            outcome.id = outcome.label = client.host
            result.low_space += client.low_space_warnings()
            selection = select_robot_files(
                client, pulls, include_active=include_active, settle_s=config.settle_s, sleep=sleep
            )
            outcome.unstable = len(selection.unstable)
            outcome.skipped_active = len(selection.skipped_active)
            if dry_run:
                outcome.planned = [
                    PlannedFile(client.host, f.remote_path, f.size) for f in selection.pull
                ]
                return outcome
            outcome.transfers = pull_robot(client, pulls, lake.inbox, selection, stop=stop)
        if any(t.status == TransferStatus.SIZE_VERIFIED for t in outcome.transfers):
            result.size_verified_hosts.append(outcome.id)
        if stop.is_set():
            outcome.status = SourceStatus.STOPPED
    except Exception as exc:  # noqa: BLE001 - one source never stops the others
        log.warning("robot step failed: %s", _describe(exc), exc_info=_unexpected(exc))
        outcome.status, outcome.error = SourceStatus.ERROR, _describe(exc)
    return outcome


def _unexpected(exc: BaseException) -> bool:
    return not isinstance(exc, ROBOT_ERRORS)


def _volume_step(
    media: RemovableMedia, result: CycleResult, stop: threading.Event, dry_run: bool
) -> list[SourceOutcome]:
    try:
        selections = media.select()
    except Exception as exc:  # noqa: BLE001
        log.exception("removable volume detection failed")
        return [SourceOutcome("volume", "", "", SourceStatus.ERROR, _describe(exc))]
    outcomes: list[SourceOutcome] = []
    for selection in selections:
        outcomes.append(_volume_outcome(media, selection, stop, dry_run))
        if stop.is_set():
            break
    return outcomes


def _volume_outcome(
    media: RemovableMedia, selection: VolumeSelection, stop: threading.Event, dry_run: bool
) -> SourceOutcome:
    volume = selection.volume
    outcome = SourceOutcome("volume", volume.id, volume.label, unstable=len(selection.unstable))
    if dry_run:
        source = volume_source(volume)
        outcome.planned = [PlannedFile(source, f.relpath, f.size) for f in selection.pull]
        return outcome
    try:
        outcome.transfers = media.pull([selection], stop=stop)
        if stop.is_set():
            outcome.status = SourceStatus.STOPPED
    except Exception as exc:  # noqa: BLE001
        log.exception("volume %s failed", volume.label)
        outcome.status, outcome.error = SourceStatus.ERROR, _describe(exc)
    return outcome


# --- inbox: settle, ingest, clear ----------------------------------------------------------


@dataclass(frozen=True)
class _Ready:
    path: Path
    sha256: str
    origin: str
    include_active: bool
    was_done: bool  # already success/quarantined at this pipeline version before ingest


def _inbox_logs(inbox: Path) -> list[Path]:
    if not inbox.is_dir():
        return []
    found: list[Path] = []
    for directory, _dirs, files in os.walk(inbox):
        for name in files:
            if not name.startswith(".") and Path(name).suffix.lower() in LOG_SUFFIXES:
                found.append(Path(directory, name))
    return sorted(found)


def _stat_entries(paths: Sequence[Path]) -> Iterator[tuple[Path, int, int]]:
    for path in paths:
        try:
            st = path.stat()
        except FileNotFoundError:
            continue
        yield path, st.st_size, st.st_mtime_ns


def _inbox_step(
    config: AcquireConfig,
    lake: LakePaths,
    pulls: PullLedger,
    result: CycleResult,
    stop: threading.Event,
    sleep: Callable[[float], None],
    ingest_command: IngestCommand,
) -> None:
    pulled = {t.dest: t.sha256 for s in result.sources for t in s.transfers if t.sha256 is not None}
    files = _inbox_logs(lake.inbox)
    others = [p for p in files if p not in pulled]
    first = list(_stat_entries(others))
    settled = settle(first, lambda: _stat_entries(others), settle_s=config.settle_s, sleep=sleep)
    stable = {path for path, _size, _mtime in settled}
    unstable = [p for p in others if p not in stable]
    hashes: dict[Path, str] = {}
    for path in files:
        if path in pulled:
            hashes[path] = pulled[path]
        elif path in stable:
            try:
                hashes[path] = file_sha256(path)
            except FileNotFoundError:
                unstable.append(path)
    if not hashes:
        result.inbox += _unstable_entries(unstable)
        return
    ledger = Ledger(lake.ledger)
    try:
        ready = [_ready(path, sha, pulls, ledger) for path, sha in hashes.items()]
        # Only the settled paths, never the inbox directory: ingest would re-walk it and pick
        # up a drop that appeared after the settle wait. Files already done skip ingest.
        targets = [r.path for r in ready if not r.was_done]
        crashed = False
        if targets:
            result.ingest = _run_ingest(targets, lake, ledger, stop, ingest_command)
            changed = result.ingest.ledger_changed
            result.ledger_changed |= changed
            result.raw_changed |= changed and result.ingest.stopped  # stored before the stop
            result.derive_pending |= changed
            if result.ingest.stopped:
                return
            crashed = result.ingest.crashed
        for item in ready:
            kept = crashed and not item.was_done
            result.inbox.append(_settle_file(item, lake.inbox, ledger, result, kept))
    finally:
        ledger.close()
    result.inbox += _unstable_entries(unstable)


def _unstable_entries(paths: Sequence[Path]) -> list[InboxFile]:
    return [InboxFile(p, "", "manual", InboxOutcome.UNSTABLE) for p in paths]


def _ready(path: Path, sha: str, pulls: PullLedger, ledger: Ledger) -> _Ready:
    rows = pulls.query(
        "SELECT include_active FROM pulls WHERE sha256 = ? AND status IN (?, ?)",
        (sha, PullStatus.VERIFIED, PullStatus.SIZE_VERIFIED),
    )
    return _Ready(
        path=path,
        sha256=sha,
        origin="pull" if rows else "manual",
        include_active=any(r["include_active"] for r in rows),
        was_done=not ledger.needs_processing(sha, flashpoint_config.PIPELINE_VERSION),
    )


def _settle_file(
    item: _Ready, inbox: Path, ledger: Ledger, result: CycleResult, kept: bool
) -> InboxFile:
    """Clear one inbox file once the ledger has it as success or quarantined.

    Derive may still be pending; that is retried on its own, from the lake.
    """
    stage = ledger.stage(item.sha256)
    if kept or stage not in _DONE_STAGES:
        return InboxFile(item.path, item.sha256, item.origin, InboxOutcome.KEPT)
    if item.include_active:
        ledger.add_warning(item.sha256, INCOMPLETE_READ)
        result.ledger_changed = True
    if item.was_done:
        outcome = InboxOutcome.SKIPPED
    else:
        outcome = InboxOutcome(stage)
        result.raw_changed = True
    try:
        item.path.unlink(missing_ok=True)
    except OSError as exc:  # e.g. open elsewhere on Windows; cleared as skipped next cycle
        log.warning("cannot remove %s from the inbox: %s", item.path, exc)
    else:
        _prune(item.path.parent, inbox)
    return InboxFile(item.path, item.sha256, item.origin, outcome)


def _prune(directory: Path, inbox: Path) -> None:
    """Remove empty directories from `directory` up to, never including, the inbox root."""
    while directory != inbox and inbox in directory.parents:
        try:
            directory.rmdir()
        except OSError:
            return
        directory = directory.parent


def _data_version(ledger: Ledger) -> int:
    # Changes whenever another connection (the ingest subprocess) commits.
    return int(ledger.query("PRAGMA data_version")[0]["data_version"])


def _run_ingest(
    targets: Sequence[Path],
    lake: LakePaths,
    ledger: Ledger,
    stop: threading.Event,
    ingest_command: IngestCommand,
) -> IngestOutcome:
    before = _data_version(ledger)
    returncode: int | None = 0
    output: list[str] = []
    stopped = False
    for start in range(0, len(targets), INGEST_BATCH):
        argv = ingest_command(targets[start : start + INGEST_BATCH], lake)
        code, text = _run_subprocess(argv, stop)
        output.append(text)
        if code is None:
            stopped = stop.is_set()
            returncode = None
            break
        if code != 0 and returncode == 0:
            returncode = code
    changed = _data_version(ledger) != before
    tail = "".join(output)[-OUTPUT_TAIL_CHARS:]
    crashed = not stopped and returncode != 0 and not changed
    if crashed:
        log.warning("ingest exited with %s and changed nothing; inbox kept\n%s", returncode, tail)
    return IngestOutcome(returncode, crashed, stopped, changed, tail)


def _derive_step(
    lake: LakePaths, result: CycleResult, stop: threading.Event, derive_command: DeriveCommand
) -> None:
    code, text = _run_subprocess(derive_command(lake), stop)
    tail = text[-OUTPUT_TAIL_CHARS:]
    result.derive = DeriveOutcome(code, tail)
    if code is not None:
        result.ledger_changed = True  # derive rewrites the derived tables
    if code == 0:
        result.derive_pending = False
    elif not stop.is_set():
        log.warning("derive exited with %s; retrying next cycle\n%s", code, tail)
        result.errors.append(f"derive: exit {code}")


def _new_process_group() -> dict[str, Any]:
    if sys.platform == "win32":
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    return {"start_new_session": True}


def _end_process_group(proc: "subprocess.Popen[str]") -> None:
    """Ask the whole group to stop, then kill whatever is left after the grace period."""
    if sys.platform == "win32":
        proc.send_signal(signal.CTRL_BREAK_EVENT)  # delivered to the whole process group
        try:
            proc.wait(TERMINATE_GRACE_S)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGTERM)
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(TERMINATE_GRACE_S)
    # The group id outlives the leader while any member is alive; kill stragglers too.
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, signal.SIGKILL)
    proc.wait()


def _run_subprocess(argv: list[str], stop: threading.Event) -> tuple[int | None, str]:
    """Run ingest or derive in its own process group; (exit code, output).

    None: it could not start, or the stop flag ended it (with everything it started).
    """
    try:
        proc = subprocess.Popen(  # noqa: S603 - argv is our own interpreter and paths
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            **_new_process_group(),
        )
    except OSError as exc:
        log.error("cannot start %s: %s", argv[:4], exc)
        return None, _describe(exc)
    while True:
        try:
            out, _ = proc.communicate(timeout=STOP_POLL_S)
            return proc.returncode, out or ""
        except subprocess.TimeoutExpired:
            if stop.is_set():
                break
    _end_process_group(proc)
    if proc.stdout is not None:
        proc.stdout.close()
    return None, ""
