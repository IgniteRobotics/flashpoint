"""Removable-media acquisition: scan a volume, copy its logs into the inbox, say when to eject.

The volume is only ever opened for reading. Copies go through the shared verified-copy core;
the expected hash is a re-hash of the source in the same cycle. A stick pulled out mid-copy
is not a failed attempt of the file: its `.part` is removed and the volume is left for now.
"""

import hashlib
import logging
import os
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import BinaryIO, Self

from flashpoint.acquire import volumes
from flashpoint.acquire.config import AcquireConfig
from flashpoint.acquire.pulls import PullKey, PullLedger
from flashpoint.acquire.transfer import (
    CHUNK_BYTES,
    HOOT_SUFFIX,
    WPILOG_SUFFIX,
    CopyJob,
    TransferResult,
    TransferStatus,
    inbox_path,
    settle,
    verified_copy,
)
from flashpoint.acquire.volumes import Volume

log = logging.getLogger(__name__)

MAX_SCAN_DEPTH = 4  # directory levels below the volume root
SOURCE_KIND = "volume"
SKIP_DIRS = frozenset({"System Volume Information", "$RECYCLE.BIN"})  # plus every dot-directory
_LOG_SUFFIXES = (WPILOG_SUFFIX, HOOT_SUFFIX)
_UNSAFE_LABEL_CHARS = re.compile(r"[^A-Za-z0-9_-]")
_ID_PREFIX_LEN = 8


@dataclass(frozen=True)
class VolumeFile:
    relpath: str  # POSIX, relative to the volume root
    size: int
    mtime_ns: int


def scan_volume(
    mount: Path, max_depth: int = MAX_SCAN_DEPTH, errors: list[str] | None = None
) -> list[VolumeFile]:
    """`.wpilog` and `.hoot` files up to `max_depth` directories deep, in a stable order.

    Hidden entries (including AppleDouble `._*` files) and OS metadata directories are
    skipped, and symlinks are never followed. An unreadable subdirectory is skipped with a
    warning and noted in `errors`; an unreadable root raises OSError (the volume went away).
    """
    found: list[VolumeFile] = []

    def walk(directory: Path, prefix: str, depth: int) -> None:
        with os.scandir(directory) as it:
            entries = sorted(it, key=lambda e: e.name)
        for entry in entries:
            if entry.name.startswith(".") or entry.name in SKIP_DIRS:
                continue
            relpath = f"{prefix}{entry.name}"
            try:
                if entry.is_dir(follow_symlinks=False):
                    if depth < max_depth:
                        try:
                            walk(Path(entry.path), f"{relpath}/", depth + 1)
                        except OSError as exc:
                            log.warning("skipping unreadable %s: %s", entry.path, exc)
                            if errors is not None:
                                errors.append(f"{relpath}: {exc}")
                elif entry.is_file(follow_symlinks=False) and entry.name.lower().endswith(
                    _LOG_SUFFIXES
                ):
                    st = entry.stat(follow_symlinks=False)
                    found.append(VolumeFile(relpath, st.st_size, st.st_mtime_ns))
            except FileNotFoundError:
                continue

    walk(mount, "", 0)
    return found


def volume_source(volume: Volume) -> str:
    """The inbox folder: `usb-<label>`, the label limited to `[A-Za-z0-9_-]`.

    An empty label uses the first 8 characters of the volume ID instead.
    """
    name = volume.label or volume.id[:_ID_PREFIX_LEN]
    return "usb-" + _UNSAFE_LABEL_CHARS.sub("_", name)


def volume_key(volume: Volume, f: VolumeFile) -> PullKey:
    # The path is relative to the volume root, so a stick remounted elsewhere is the same file.
    return PullKey(SOURCE_KIND, volume.id, f.relpath, f.size, f.mtime_ns)


@dataclass(frozen=True)
class VolumeSelection:
    volume: Volume
    files: list[VolumeFile]  # every log found on the volume this cycle
    pull: list[VolumeFile]  # to copy this cycle (pending and stable), in scan order
    unstable: list[VolumeFile]  # changed during the settle wait
    insertion: tuple[str, int]  # (volume ID, mount time): one safe-to-eject notice per value
    scan_errors: tuple[str, ...] = ()  # unreadable directories: logs may be unseen, no notice


def select_volume_files(
    volume: Volume,
    ledger: PullLedger,
    *,
    settle_s: float,
    sleep: Callable[[float], None],
) -> VolumeSelection:
    """Scan, drop pulled/failed files, keep those unchanged across one settle wait.

    Volumes have no active-file rule: nothing writes to a stick in the pit machine.
    Raises OSError if the volume can't be read.
    """
    mounted_ns = volume.mount.stat().st_mtime_ns
    errors: list[str] = []
    files = scan_volume(volume.mount, errors=errors)
    todo = [f for f in files if ledger.should_pull(volume_key(volume, f))]
    relist = partial(scan_volume, volume.mount, errors=errors)
    stable = set(settle(todo, relist, settle_s=settle_s, sleep=sleep))
    return VolumeSelection(
        volume=volume,
        files=files,
        pull=[f for f in todo if f in stable],
        unstable=[f for f in todo if f not in stable],
        insertion=(volume.id, mounted_ns),
        scan_errors=tuple(dict.fromkeys(errors)),  # the settle re-scan repeats them
    )


# --- copy ----------------------------------------------------------------------------------


def _open_readonly(path: Path) -> BinaryIO:
    return path.open("rb")


class _VolumeGoneError(Exception):
    """The volume was removed; not an attempt (deliberately not an OSError)."""


class _Mount:
    """Tells a read error on a volume apart from the volume being pulled out."""

    def __init__(self, mount: Path) -> None:
        self._mount = mount
        self._dev = mount.stat().st_dev

    def present(self) -> bool:
        try:
            return self._mount.stat().st_dev == self._dev  # unmounted: the dir is gone or moved
        except OSError:
            return False

    def check(self, exc: OSError) -> None:
        if not self.present():
            raise _VolumeGoneError(str(exc)) from exc


class _GuardedReader:
    def __init__(self, f: BinaryIO, mount: _Mount) -> None:
        self._f = f
        self._mount = mount

    def read(self, size: int, /) -> bytes:
        try:
            chunk = self._f.read(size)
        except OSError as exc:
            self._mount.check(exc)
            raise
        if not chunk and not self._mount.present():
            raise _VolumeGoneError("short read")
        return chunk

    def close(self) -> None:
        self._f.close()


def _open_source(path: Path, mount: _Mount) -> _GuardedReader:
    try:
        return _GuardedReader(_open_readonly(path), mount)
    except OSError as exc:
        mount.check(exc)
        raise


def _rehash(path: Path, mount: _Mount) -> str:
    try:
        digest = hashlib.sha256()
        with _open_readonly(path) as f:
            while chunk := f.read(CHUNK_BYTES):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError as exc:
        mount.check(exc)
        raise


def pull_volume(
    ledger: PullLedger,
    inbox: Path,
    selection: VolumeSelection,
    *,
    stop: threading.Event,
) -> list[TransferResult]:
    """Copy the selected files into `inbox/usb-<label>/<relpath>`, one result per attempt.

    Stops early on the stop flag or a lost volume; the rest wait for the next cycle.
    """
    volume = selection.volume
    if not selection.pull:
        return []
    try:
        mount = _Mount(volume.mount)
    except OSError as exc:
        log.warning("volume %s went away before the copy: %s", volume.label, exc)
        return []
    source = volume_source(volume)
    results: list[TransferResult] = []
    for f in selection.pull:
        try:
            dest = inbox_path(inbox, source, f.relpath)
        except ValueError:
            log.warning("volume %s: skipping unsafe path %r", volume.label, f.relpath)
            results.append(TransferResult(f.relpath, inbox, 0, TransferStatus.SKIPPED, "bad-path"))
            continue
        path = volume.mount.joinpath(*f.relpath.split("/"))
        job = CopyJob(
            key=volume_key(volume, f),
            dest=dest,
            size=f.size,
            open_source=partial(_open_source, path, mount),
            expected_sha256=partial(_rehash, path, mount),
        )
        try:
            result = verified_copy(job, ledger, stop)
        except _VolumeGoneError:
            log.warning("volume %s was removed during the copy of %s", volume.label, f.relpath)
            result = TransferResult(
                f.relpath, dest, 0, TransferStatus.RETRY, "volume-removed", source_lost=True
            )
        results.append(result)
        if result.status == TransferStatus.STOPPED or result.source_lost:
            break
    return results


# --- the per-process driver ----------------------------------------------------------------


class RemovableMedia:
    """Removable-media acquisition across watch cycles.

    Keep one instance for the life of the watch loop: it remembers which insertions already
    got their safe-to-eject notice. With `enabled=False` (`--no-usb`) nothing is detected.
    """

    def __init__(
        self,
        ledger: PullLedger,
        inbox: Path,
        *,
        enabled: bool,
        settle_s: float,
        sleep: Callable[[float], None],
        detect: Callable[[], list[Volume]] = volumes.detect,
    ) -> None:
        self._ledger = ledger
        self._inbox = inbox
        self._enabled = enabled
        self._settle_s = settle_s
        self._sleep = sleep
        self._detect = detect
        self._announced: set[tuple[str, int]] = set()

    @classmethod
    def from_config(
        cls,
        config: AcquireConfig,
        ledger: PullLedger,
        inbox: Path,
        *,
        sleep: Callable[[float], None],
        detect: Callable[[], list[Volume]] = volumes.detect,
    ) -> Self:
        return cls(
            ledger,
            inbox,
            enabled=config.removable_media,
            settle_s=config.settle_s,
            sleep=sleep,
            detect=detect,
        )

    def select(self) -> list[VolumeSelection]:
        """Detect volumes and choose what to copy, without copying (also the dry run)."""
        if not self._enabled:
            return []
        found = self._detect()
        present = {v.id for v in found}
        self._announced = {k for k in self._announced if k[0] in present}  # removed: forget
        selections: list[VolumeSelection] = []
        for volume in found:
            try:
                selections.append(
                    select_volume_files(
                        volume, self._ledger, settle_s=self._settle_s, sleep=self._sleep
                    )
                )
            except OSError as exc:
                log.warning("cannot read volume %s at %s: %s", volume.label, volume.mount, exc)
        return selections

    def pull(
        self, selections: list[VolumeSelection], *, stop: threading.Event
    ) -> list[TransferResult]:
        """Copy each selection, then log safe-to-eject for volumes with nothing left to do."""
        results: list[TransferResult] = []
        for selection in selections:
            results.extend(pull_volume(self._ledger, self._inbox, selection, stop=stop))
            if stop.is_set():
                break
            self._announce_if_done(selection)
        return results

    def cycle(self, *, stop: threading.Event) -> list[TransferResult]:
        return self.pull(self.select(), stop=stop)

    def _announce_if_done(self, selection: VolumeSelection) -> None:
        if selection.insertion in self._announced or selection.scan_errors:
            return
        volume = selection.volume
        if all(self._ledger.is_pulled(volume_key(volume, f)) for f in selection.files):
            self._announced.add(selection.insertion)
            log.info("volume %s (%s) is safe to remove", volume.label, volume.mount)
