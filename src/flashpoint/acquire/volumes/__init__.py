"""Removable-volume detection: per-OS parsers and thin runners, dispatched by `detect()`."""

import logging
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

OS_CALL_TIMEOUT_S = 10.0


class VolumeDetectionError(Exception):
    """An OS call or its output failed; the caller warns and skips USB for this cycle."""


class CommandFailedError(VolumeDetectionError):
    """An OS command ran but exited nonzero (as opposed to timing out or being missing)."""


@dataclass(frozen=True)
class Volume:
    id: str  # volume UUID/GUID; the label when the OS reports none
    label: str
    mount: Path


def run_os_command(args: list[str]) -> bytes:
    """Run an OS command with the shared timeout and return its stdout."""
    try:
        proc = subprocess.run(
            args,
            capture_output=True,
            stdin=subprocess.DEVNULL,
            timeout=OS_CALL_TIMEOUT_S,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise VolumeDetectionError(f"{args[0]} timed out after {OS_CALL_TIMEOUT_S:g} s") from exc
    except OSError as exc:
        raise VolumeDetectionError(f"cannot run {args[0]}: {exc}") from exc
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", "replace").strip()
        raise CommandFailedError(f"{args[0]} failed (exit {proc.returncode}): {detail}")
    return proc.stdout


def detect() -> list[Volume]:
    """Removable volumes mounted now, one per ID (Linux bind mounts can repeat a volume).

    A failed OS call logs a warning and finds nothing, so USB is skipped for this cycle.
    """
    from flashpoint.acquire.volumes import linux, macos, windows  # they import this module

    platform = sys.platform
    try:
        if platform == "darwin":
            found = macos.detect_macos()
        elif platform.startswith("linux"):
            found = linux.detect_linux()
        elif platform == "win32":
            found = windows.detect_windows()
        else:
            return []
    except VolumeDetectionError as exc:
        log.warning("removable volume detection failed, skipping USB this cycle: %s", exc)
        return []
    unique: dict[str, Volume] = {}
    for volume in found:
        unique.setdefault(volume.id, volume)
    return list(unique.values())
