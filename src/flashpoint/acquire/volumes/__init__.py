"""Removable-volume detection: per-OS parsers and thin runners (dispatch lives in a later task)."""

import subprocess
from dataclasses import dataclass
from pathlib import Path

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
