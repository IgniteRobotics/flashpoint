"""macOS removable-volume detection via `diskutil info -plist`."""

import plistlib
from pathlib import Path
from typing import Any

from flashpoint.acquire import volumes
from flashpoint.acquire.volumes import Volume, VolumeDetectionError

VOLUMES_DIR = Path("/Volumes")
NETWORK_FILESYSTEMS = frozenset({"smbfs", "afpfs", "nfs", "webdav"})
DISK_IMAGE_BUS = "Disk Image"  # a mounted .dmg has no VirtualOrPhysical key, only this


def parse_diskutil_info(data: bytes, fallback_mount: Path | None = None) -> Volume | None:
    """Return the Volume when `diskutil info -plist` output describes a removable volume."""
    try:
        info: Any = plistlib.loads(data)
    except (plistlib.InvalidFileException, ValueError) as exc:
        raise VolumeDetectionError(f"unreadable diskutil plist: {exc}") from exc
    if not isinstance(info, dict):
        raise VolumeDetectionError("diskutil plist is not a dictionary")

    removable = bool(
        info.get("Ejectable") or info.get("RemovableMedia") or not info.get("Internal", True)
    )
    virtual = (
        info.get("VirtualOrPhysical") == "Virtual" or info.get("BusProtocol") == DISK_IMAGE_BUS
    )
    if not removable or virtual or info.get("FilesystemType") in NETWORK_FILESYSTEMS:
        return None

    mount_point = info.get("MountPoint")
    mount = Path(mount_point) if mount_point else fallback_mount
    if mount is None:
        return None
    label = str(info.get("VolumeName") or mount.name)
    return Volume(id=str(info.get("VolumeUUID") or label), label=label, mount=mount)


def detect_macos(volumes_dir: Path = VOLUMES_DIR) -> list[Volume]:
    """Query every entry under /Volumes (except the boot-volume symlink)."""
    try:
        entries = sorted(volumes_dir.iterdir())
    except OSError:
        return []
    found: list[Volume] = []
    for entry in entries:
        if entry.is_symlink() and entry.resolve() == Path("/"):
            continue
        output = volumes.run_os_command(["diskutil", "info", "-plist", str(entry)])
        volume = parse_diskutil_info(output, fallback_mount=entry)
        if volume is not None:
            found.append(volume)
    return found
