"""Linux removable-volume detection from /proc/self/mountinfo and /sys."""

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from flashpoint.acquire.volumes import Volume, VolumeDetectionError

MOUNTINFO = Path("/proc/self/mountinfo")
SYS_ROOT = Path("/sys")
BY_UUID_DIR = Path("/dev/disk/by-uuid")
MOUNT_ROOTS = (PurePosixPath("/media"), PurePosixPath("/run/media"), PurePosixPath("/mnt"))

_OCTAL_ESCAPE = re.compile(r"\\([0-7]{3})")


@dataclass(frozen=True)
class MountEntry:
    device: str
    mount: str
    fstype: str


def _unescape(field: str) -> str:
    return _OCTAL_ESCAPE.sub(lambda m: chr(int(m.group(1), 8)), field)


def parse_mountinfo(text: str) -> list[MountEntry]:
    """Parse mountinfo lines; fields before the lone `-` are fixed, after it fstype and source."""
    entries: list[MountEntry] = []
    for line in text.splitlines():
        fields = line.split()
        if "-" not in fields[4:]:
            continue
        sep = fields.index("-", 4)
        if len(fields) < sep + 3:
            continue
        entries.append(
            MountEntry(
                device=_unescape(fields[sep + 2]),
                mount=_unescape(fields[4]),
                fstype=fields[sep + 1],
            )
        )
    return entries


def removable_candidates(entries: list[MountEntry]) -> list[MountEntry]:
    """Block-device mounts under /media, /run/media or /mnt."""
    return [
        e
        for e in entries
        if e.device.startswith("/dev/")
        and any(PurePosixPath(e.mount).is_relative_to(root) for root in MOUNT_ROOTS)
    ]


def is_removable_device(name: str, sys_root: Path = SYS_ROOT) -> bool:
    """True when the disk under partition `name` has removable=1 or sits on a USB bus path."""
    link = sys_root / "class" / "block" / name
    if not link.exists():
        return False
    real = link.resolve()
    disk = real.parent.name if (real / "partition").exists() else real.name
    try:
        bus_path = "/" + real.relative_to(sys_root.resolve()).as_posix()
    except ValueError:
        bus_path = real.as_posix()
    try:
        removable = (sys_root / "block" / disk / "removable").read_text().strip() == "1"
    except OSError:
        removable = False
    return removable or "/usb" in bus_path


def _uuid_for(name: str, by_uuid_dir: Path) -> str | None:
    try:
        links = list(by_uuid_dir.iterdir())
    except OSError:
        return None
    for link in links:
        try:
            if link.readlink().name == name:
                return link.name
        except OSError:
            continue
    return None


def detect_linux(
    mountinfo_path: Path = MOUNTINFO,
    sys_root: Path = SYS_ROOT,
    by_uuid_dir: Path = BY_UUID_DIR,
) -> list[Volume]:
    """Detect removable volumes; roots are injectable for tests."""
    try:
        text = mountinfo_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise VolumeDetectionError(f"cannot read {mountinfo_path}: {exc}") from exc
    found: list[Volume] = []
    for entry in removable_candidates(parse_mountinfo(text)):
        name = PurePosixPath(entry.device).name
        if not is_removable_device(name, sys_root):
            continue
        label = PurePosixPath(entry.mount).name
        found.append(
            Volume(id=_uuid_for(name, by_uuid_dir) or label, label=label, mount=Path(entry.mount))
        )
    return found
