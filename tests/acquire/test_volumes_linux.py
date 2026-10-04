import os
from pathlib import Path

import pytest

from flashpoint.acquire import volumes
from flashpoint.acquire.volumes import linux

MOUNTINFO = (
    Path(__file__).resolve().parents[1] / "fixtures" / "volumes" / "linux" / "mountinfo-pit.txt"
)

USB_PATH = "devices/pci0000:00/0000:00:14.0/usb1/1-2/1-2:1.0/host6/target6:0:0/6:0:0:0/block"
NVME_PATH = "devices/pci0000:00/0000:00:1d.0/0000:3d:00.0/nvme/nvme0/nvme0n1"


def _symlink(target: Path, link: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks unavailable")


def _disk(sys: Path, base: str, disk: str, removable: str) -> Path:
    path = sys / base / disk
    path.mkdir(parents=True)
    (path / "removable").write_text(removable + "\n")
    _symlink(path, sys / "block" / disk)
    return path


def _part(sys: Path, disk_dir: Path, part: str) -> None:
    path = disk_dir / part
    path.mkdir()
    (path / "partition").write_text("1\n")
    _symlink(path, sys / "class" / "block" / part)


@pytest.fixture
def fake_sys(tmp_path: Path) -> tuple[Path, Path]:
    sys = tmp_path / "sys"
    sdb = _disk(sys, f"{USB_PATH}", "sdb", "1")
    _part(sys, sdb, "sdb1")
    # USB SSD that reports removable=0 (second device on the same USB hub)
    sdc = _disk(sys, USB_PATH.replace("6:0:0:0", "7:0:0:0"), "sdc", "0")
    _part(sys, sdc, "sdc1")
    sdd = _disk(sys, USB_PATH.replace("6:0:0:0", "8:0:0:0"), "sdd", "1")
    _part(sys, sdd, "sdd1")
    nvme = _disk(sys, NVME_PATH.rsplit("/", 1)[0], "nvme0n1", "0")
    _part(sys, nvme, "nvme0n1p3")
    by_uuid = tmp_path / "by-uuid"
    by_uuid.mkdir()
    (by_uuid / "6A1B-2C3D").symlink_to("../../sdb1")
    (by_uuid / "7e5a1f0c-aaaa-4bbb-8ccc-0123456789ab").symlink_to("../../sdc1")
    (by_uuid / "0123-4567").symlink_to("../../nvme0n1p3")
    return sys, by_uuid


def test_parse_mountinfo_unescapes_octal_and_extracts_fields() -> None:
    entries = linux.parse_mountinfo(MOUNTINFO.read_text())
    stick = next(e for e in entries if e.device == "/dev/sdb1")
    assert stick.mount == "/run/media/pit/ROBOT LOGS"
    assert stick.fstype == "vfat"


def test_parse_mountinfo_skips_blank_and_malformed_lines() -> None:
    assert linux.parse_mountinfo("\n\ngarbage line\n") == []


def test_candidates_limited_to_media_roots_and_block_devices() -> None:
    entries = linux.parse_mountinfo(MOUNTINFO.read_text())
    got = {e.device for e in linux.removable_candidates(entries)}
    # nas (nfs), tmpfs, / and /boot/efi never qualify; nvme0n1p3 is under /mnt.
    assert got == {"/dev/nvme0n1p3", "/dev/sdb1", "/dev/sdc1", "/dev/sdd1"}


def test_stick_removable_flag_accepted(fake_sys: tuple[Path, Path]) -> None:
    sys, _ = fake_sys
    assert linux.is_removable_device("sdb1", sys) is True


def test_usb_ssd_with_removable_zero_accepted_by_bus_path(fake_sys: tuple[Path, Path]) -> None:
    sys, _ = fake_sys
    assert linux.is_removable_device("sdc1", sys) is True


def test_internal_nvme_rejected(fake_sys: tuple[Path, Path]) -> None:
    sys, _ = fake_sys
    assert linux.is_removable_device("nvme0n1p3", sys) is False


def test_unknown_device_rejected(fake_sys: tuple[Path, Path]) -> None:
    sys, _ = fake_sys
    assert linux.is_removable_device("sdz9", sys) is False


def test_detect_end_to_end(fake_sys: tuple[Path, Path], tmp_path: Path) -> None:
    sys, by_uuid = fake_sys
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(MOUNTINFO.read_text())
    found = linux.detect_linux(mountinfo_path=mountinfo, sys_root=sys, by_uuid_dir=by_uuid)
    assert {(v.id, v.label, v.mount) for v in found} == {
        ("6A1B-2C3D", "ROBOT LOGS", Path("/run/media/pit/ROBOT LOGS")),
        ("7e5a1f0c-aaaa-4bbb-8ccc-0123456789ab", "SSDLOGS", Path("/media/pit/SSDLOGS")),
        # no by-uuid entry: the label is the identifier
        ("NOUUID", "NOUUID", Path("/media/pit/NOUUID")),
    }


def test_detect_missing_by_uuid_dir_falls_back_to_label(
    fake_sys: tuple[Path, Path], tmp_path: Path
) -> None:
    sys, _ = fake_sys
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(MOUNTINFO.read_text())
    found = linux.detect_linux(
        mountinfo_path=mountinfo, sys_root=sys, by_uuid_dir=tmp_path / "absent"
    )
    assert {v.id for v in found} == {"ROBOT LOGS", "SSDLOGS", "NOUUID"}


def test_unreadable_mountinfo_raises(tmp_path: Path) -> None:
    with pytest.raises(volumes.VolumeDetectionError):
        linux.detect_linux(
            mountinfo_path=tmp_path / "nope", sys_root=tmp_path, by_uuid_dir=tmp_path
        )


def test_detect_on_real_host_does_not_crash_off_linux() -> None:
    if os.name == "nt":
        pytest.skip("no /proc")
    try:
        result = linux.detect_linux()
    except volumes.VolumeDetectionError:
        return
    assert isinstance(result, list)
