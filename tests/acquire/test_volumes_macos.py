import plistlib
import subprocess
from pathlib import Path

import pytest

from flashpoint.acquire import volumes
from flashpoint.acquire.volumes import macos

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "volumes" / "macos"


def _load(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_usb_stick_is_accepted() -> None:
    vol = macos.parse_diskutil_info(_load("usb-stick.plist"))
    assert vol == volumes.Volume(
        id="1A2B3C4D-0000-4000-8000-00000000A001",
        label="ROBOTLOGS",
        mount=Path("/Volumes/ROBOTLOGS"),
    )


def test_internal_ssd_is_rejected() -> None:
    assert macos.parse_diskutil_info(_load("internal-ssd.plist")) is None


def test_disk_image_is_rejected_although_ejectable() -> None:
    # The captured dmg reports Ejectable, RemovableMedia, not Internal and has no
    # VirtualOrPhysical key; only BusProtocol marks it as an image.
    info = plistlib.loads(_load("dmg.plist"))
    assert info["Ejectable"] is True and info["Internal"] is False
    assert macos.parse_diskutil_info(_load("dmg.plist")) is None


def test_virtual_or_physical_virtual_is_rejected() -> None:
    info = plistlib.loads(_load("usb-stick.plist"))
    info["VirtualOrPhysical"] = "Virtual"
    assert macos.parse_diskutil_info(plistlib.dumps(info)) is None


def test_smb_share_is_rejected() -> None:
    assert macos.parse_diskutil_info(_load("smb-share.plist")) is None


@pytest.mark.parametrize("fs", ["smbfs", "afpfs", "nfs", "webdav"])
def test_network_filesystems_rejected(fs: str) -> None:
    info = plistlib.loads(_load("usb-stick.plist"))
    info["FilesystemType"] = fs
    assert macos.parse_diskutil_info(plistlib.dumps(info)) is None


def test_non_internal_without_ejectable_flags_is_accepted() -> None:
    info = plistlib.loads(_load("usb-stick.plist"))
    info.update(Ejectable=False, RemovableMedia=False, Internal=False)
    assert macos.parse_diskutil_info(plistlib.dumps(info)) is not None


def test_internal_flags_all_false_is_rejected() -> None:
    info = plistlib.loads(_load("usb-stick.plist"))
    info.update(Ejectable=False, RemovableMedia=False, Internal=True)
    assert macos.parse_diskutil_info(plistlib.dumps(info)) is None


def test_missing_uuid_falls_back_to_label() -> None:
    info = plistlib.loads(_load("usb-stick.plist"))
    del info["VolumeUUID"]
    vol = macos.parse_diskutil_info(plistlib.dumps(info))
    assert vol is not None and vol.id == "ROBOTLOGS"


def test_missing_mount_point_uses_fallback_then_rejects() -> None:
    info = plistlib.loads(_load("usb-stick.plist"))
    del info["MountPoint"]
    data = plistlib.dumps(info)
    vol = macos.parse_diskutil_info(data, fallback_mount=Path("/Volumes/X"))
    assert vol is not None and vol.mount == Path("/Volumes/X")
    assert macos.parse_diskutil_info(data) is None


def test_malformed_plist_raises() -> None:
    with pytest.raises(volumes.VolumeDetectionError):
        macos.parse_diskutil_info(b"not a plist")


def test_detect_skips_root_symlink_and_queries_each_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "Volumes"
    root.mkdir()
    (root / "ROBOTLOGS").mkdir()
    try:
        (root / "Macintosh HD").symlink_to("/")
    except OSError:
        pytest.skip("symlinks unavailable")
    calls: list[list[str]] = []

    def fake_run(args: list[str]) -> bytes:
        calls.append(args)
        return _load("usb-stick.plist")

    monkeypatch.setattr(volumes, "run_os_command", fake_run)
    found = macos.detect_macos(volumes_dir=root)
    assert [v.label for v in found] == ["ROBOTLOGS"]
    assert calls == [["diskutil", "info", "-plist", str(root / "ROBOTLOGS")]]


def test_detect_skips_entry_whose_diskutil_exits_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "STALE").mkdir()
    (tmp_path / "ROBOTLOGS").mkdir()

    def fake_run(args: list[str]) -> bytes:
        if args[-1].endswith("STALE"):
            raise volumes.CommandFailedError("diskutil failed (exit 1)")
        return _load("usb-stick.plist")

    monkeypatch.setattr(volumes, "run_os_command", fake_run)
    assert [v.label for v in macos.detect_macos(volumes_dir=tmp_path)] == ["ROBOTLOGS"]


def test_detect_timeout_still_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "X").mkdir()

    def fake_run(args: list[str]) -> bytes:
        raise volumes.VolumeDetectionError("diskutil timed out")

    monkeypatch.setattr(volumes, "run_os_command", fake_run)
    with pytest.raises(volumes.VolumeDetectionError):
        macos.detect_macos(volumes_dir=tmp_path)


def test_detect_missing_volumes_dir_is_empty(tmp_path: Path) -> None:
    assert macos.detect_macos(volumes_dir=tmp_path / "nope") == []


def test_run_os_command_timeout_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args: object, **kwargs: object) -> None:
        raise subprocess.TimeoutExpired(cmd="diskutil", timeout=10)

    monkeypatch.setattr(subprocess, "run", boom)
    with pytest.raises(volumes.VolumeDetectionError, match="timed out"):
        volumes.run_os_command(["diskutil", "info"])


def test_run_os_command_failure_and_missing_binary_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failed(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(args=[], returncode=1, stdout=b"", stderr=b"bad")

    monkeypatch.setattr(subprocess, "run", failed)
    with pytest.raises(volumes.VolumeDetectionError, match="exit 1"):
        volumes.run_os_command(["x"])

    def missing(*args: object, **kwargs: object) -> None:
        raise FileNotFoundError("x")

    monkeypatch.setattr(subprocess, "run", missing)
    with pytest.raises(volumes.VolumeDetectionError):
        volumes.run_os_command(["x"])


def test_run_os_command_passes_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def ok(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        seen.update(kwargs)
        return subprocess.CompletedProcess(args=[], returncode=0, stdout=b"hi", stderr=b"")

    monkeypatch.setattr(subprocess, "run", ok)
    assert volumes.run_os_command(["x"]) == b"hi"
    assert seen["stdin"] == subprocess.DEVNULL
    assert seen["timeout"] == volumes.OS_CALL_TIMEOUT_S == 10.0
