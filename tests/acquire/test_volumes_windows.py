from pathlib import Path

import pytest

from flashpoint.acquire import volumes
from flashpoint.acquire.volumes import windows

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "volumes" / "windows"


def _load(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_single_usb_object_parses() -> None:
    assert windows.parse_powershell_json(_load("usb-single.json")) == [
        volumes.Volume(
            id="5e1d2b8a-3c4f-4a6b-9d7e-1f2a3b4c5d6e",
            label="ROBOTLOGS",
            mount=Path("E:\\"),
        )
    ]


def test_single_nvme_object_is_rejected() -> None:
    assert windows.parse_powershell_json(_load("nvme-single.json")) == []


def test_array_keeps_usb_and_sd_only() -> None:
    found = windows.parse_powershell_json(_load("mixed-array.json"))
    assert [(v.mount, v.label) for v in found] == [
        (Path("E:\\"), "ROBOTLOGS"),
        (Path("F:\\"), "F:"),
    ]


def test_empty_output_is_no_volumes() -> None:
    assert windows.parse_powershell_json("") == []
    assert windows.parse_powershell_json("  \r\n") == []
    assert windows.parse_powershell_json("[]") == []


def test_mmc_accepted_and_case_insensitive() -> None:
    text = '{"DriveLetter":"h","BusType":"mmc","UniqueId":null,"FileSystemLabel":"CARD"}'
    found = windows.parse_powershell_json(text)
    assert [v.mount for v in found] == [Path("H:\\")]


def test_unique_id_without_guid_is_kept_raw() -> None:
    text = '{"DriveLetter":"E","BusType":"USB","UniqueId":"opaque-id","FileSystemLabel":"X"}'
    assert windows.parse_powershell_json(text)[0].id == "opaque-id"


def test_missing_unique_id_falls_back_to_label() -> None:
    text = '{"DriveLetter":"E","BusType":"USB","UniqueId":null,"FileSystemLabel":"X"}'
    assert windows.parse_powershell_json(text)[0].id == "X"


def test_entry_without_drive_letter_is_skipped() -> None:
    text = '[{"DriveLetter":"","BusType":"USB","UniqueId":null,"FileSystemLabel":"X"}]'
    assert windows.parse_powershell_json(text) == []


def test_garbage_raises() -> None:
    with pytest.raises(volumes.VolumeDetectionError):
        windows.parse_powershell_json("not json")
    with pytest.raises(volumes.VolumeDetectionError):
        windows.parse_powershell_json("42")


def test_detect_runs_one_powershell_call(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str]) -> bytes:
        calls.append(args)
        return _load("mixed-array.json").encode()

    monkeypatch.setattr(volumes, "run_os_command", fake_run)
    found = windows.detect_windows()
    assert len(found) == 2
    assert len(calls) == 1
    assert calls[0][:3] == ["powershell", "-NoProfile", "-Command"]
    script = calls[0][3]
    for token in ("Get-Partition", "Get-Disk", "Get-Volume", "ConvertTo-Json"):
        assert token in script
