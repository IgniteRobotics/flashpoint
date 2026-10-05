import logging
import sys
from pathlib import Path

import pytest

from flashpoint.acquire import volumes
from flashpoint.acquire.volumes import Volume, VolumeDetectionError, linux, macos, windows

STICK = Volume(id="UUID-1", label="ROBOTLOGS", mount=Path("/media/frc/ROBOTLOGS"))


@pytest.mark.parametrize(
    ("platform", "module", "func"),
    [
        ("darwin", macos, "detect_macos"),
        ("linux", linux, "detect_linux"),
        ("win32", windows, "detect_windows"),
    ],
)
def test_detect_dispatches_on_platform(
    monkeypatch: pytest.MonkeyPatch, platform: str, module: object, func: str
) -> None:
    called: list[str] = []

    def fake(name: str) -> object:
        def backend() -> list[Volume]:
            called.append(name)
            return [STICK]

        return backend

    monkeypatch.setattr(macos, "detect_macos", fake("macos"))
    monkeypatch.setattr(linux, "detect_linux", fake("linux"))
    monkeypatch.setattr(windows, "detect_windows", fake("windows"))
    monkeypatch.setattr(sys, "platform", platform)
    assert volumes.detect() == [STICK]
    assert called == [module.__name__.rsplit(".", 1)[1]]  # type: ignore[attr-defined]


def test_detect_unknown_platform_finds_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "freebsd14")
    assert volumes.detect() == []


def test_detect_dedupes_by_id(monkeypatch: pytest.MonkeyPatch) -> None:
    bind = Volume(id="UUID-1", label="bind", mount=Path("/mnt/bind"))
    other = Volume(id="UUID-2", label="OTHER", mount=Path("/media/frc/OTHER"))
    monkeypatch.setattr(linux, "detect_linux", lambda: [STICK, bind, other])
    monkeypatch.setattr(sys, "platform", "linux")
    assert volumes.detect() == [STICK, other]


def test_detect_failure_warns_and_skips_the_cycle(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken() -> list[Volume]:
        raise VolumeDetectionError("diskutil timed out after 10 s")

    monkeypatch.setattr(macos, "detect_macos", broken)
    monkeypatch.setattr(sys, "platform", "darwin")
    with caplog.at_level(logging.WARNING, logger="flashpoint.acquire.volumes"):
        assert volumes.detect() == []
    assert "diskutil timed out" in caplog.text
