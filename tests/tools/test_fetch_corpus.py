import hashlib
import importlib.util
import subprocess
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "tools" / "fetch-corpus.py"
RELEASE_URL = "https://github.com/IgniteRobotics/flashpoint/releases/download/corpus-v1"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("fetch_corpus", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetch_corpus = _load()


def _entry(name: str, payload: bytes) -> dict[str, Any]:
    return {"name": name, "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}


def test_parses_github_release_url() -> None:
    assert fetch_corpus.github_release(RELEASE_URL) == ("IgniteRobotics/flashpoint", "corpus-v1")


def test_other_urls_are_not_github_releases() -> None:
    assert fetch_corpus.github_release("https://example.com/corpus-v1") is None


def test_downloads_through_gh_when_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = b"hoot bytes"
    entry = _entry("a.hoot", payload)
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], check: bool) -> subprocess.CompletedProcess[bytes]:
        calls.append(cmd)
        Path(cmd[cmd.index("--output") + 1]).write_bytes(payload)
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(fetch_corpus.shutil, "which", lambda _: "/usr/bin/gh")
    monkeypatch.setattr(fetch_corpus.subprocess, "run", fake_run)

    dest = tmp_path / "a.hoot"
    fetch_corpus.fetch_entry(entry, dest, RELEASE_URL, None)

    assert dest.read_bytes() == payload
    cmd = calls[0]
    assert cmd[:4] == ["/usr/bin/gh", "release", "download", "corpus-v1"]
    assert cmd[cmd.index("--repo") + 1] == "IgniteRobotics/flashpoint"
    assert cmd[cmd.index("--pattern") + 1] == "a.hoot"
    assert "--clobber" in cmd


def test_gh_failure_is_reported_as_oserror(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(cmd: list[str], check: bool) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.CalledProcessError(1, cmd)

    monkeypatch.setattr(fetch_corpus.shutil, "which", lambda _: "/usr/bin/gh")
    monkeypatch.setattr(fetch_corpus.subprocess, "run", fake_run)

    with pytest.raises(OSError, match="gh release download failed"):
        fetch_corpus.fetch_entry(_entry("a.hoot", b"x"), tmp_path / "a.hoot", RELEASE_URL, None)
    assert not (tmp_path / "a.hoot.part").exists()


def test_falls_back_to_plain_download_without_gh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = b"wpilog bytes"
    source = tmp_path / "remote.wpilog"
    source.write_bytes(payload)
    monkeypatch.setattr(fetch_corpus.shutil, "which", lambda _: None)
    monkeypatch.setattr(fetch_corpus.urllib.request, "urlopen", lambda url: source.open("rb"))

    dest = tmp_path / "a.wpilog"
    fetch_corpus.fetch_entry(_entry("a.wpilog", payload), dest, RELEASE_URL, None)

    assert dest.read_bytes() == payload
