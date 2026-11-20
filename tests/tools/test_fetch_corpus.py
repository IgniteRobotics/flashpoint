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


# --- --group (ci-quality-gates, "First-run smoke on every platform") ------------------------


def _two_group_manifest(tmp_path: Path) -> tuple[Path, Path]:
    source = tmp_path / "source"
    source.mkdir()
    lines = ['version = "corpus-test"', f'release_url = "{RELEASE_URL}"', ""]
    for group, name in (("a", "a1.wpilog"), ("a", "a2.hoot"), ("b", "b1.wpilog")):
        payload = name.encode()
        (source / name).write_bytes(payload)
        entry = _entry(name, payload)
        lines += [
            "[[file]]",
            f'group = "{group}"',
            f'name = "{name}"',
            f'sha256 = "{entry["sha256"]}"',
            f"size = {entry['size']}",
            "",
        ]
    manifest = tmp_path / "manifest.toml"
    manifest.write_text("\n".join(lines), encoding="utf-8")
    return manifest, source


def test_group_fetches_only_that_group(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest, source = _two_group_manifest(tmp_path)
    monkeypatch.setattr(fetch_corpus, "MANIFEST_PATH", manifest)
    monkeypatch.setenv("FLASHPOINT_CORPUS_DIR", str(tmp_path / "corpus"))
    assert fetch_corpus.main(["--from", str(source), "--group", "a"]) == 0
    fetched = sorted(p.name for p in (tmp_path / "corpus" / "corpus-test").iterdir())
    assert fetched == ["a1.wpilog", "a2.hoot"]


def test_unknown_group_exits_non_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest, source = _two_group_manifest(tmp_path)
    monkeypatch.setattr(fetch_corpus, "MANIFEST_PATH", manifest)
    monkeypatch.setenv("FLASHPOINT_CORPUS_DIR", str(tmp_path / "corpus"))
    assert fetch_corpus.main(["--from", str(source), "--group", "nope"]) != 0
    assert "nope" in capsys.readouterr().err
