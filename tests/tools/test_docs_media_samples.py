"""The extra sample matches `tools/docs-media.py` adds to the sample lake: GACMP Q73 and Q89 from
the samples-v1 release, fetched, checked against their SHA-256 and SHA256SUMS, and unpacked."""

import hashlib
import importlib.util
import zipfile
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parents[2]


def _tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("docs_media", REPO / "tools" / "docs-media.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sample_zip(path: Path, files: dict[str, bytes], sums: dict[str, bytes] | None = None) -> Path:
    """A sample zip like the release's: the files, and SHA256SUMS over `sums` (default: files)."""
    listed = files if sums is None else sums
    lines = "".join(
        f"{hashlib.sha256(data).hexdigest()}  {name}\n" for name, data in listed.items()
    )
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
        archive.writestr("SHA256SUMS", lines)
    return path


def test_extra_samples_are_q73_and_q89_and_in_the_demo() -> None:
    tool = _tool()
    assert tool.EXTRA_SAMPLES == {
        "flashpoint-sample-2026gacmp_qm73.zip": (
            "f8c8e6989620a5f11000a6c8db646edd18caf195cfa1ccbad043af425c4c33a1"
        ),
        "flashpoint-sample-2026gacmp_qm89.zip": (
            "ff0502ca470a3012193b9fc150a54eb3fe48e3a0bc9f440489f6a2918e547e3d"
        ),
    }
    assert {"2026gacmp_qm73", "2026gacmp_qm89"} <= set(tool.DEMO_MATCHES)


def test_unpack_returns_the_logs_checked_against_sha256sums(tmp_path: Path) -> None:
    tool = _tool()
    archive = _sample_zip(
        tmp_path / "s.zip", {"logs/a.wpilog": b"wpilog", "logs/b_rio.hoot": b"hoot"}
    )
    logs = tool.unpack_sample(archive, tmp_path / "s")
    assert [p.relative_to(tmp_path / "s").as_posix() for p in logs] == [
        "logs/a.wpilog",
        "logs/b_rio.hoot",
    ]


def test_unpack_refuses_a_file_that_doesnt_match_sha256sums(tmp_path: Path) -> None:
    tool = _tool()
    archive = _sample_zip(
        tmp_path / "s.zip", {"logs/a.wpilog": b"damaged"}, sums={"logs/a.wpilog": b"wpilog"}
    )
    with pytest.raises(SystemExit, match=r"s\.zip: logs/a\.wpilog doesn't match its SHA256SUMS"):
        tool.unpack_sample(archive, tmp_path / "s")


def test_unpack_refuses_a_path_outside_its_folder(tmp_path: Path) -> None:
    tool = _tool()
    archive = _sample_zip(tmp_path / "s.zip", {"../escape.wpilog": b"x"})
    with pytest.raises(SystemExit, match=r"s\.zip: \.\./escape\.wpilog is outside logs/"):
        tool.unpack_sample(archive, tmp_path / "s")
    assert not (tmp_path / "escape.wpilog").exists()


def test_fetch_downloads_a_missing_sample_and_refuses_a_bad_checksum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tool = _tool()
    fetched: list[str] = []

    def fake_download(name: str, release_url: str, out: Path) -> None:
        fetched.append(name)
        out.write_bytes(b"not the release's zip")

    fake_fetch = SimpleNamespace(download=fake_download, sha256_of=tool._fetch_corpus().sha256_of)
    monkeypatch.setattr(tool, "_fetch_corpus", lambda: fake_fetch)
    with pytest.raises(SystemExit, match=r"flashpoint-sample-2026gacmp_qm73\.zip: SHA-256"):
        tool.fetch_extra_samples(tmp_path)
    assert fetched == ["flashpoint-sample-2026gacmp_qm73.zip"]
    assert not (tmp_path / "flashpoint-sample-2026gacmp_qm73.zip").exists()
