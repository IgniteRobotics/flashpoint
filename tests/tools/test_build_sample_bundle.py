"""The "Try it with sample logs" bundle (mvp-documentation design D-i; spec project-documentation,
"Quickstart with sample logs")."""

import hashlib
import importlib.util
import zipfile
from pathlib import Path
from types import ModuleType

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "build-sample-bundle.py"


def _tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("build_sample_bundle", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _logs(tmp_path: Path) -> list[Path]:
    folder = tmp_path / "corpus"
    folder.mkdir()
    files = []
    for name, body in [
        ("FRC_20260411_192626_GACMP_E10.wpilog", b"wpilog bytes"),
        ("GACMP_E10_rio_2026-04-11_19-26-19.hoot", b"hoot one"),
        ("GACMP_E10_rio_2026-04-11_19-29-15.hoot", b"hoot two"),
    ]:
        (folder / name).write_bytes(body)
        files.append(folder / name)
    return files


def test_bundle_layout_and_sums(tmp_path: Path) -> None:
    out = tmp_path / "flashpoint-sample-1.zip"
    _tool().build(_logs(tmp_path), REPO / "config" / "robots" / "2026-comp.toml", out)
    with zipfile.ZipFile(out) as bundle:
        names = sorted(bundle.namelist())
        assert names == [
            "README.txt",
            "SHA256SUMS",
            "config/robots/2026-comp.toml",
            "logs/FRC_20260411_192626_GACMP_E10.wpilog",
            "logs/GACMP_E10_rio_2026-04-11_19-26-19.hoot",
            "logs/GACMP_E10_rio_2026-04-11_19-29-15.hoot",
        ]
        sums: dict[str, str] = {}
        for line in bundle.read("SHA256SUMS").decode().splitlines():
            digest, name = line.split("  ", 1)
            sums[name] = digest
        assert set(sums) == set(names) - {"SHA256SUMS"}
        for name, digest in sums.items():
            assert hashlib.sha256(bundle.read(name)).hexdigest() == digest, name
        assert "flashpoint" in bundle.read("README.txt").decode().lower()


def test_bundle_is_reproducible(tmp_path: Path) -> None:
    tool = _tool()
    robot = REPO / "config" / "robots" / "2026-comp.toml"
    logs = _logs(tmp_path)
    tool.build(logs, robot, tmp_path / "a.zip")
    tool.build(logs, robot, tmp_path / "b.zip")
    assert (tmp_path / "a.zip").read_bytes() == (tmp_path / "b.zip").read_bytes()


def test_a_truncated_file_never_goes_in(tmp_path: Path) -> None:
    logs = _logs(tmp_path)
    cut = logs[0].parent / "GACMP_E10_rio_2026-04-11_19-26-19.truncated-60pct.hoot"
    cut.write_bytes(b"cut short")
    with pytest.raises(ValueError, match="truncated"):
        _tool().build(
            [*logs, cut], REPO / "config" / "robots" / "2026-comp.toml", tmp_path / "x.zip"
        )


def test_corpus_selection_is_the_e10_match_without_the_truncated_copy() -> None:
    names = [entry["name"] for entry in _tool().sample_entries()]
    assert len(names) == 5
    assert sum(n.endswith(".wpilog") for n in names) == 1
    assert not [n for n in names if "truncated" in n]
    assert all("E10" in n for n in names)
