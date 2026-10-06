"""Staging a match's raw logs under readable names (spec: advantagescope-launch)."""

import hashlib
import os
import time
from pathlib import Path
from typing import Any

import pytest

from flashpoint.lake.ledger import INCOMPLETE_READ
from flashpoint.lake.raw import file_sha256
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.advantagescope import MAX_AGE_S, StagingError, cleanup, stage
from tests.report.lake import MatchLake

KEY = "2026johnson_qm15"


def _tree(path: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(path)): p.read_bytes() for p in sorted(path.rglob("*")) if p.is_file()
    }


@pytest.fixture
def lake(tmp_path: Path) -> MatchLake:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match(KEY, "q15", None, wpilog_name="FRC_20260430_151512_JOHNSON_Q15.wpilog")
    lake.write_meta()
    return lake


def _sources(lake: MatchLake, key: str = KEY) -> list[dict[str, Any]]:
    queries = HistoryQueries(lake.lake)
    try:
        return queries.sources(key)
    finally:
        queries.close()


def test_stage_q15(lake: MatchLake, tmp_path: Path) -> None:
    before = _tree(lake.lake.raw)
    root = tmp_path / "stage"
    staged = stage(lake.lake, KEY, _sources(lake), root)
    folder = root / KEY
    assert sorted(p.name for p in folder.iterdir()) == sorted(s.path.name for s in staged)
    assert len(staged) == 3 and all(s.path.parent == folder for s in staged)
    assert all(s.path.name.startswith(f"{KEY}__") for s in staged)
    assert staged[0].part == "wpilog"
    assert staged[0].path.name == f"{KEY}__wpilog__FRC_20260430_151512_JOHNSON_Q15.wpilog"
    for s in staged:
        assert file_sha256(s.path) == s.sha256
        assert s.linked == (os.name != "nt")  # hard links on one volume; Windows copies
    assert _tree(lake.lake.raw) == before


def test_repeat_reuses_the_folder(lake: MatchLake, tmp_path: Path) -> None:
    root = tmp_path / "stage"
    first = stage(lake.lake, KEY, _sources(lake), root)
    inode = first[0].path.stat().st_ino
    second = stage(lake.lake, KEY, _sources(lake), root)
    assert [s.path for s in second] == [s.path for s in first]
    assert second[0].path.stat().st_ino == inode  # kept, not re-linked or re-copied
    assert len(list((root / KEY).iterdir())) == 3


def test_mismatched_leftover_is_replaced(lake: MatchLake, tmp_path: Path) -> None:
    root = tmp_path / "stage"
    first = stage(lake.lake, KEY, _sources(lake), root)
    first[0].path.unlink()
    first[0].path.write_bytes(b"stale and longer than the real file")
    again = stage(lake.lake, KEY, _sources(lake), root)
    assert file_sha256(again[0].path) == again[0].sha256


def test_link_failure_falls_back_to_copy(lake: MatchLake, tmp_path: Path) -> None:
    def no_link(src: Path, dst: Path) -> None:
        raise OSError(18, "Invalid cross-device link")

    before = _tree(lake.lake.raw)
    staged = stage(lake.lake, KEY, _sources(lake), tmp_path / "stage", link=no_link)
    assert len(staged) == 3 and not any(s.linked for s in staged)
    for s in staged:
        assert file_sha256(s.path) == s.sha256
        assert not s.path.samefile(lake.lake.raw / s.sha256[:2] / f"{s.sha256}.{s.kind}")
    assert _tree(lake.lake.raw) == before


def test_missing_raw_file_stages_nothing(lake: MatchLake, tmp_path: Path) -> None:
    sources = _sources(lake)
    gone = sources[1]
    raw = lake.lake.raw / gone["sha256"][:2] / f"{gone['sha256']}.{gone['kind']}"
    raw.chmod(0o644)
    raw.unlink()
    root = tmp_path / "stage"
    with pytest.raises(StagingError) as error:
        stage(lake.lake, KEY, sources, root)
    assert error.value.missing == [gone["sha256"]]
    assert gone["sha256"] in str(error.value)
    assert not (root / KEY).exists()


def test_incomplete_hoot_is_staged_and_flagged(tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match(KEY, "q15", None)
    lake.tables["files"][-1]["stage"] = INCOMPLETE_READ  # the CANivore hoot
    lake.write_meta()
    staged = stage(lake.lake, KEY, _sources(lake), tmp_path / "stage")
    flagged = {s.part: s.incomplete for s in staged}
    assert flagged == {"wpilog": False, "rio": False, "6E9415C3394C485320202050101C18FF": True}


def test_hostile_match_key_never_leaves_the_root(lake: MatchLake, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="match key"):
        stage(lake.lake, "../../escape", _sources(lake), tmp_path / "stage")
    assert not (tmp_path / "escape").exists()


def _aged(path: Path, seconds: float) -> None:
    then = time.time() - seconds
    os.utime(path, (then, then), follow_symlinks=False)


NO_SYMLINKS = pytest.mark.skipif(os.name == "nt", reason="symlinks need developer mode on Windows")


@NO_SYMLINKS
def test_cleanup_removes_only_old_staged_folders(tmp_path: Path) -> None:
    root = tmp_path / "flashpoint-as"
    old = root / "2026gacmp_qm7"
    new = root / "2026gacmp_qm8"
    for folder in (old, new):
        folder.mkdir(parents=True)
        (folder / "x.wpilog").write_bytes(b"x")
    loose = root / "notes.txt"
    loose.write_text("not a staged folder")
    outside = tmp_path / "keep"
    outside.mkdir()
    (outside / "precious.wpilog").write_bytes(b"keep me")
    link = root / "2026gacmp_qm9"
    link.symlink_to(outside, target_is_directory=True)
    sibling = tmp_path / "other-app-temp"
    sibling.mkdir()
    for path in (old, loose, link, sibling, outside):
        _aged(path, MAX_AGE_S + 60)
    _aged(new, MAX_AGE_S - 60)
    removed = cleanup(root)
    assert removed == [old]
    assert not old.exists() and new.is_dir() and loose.is_file() and sibling.is_dir()
    assert link.is_symlink() and (outside / "precious.wpilog").read_bytes() == b"keep me"


def test_cleanup_tolerates_a_missing_root(tmp_path: Path) -> None:
    assert cleanup(tmp_path / "absent") == []


@NO_SYMLINKS
def test_cleanup_ignores_a_symlinked_root(tmp_path: Path) -> None:
    target = tmp_path / "target"
    (target / "2026gacmp_qm7").mkdir(parents=True)
    _aged(target / "2026gacmp_qm7", MAX_AGE_S + 60)
    root = tmp_path / "flashpoint-as"
    root.symlink_to(target, target_is_directory=True)
    assert cleanup(root) == []
    assert (target / "2026gacmp_qm7").is_dir()


@pytest.mark.corpus
def test_stage_corpus_q7(derived_corpus_lake: Any, tmp_path: Path) -> None:
    queries = HistoryQueries(derived_corpus_lake)
    try:
        sources = queries.sources("2026gacmp_qm7")
    finally:
        queries.close()
    before = {p: p.stat().st_mtime_ns for p in derived_corpus_lake.raw.rglob("*") if p.is_file()}
    staged = stage(derived_corpus_lake, "2026gacmp_qm7", sources, tmp_path / "stage")
    assert staged[0].part == "wpilog" and len(staged) == len(sources) >= 3
    for s in staged:
        assert s.path.name.startswith("2026gacmp_qm7__")
        assert hashlib.sha256(s.path.read_bytes()).hexdigest() == s.sha256
    after = {p: p.stat().st_mtime_ns for p in derived_corpus_lake.raw.rglob("*") if p.is_file()}
    assert after == before
