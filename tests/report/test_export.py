import os
from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.report.build import ReportBuilder
from flashpoint.report.export import MARKER, ExportError, export_static
from tests.report.lake import MatchLake, quiet_rows


@pytest.fixture
def built(tmp_path: Path) -> MatchLake:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows())
    lake.add_match("2026gacmp_e10", "hoot-e10", None, with_wpilog=False, phases=[])
    builder = ReportBuilder(lake.write_meta(), config.config_root())
    builder.build()
    builder.close()
    return lake


def test_layout_and_history_absent(built: MatchLake, tmp_path: Path) -> None:
    out = tmp_path / "usb" / "flashpoint"
    result = export_static(built.lake, out)
    assert result.matches == 2
    assert (out / MARKER).is_file()
    index = (out / "index.html").read_text()
    assert "views/replay.js" in index and "history.js" not in index
    assert not (out / "static/views/history.js").exists()
    assert (out / "static/views/replay.js").is_file() and (out / "static/shell.js").is_file()
    assert (out / "static/vendor/uplot/uPlot.iife.min.js").is_file()
    assert '"static": true' in (out / "static/mode.js").read_text()
    assert (out / "data/matches.js").is_file() and (out / "data/2026gacmp_qm7.js").is_file()
    assert not (out / "data/site-state.json").exists()


def test_raw_files_linked_with_match_names(built: MatchLake, tmp_path: Path) -> None:
    out = tmp_path / "export"
    result = export_static(built.lake, out)
    names = sorted(p.name for p in (out / "raw").iterdir())
    assert len(names) == result.raw_files == 5  # Q7 wpilog + 2 hoots, E10's 2 hoots
    assert all(n.startswith(("2026gacmp_qm7__", "2026gacmp_e10__")) for n in names)
    wpilog = out / "raw" / "2026gacmp_qm7__wpilog__FRC_q7.wpilog"
    assert wpilog.read_bytes() == b"wpilog q7"
    assert result.raw_linked == 5 and wpilog.stat().st_nlink >= 2  # same filesystem


def test_copy_when_link_fails(
    built: MatchLake, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*_: object) -> None:
        raise OSError(18, "Invalid cross-device link")

    monkeypatch.setattr(os, "link", refuse)
    result = export_static(built.lake, tmp_path / "export")
    assert result.raw_files == 5 and result.raw_linked == 0
    assert (
        tmp_path / "export/raw/2026gacmp_qm7__wpilog__FRC_q7.wpilog"
    ).read_bytes() == b"wpilog q7"


def test_no_raw(built: MatchLake, tmp_path: Path) -> None:
    out = tmp_path / "export"
    result = export_static(built.lake, out, include_raw=False)
    assert result.raw_files == 0 and not (out / "raw").exists()
    assert '"raw": false' in (out / "static/mode.js").read_text()


def test_refuses_foreign_folder_but_replaces_own(built: MatchLake, tmp_path: Path) -> None:
    foreign = tmp_path / "docs"
    foreign.mkdir()
    (foreign / "thesis.docx").write_text("x")
    with pytest.raises(ExportError, match="not empty"):
        export_static(built.lake, foreign)
    assert (foreign / "thesis.docx").exists()
    out = tmp_path / "export"
    export_static(built.lake, out)
    (out / "data" / "stale.js").write_text("old")
    export_static(built.lake, out, include_raw=False)
    assert not (out / "data" / "stale.js").exists() and not (out / "raw").exists()


def test_nothing_built(tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    with pytest.raises(ExportError, match="flashpoint report"):
        export_static(lake.lake, tmp_path / "out")
