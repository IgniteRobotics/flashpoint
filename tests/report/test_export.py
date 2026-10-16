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


def _anomaly(anomaly_id: str, keys: list[str], **extra: object) -> dict[str, object]:
    import json

    from flashpoint.anomaly import detect

    row: dict[str, object] = dict.fromkeys(detect.SCHEMA)
    row.update({
        "id": anomaly_id, "subject": "power", "subject_key": "power:2026-comp",
        "subject_kind": "power", "robot": "2026-comp", "slot_id": "power", "method": "SPEC",
        "metric": "brownouts", "severity": "HIGH", "sensitivity": "relaxed",
        "title": "6 brownouts in Q7 </script>", "onset_match_key": keys[0],
        "last_match_key": keys[-1], "flagged_keys": json.dumps(keys), "evidence": "{}",
        **extra,
    })  # fmt: skip
    return row


def test_anomaly_snapshot(built: MatchLake, tmp_path: Path) -> None:
    import json

    import polars as pl

    from flashpoint.anomaly import detect
    from flashpoint.views.anomalies import AnomalyQueries

    rows = [
        _anomaly("a" * 16, ["2026gacmp_qm6", "2026gacmp_qm7"]),
        _anomaly("b" * 16, ["2026gacmp_qm6"]),  # no exported match
        _anomaly("c" * 16, ["2026gacmp_qm7"]),  # resolved below
    ]
    path = built.root / "gold" / "anomalies" / "anomalies.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(rows, schema=detect.SCHEMA).write_parquet(path)
    queries = AnomalyQueries(built.lake, config.config_root())
    queries.triage("c" * 16, {"to": "investigating"})
    queries.triage("c" * 16, {"to": "resolved", "outcome": "battery swapped"})

    out = tmp_path / "export"
    export_static(built.lake, out)
    text = (out / "data" / "anomalies-snapshot.js").read_text()
    assert text.startswith("FP.anomalySnapshot = ") and text.endswith(";\n")
    assert "</script>" not in text  # escaped like the other data files
    snapshot = json.loads(text.removeprefix("FP.anomalySnapshot = ").removesuffix(";\n"))
    assert [i["id"] for i in snapshot["items"]] == ["a" * 16]
    assert snapshot["items"][0]["flagged_keys"] == ["2026gacmp_qm7"]
    assert snapshot["taken_at"].endswith("+00:00")
    assert "http" not in text


def test_anomaly_snapshot_without_detection(built: MatchLake, tmp_path: Path) -> None:
    out = tmp_path / "export"
    export_static(built.lake, out)
    assert '"items": []' in (out / "data" / "anomalies-snapshot.js").read_text()
