"""An AdvantageKit wpilog with corpus Q7's hoots, end to end (match-identity, log-metadata)."""

import json
from collections.abc import Callable
from datetime import UTC
from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.ingest import Ingestor
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.meta import extract
from flashpoint.readers import hoot
from flashpoint.report.build import ReportBuilder
from flashpoint.report.paths import data_dir
from flashpoint.semantics.derive import Deriver
from tests.wpilog_builder import WpilogBuilder

pytestmark = pytest.mark.corpus


def _akit_like_q7(q7_wpilog: Path, folder: Path) -> Path:
    """No systemTime and no FMSInfo: /SystemStats/EpochTimeMicros and /DriverStation instead,
    covering the wall-clock time of corpus Q7's own wpilog."""
    meta = extract.read_metadata(q7_wpilog, q7_wpilog.name)
    assert meta.utc_start is not None and meta.utc_end is not None
    start_us = int(meta.utc_start.timestamp() * 1_000_000)
    seconds = int((meta.utc_end - meta.utc_start).total_seconds()) + 1
    b = WpilogBuilder().start(1, "/SystemStats/EpochTimeMicros", "int64")
    b.start(2, "/DriverStation/EventName", "string").start(3, "/DriverStation/MatchType", "int64")
    b.start(4, "/DriverStation/MatchNumber", "int64").start(
        5, "/DriverStation/ReplayNumber", "int64"
    )
    b.string(2, 0, "").int64(3, 0, 0).int64(4, 0, 0)  # before the FMS connects
    b.string(2, 5_000_000, "GACMP").int64(3, 5_000_000, 2).int64(4, 5_000_000, 7)
    b.int64(5, 5_000_000, 1)
    for i in range(seconds):
        b.int64(1, i * 1_000_000, start_us + i * 1_000_000)
    stamp = meta.utc_start.astimezone(UTC).strftime("%y-%m-%d_%H-%M-%S")
    return b.write(folder / f"akit_{stamp}_gacmp_q7.wpilog")


def test_advantagekit_wpilog_with_corpus_hoots(
    corpus_group: Callable[[str], list[Path]], tmp_path: Path
) -> None:
    files = corpus_group("2026-gacmp-q7")
    q7_wpilog = next(p for p in files if p.suffix == ".wpilog")
    (tmp_path / "in").mkdir()
    akit = _akit_like_q7(q7_wpilog, tmp_path / "in")
    lake = LakePaths(tmp_path / "lake")
    ingestor = Ingestor(lake, hoot.default_registry(config.cache_root()))
    try:
        ingestor.ingest([akit, *(p for p in files if p.suffix == ".hoot")])
    finally:
        ingestor.close()

    deriver = Deriver(lake, config.config_root())
    try:
        deriver.run()
        (session,) = deriver.ledger.query("SELECT * FROM sessions")
        hoots = deriver.ledger.query("SELECT * FROM session_hoots")
        framing = deriver.framing_for(session["session_id"])
    finally:
        deriver.close()
    assert (session["match_key"], session["match_source"]) == ("2026gacmp_qm7", "fms")
    assert len(hoots) == 2
    assert {h["method"] for h in hoots} == {"wall-clock"}
    assert framing.source == "hoot-robot-mode"
    ledger = Ledger(lake.ledger)
    try:
        (log,) = ledger.query("SELECT * FROM logs WHERE kind = 'wpilog'")
    finally:
        ledger.close()
    assert (log["anchor_source"], log["fms_origin"]) == ("advantagekit-epoch", "driver-station")

    builder = ReportBuilder(lake, config.config_root())
    try:
        builder.build(force=True)
    finally:
        builder.close()
    text = (data_dir(lake) / "matches.js").read_text(encoding="utf-8")
    index = json.loads(text[text.index("(") + 1 : -3])
    entry = next(m for m in index["matches"] if m["key"] == "2026gacmp_qm7")
    assert entry["low_alignment"] is True
    assert entry["alignment"] == "very-low"
