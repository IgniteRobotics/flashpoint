import sqlite3
from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.ingest import Ingestor
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.readers.hoot import OwletRegistry
from flashpoint.semantics.derive import Deriver
from tests.wpilog_builder import WpilogBuilder


def _triage(ledger: Ledger, anomaly_id: str, to: str, **extra: object) -> int:
    row = {
        "anomaly_id": anomaly_id,
        "from_state": extra.pop("from_state", "new"),
        "to_state": to,
        "outcome": extra.pop("outcome", None),
        "restart_baseline": extra.pop("restart_baseline", False),
        "subject": "ctre:DRIVE-FL",
        "metric": "stator_current_p95",
        "onset_match_key": "2026gacmp_qm7",
    }
    return ledger.add_triage(row)


def test_migration_adds_table_to_an_existing_ledger(tmp_path: Path) -> None:
    path = tmp_path / "meta" / "flashpoint.sqlite"
    path.parent.mkdir()
    old = sqlite3.connect(path)
    old.execute("CREATE TABLE files (sha256 TEXT PRIMARY KEY)")
    old.commit()
    old.close()

    ledger = Ledger(path)
    try:
        assert _triage(ledger, "a" * 16, "investigating") == 1
        assert ledger.triage()[0]["to_state"] == "investigating"
    finally:
        ledger.close()


def test_append_only_current_state_is_the_last_row(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path / "flashpoint.sqlite")
    try:
        _triage(ledger, "a" * 16, "investigating")
        _triage(ledger, "b" * 16, "dismissed", outcome="expected", restart_baseline=True)
        _triage(ledger, "a" * 16, "resolved", from_state="investigating", outcome="bearing")
        history = ledger.triage("a" * 16)
        assert [r["to_state"] for r in history] == ["investigating", "resolved"]
        assert [r["seq"] for r in history] == sorted(r["seq"] for r in history)
        current = ledger.triage_states()
        assert current["a" * 16]["to_state"] == "resolved"
        assert current["a" * 16]["outcome"] == "bearing"
        assert current["b" * 16]["restart_baseline"] == 1
        assert history[0]["at"].endswith("+00:00")
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            ledger.execute("UPDATE anomaly_triage SET outcome = 'x'")
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            ledger.execute("DELETE FROM anomaly_triage")
        assert len(ledger.triage()) == 3
    finally:
        ledger.close()


def test_survives_rebuild_pipeline_version_and_derive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lake = LakePaths(tmp_path / "lake")
    manifest = tmp_path / "empty-manifest.toml"
    manifest.write_text("")
    registry = OwletRegistry(manifest, tmp_path / "owlet-cache")
    b = WpilogBuilder().start(1, "NT:/FMSInfo/EventName", "string").start(2, "x", "double")
    log = b.string(1, 10, "GACMP").double(2, 20, 1.0).write(tmp_path / "FRC_20260409_215113.wpilog")
    ingestor = Ingestor(lake, registry)
    try:
        ingestor.ingest([log])
        _triage(ingestor.ledger, "a" * 16, "investigating")
    finally:
        ingestor.close()

    monkeypatch.setattr(config, "PIPELINE_VERSION", config.PIPELINE_VERSION + 1)
    ingestor = Ingestor(lake, registry)
    try:
        assert ingestor.rebuild().count("success") == 1
    finally:
        ingestor.close()
    deriver = Deriver(lake, config.config_root())
    try:
        deriver.run(force=True)
    finally:
        deriver.close()

    ledger = Ledger(lake.ledger)
    try:
        assert [r["anomaly_id"] for r in ledger.triage()] == ["a" * 16]
    finally:
        ledger.close()
    assert (lake.meta / "anomaly_triage.parquet").is_file()
