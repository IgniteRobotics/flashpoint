"""Older lakes get AdvantageKit metadata on the next derive (log-metadata "Wall-clock anchor")."""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

from flashpoint.ingest import Ingestor
from flashpoint.lake import bronze
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.raw import raw_path
from flashpoint.readers.hoot import OwletRegistry
from flashpoint.semantics.derive import Deriver
from tests.wpilog_builder import WpilogBuilder

START = datetime(2026, 4, 9, 21, 51, 13, tzinfo=UTC)
EPOCH_US = int(START.timestamp() * 1_000_000)
HOOT_STAMP = "2026-04-09_21-51-20"
AKIT_NAME = "akit_26-04-09_21-51-13.wpilog"
FRC_NAME = "FRC_20260409_215113_GACMP_Q7.wpilog"


def _akit(tmp_path: Path) -> Path:
    b = WpilogBuilder().start(1, "/SystemStats/EpochTimeMicros", "int64")
    for i in range(150):
        b.int64(1, i * 1_000_000, EPOCH_US + i * 1_000_000)
    return b.write(tmp_path / "in" / AKIT_NAME)


def _frc(tmp_path: Path) -> Path:
    b = WpilogBuilder().start(1, "NT:/FMSInfo/EventName", "string")
    b.start(2, "NT:/FMSInfo/MatchNumber", "int64").start(3, "NT:/FMSInfo/MatchType", "int64")
    b.start(4, "systemTime", "int64")
    b.string(1, 10, "GACMP").int64(2, 10, 7).int64(3, 10, 2)
    later = EPOCH_US + 3_600_000_000  # an hour after the AdvantageKit log
    for i in range(10):
        b.int64(4, i * 1_000_000, later + i * 1_000_000)
    return b.write(tmp_path / "in" / FRC_NAME)


def _hoot_bronze(lake: LakePaths, log_id: str) -> None:
    table = pa.table(
        {
            "signal": pa.array(["Phoenix6/TalonFX-1/SupplyVoltage"]).dictionary_encode(),
            "type": pa.array(["double"]).dictionary_encode(),
            "ts_us": pa.array([1_000_000], pa.int64()),
            "v_f64": pa.array([12.0], pa.float64()),
            "v_i64": pa.nulls(1, pa.int64()),
            "v_bool": pa.nulls(1, pa.bool_()),
            "v_str": pa.nulls(1, pa.large_string()),
            "v_bytes": pa.nulls(1, pa.large_binary()),
        }
    )
    bronze.write(lake, table, season="2026", log_id=log_id, run_id="t")


def _older_lake(tmp_path: Path) -> tuple[LakePaths, dict[str, str]]:
    """Ingest both wpilogs, then make their ledger rows look like an earlier version's."""
    (tmp_path / "in").mkdir()
    lake = LakePaths(tmp_path / "lake")
    manifest = tmp_path / "empty-manifest.toml"
    manifest.write_text("")
    ingestor = Ingestor(lake, OwletRegistry(manifest, tmp_path / "owlet-cache"))
    try:
        ingestor.ingest([_akit(tmp_path), _frc(tmp_path)])
    finally:
        ingestor.close()
    ledger = Ledger(lake.ledger)
    ids = {r["filename"]: r["log_id"] for r in ledger.query("SELECT log_id, filename FROM logs")}
    ledger.execute("UPDATE logs SET meta_version = NULL, fms_origin = NULL")
    ledger.execute(
        "UPDATE logs SET anchor_source = 'filename', utc_offset_us = NULL, utc_end = NULL,"
        " utc_start = ? WHERE log_id = ?",
        (START.isoformat(), ids[AKIT_NAME]),
    )
    for n, bus in enumerate(("rio", "6E9415C3394C485320202050101C18FF")):
        log_id = f"hoot{n}"
        _hoot_bronze(lake, log_id)
        ledger.execute(
            "INSERT INTO files (sha256, kind, size, stage, first_seen, updated)"
            " VALUES (?, 'hoot', 1, 'success', 't', 't')",
            (log_id,),
        )
        ledger.execute(
            "INSERT INTO logs (log_id, kind, season, filename, first_ts_us, last_ts_us)"
            " VALUES (?, 'hoot', '2026', ?, 0, 100000000)",
            (log_id, f"{bus}_{HOOT_STAMP}.hoot"),
        )
        ledger.execute(
            "INSERT INTO hoot_logs (log_id, bus, session_stamp, pro_licensed) VALUES (?, ?, ?, 1)",
            (log_id, bus, HOOT_STAMP),
        )
    ledger.close()
    return lake, ids


def _derive(lake: LakePaths, tmp_path: Path) -> Deriver:
    config_root = tmp_path / "config"
    (config_root / "robots").mkdir(parents=True, exist_ok=True)
    (config_root / "seasons").mkdir(exist_ok=True)
    deriver = Deriver(lake, config_root)
    try:
        deriver.run()
    finally:
        deriver.close()
    return deriver


def _row(lake: LakePaths, log_id: str) -> dict[str, object]:
    ledger = Ledger(lake.ledger)
    try:
        return ledger.query("SELECT * FROM logs WHERE log_id = ?", (log_id,))[0]
    finally:
        ledger.close()


def test_lake_ingested_by_an_earlier_version(tmp_path: Path) -> None:
    lake, ids = _older_lake(tmp_path)
    deriver = _derive(lake, tmp_path)

    akit = _row(lake, ids[AKIT_NAME])
    assert akit["anchor_source"] == "advantagekit-epoch"
    assert akit["utc_end"] is not None
    assert deriver.reread == [ids[AKIT_NAME]]  # the FRC log has a match number and a UTC end
    assert deriver.warnings == []
    ledger = Ledger(lake.ledger)
    try:
        grouped = ledger.query("SELECT log_id, session_id FROM session_hoots ORDER BY log_id")
    finally:
        ledger.close()
    assert grouped == [
        {"log_id": "hoot0", "session_id": ids[AKIT_NAME]},
        {"log_id": "hoot1", "session_id": ids[AKIT_NAME]},
    ]
    assert _derive(lake, tmp_path).reread == []  # once only


def test_raw_file_gone_before_the_re_read(tmp_path: Path) -> None:
    lake, ids = _older_lake(tmp_path)
    raw_path(lake, ids[AKIT_NAME], ".wpilog").unlink()

    first = _derive(lake, tmp_path)
    assert len(first.warnings) == 1
    assert AKIT_NAME in first.warnings[0]
    assert _row(lake, ids[AKIT_NAME])["anchor_source"] == "filename"
    assert _derive(lake, tmp_path).warnings == []
