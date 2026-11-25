"""Older lakes get AdvantageKit metadata on the next derive (log-metadata "Wall-clock anchor")."""

from datetime import UTC, datetime
from pathlib import Path

from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.raw import raw_path
from tests.synthetic_lake import add_hoot, derive, ingest_wpilogs, query
from tests.wpilog_builder import WpilogBuilder

START = datetime(2026, 4, 9, 21, 51, 13, tzinfo=UTC)
EPOCH_US = int(START.timestamp() * 1_000_000)
HOOT_STAMP = "2026-04-09_21-51-20"
AKIT_NAME = "akit_26-04-09_21-51-13.wpilog"
FRC_NAME = "FRC_20260409_215113_GACMP_Q7.wpilog"
CANIVORE = "6E9415C3394C485320202050101C18FF"


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


def _older_lake(tmp_path: Path) -> tuple[LakePaths, dict[str, str]]:
    """Ingest both wpilogs, then make their ledger rows look like an earlier version's."""
    (tmp_path / "in").mkdir()
    lake = ingest_wpilogs(tmp_path, [_akit(tmp_path), _frc(tmp_path)])
    ids = {
        str(r["filename"]): str(r["log_id"])
        for r in query(lake, "SELECT log_id, filename FROM logs")
    }
    ledger = Ledger(lake.ledger)
    try:
        ledger.execute("UPDATE logs SET meta_version = NULL, fms_origin = NULL")
        ledger.execute(
            "UPDATE logs SET anchor_source = 'filename', utc_offset_us = NULL, utc_end = NULL,"
            " utc_start = ? WHERE log_id = ?",
            (START.isoformat(), ids[AKIT_NAME]),
        )
    finally:
        ledger.close()
    add_hoot(lake, "hoot0", f"rio_{HOOT_STAMP}.hoot")
    add_hoot(lake, "hoot1", f"{CANIVORE}_{HOOT_STAMP}.hoot")
    return lake, ids


def _anchor(lake: LakePaths, log_id: str) -> object:
    return query(lake, "SELECT anchor_source FROM logs WHERE log_id = ?", (log_id,))[0][
        "anchor_source"
    ]


def test_lake_ingested_by_an_earlier_version(tmp_path: Path) -> None:
    lake, ids = _older_lake(tmp_path)
    deriver = derive(lake, tmp_path / "config")

    assert _anchor(lake, ids[AKIT_NAME]) == "advantagekit-epoch"
    row = query(lake, "SELECT utc_end FROM logs WHERE log_id = ?", (ids[AKIT_NAME],))[0]
    assert row["utc_end"] is not None
    assert deriver.reread == [ids[AKIT_NAME]]  # the FRC log has a match number and a UTC end
    assert deriver.warnings == []
    grouped = query(lake, "SELECT log_id, session_id FROM session_hoots ORDER BY log_id")
    assert grouped == [
        {"log_id": "hoot0", "session_id": ids[AKIT_NAME]},
        {"log_id": "hoot1", "session_id": ids[AKIT_NAME]},
    ]
    assert derive(lake, tmp_path / "config").reread == []  # once only


def test_raw_file_gone_before_the_re_read(tmp_path: Path) -> None:
    lake, ids = _older_lake(tmp_path)
    raw_path(lake, ids[AKIT_NAME], ".wpilog").unlink()

    first = derive(lake, tmp_path / "config")
    assert len(first.warnings) == 1
    assert AKIT_NAME in first.warnings[0]
    assert _anchor(lake, ids[AKIT_NAME]) == "filename"
    assert derive(lake, tmp_path / "config").warnings == []
