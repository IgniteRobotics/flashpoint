import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from flashpoint.meta import extract
from flashpoint.readers.wpilog import read_wpilog
from tests.wpilog_builder import WpilogBuilder

EPOCH_2026 = int(datetime(2026, 4, 9, 21, 51, 13, tzinfo=UTC).timestamp() * 1_000_000)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("FRC_20260409_215113_GACMP_Q7.wpilog", ("GACMP", "Q", 7)),
        ("FRC_20260411_192626_GACMP_E10.wpilog", ("GACMP", "E", 10)),
        ("FRC_20250307_213719_GADAL1_P2.wpilog", ("GADAL1", "P", 2)),
        ("FRC_20250404_221544.wpilog", (None, None, None)),
        ("Test1.wpilog", (None, None, None)),
    ],
)
def test_parse_wpilog_filename(name: str, expected: tuple[object, ...]) -> None:
    parsed = extract.parse_wpilog_filename(name)
    assert (parsed.event, parsed.match_type, parsed.match_number) == expected


def test_parse_wpilog_filename_time_is_utc() -> None:
    parsed = extract.parse_wpilog_filename("FRC_20260409_215113_GACMP_Q7.wpilog")
    assert parsed.utc == datetime(2026, 4, 9, 21, 51, 13, tzinfo=UTC)


@pytest.mark.parametrize(
    ("name", "bus", "stamp", "event"),
    [
        ("GACMP_Q7_rio_2026-04-09_21-51-06.hoot", "rio", "2026-04-09_21-51-06", "GACMP"),
        (
            "GACMP_E10_6E9415C3394C485320202050101C18FF_2026-04-11_19-26-19.hoot",
            "6E9415C3394C485320202050101C18FF",
            "2026-04-11_19-26-19",
            "GACMP",
        ),
        ("rio_2025-04-04_17-45-23.hoot", "rio", "2025-04-04_17-45-23", None),
        (
            "GACMP_E10_rio_2026-04-11_19-26-19.truncated-60pct.hoot",
            "rio",
            "2026-04-11_19-26-19",
            "GACMP",
        ),
    ],
)
def test_parse_hoot_filename(name: str, bus: str, stamp: str, event: str | None) -> None:
    parsed = extract.parse_hoot_filename(name)
    assert (parsed.bus, parsed.session_stamp, parsed.event) == (bus, stamp, event)


def _fms_log(tmp_path: Path, match_number: int = 7, match_type: int = 2) -> Path:
    b = WpilogBuilder()
    b.start(1, "NT:/FMSInfo/EventName", "string").start(2, "NT:/FMSInfo/MatchNumber", "int64")
    b.start(3, "NT:/FMSInfo/MatchType", "int64").start(4, "NT:/FMSInfo/ReplayNumber", "int64")
    b.start(5, "NT:/FMSInfo/IsRedAlliance", "boolean").start(
        6, "NT:/FMSInfo/StationNumber", "int64"
    )
    b.start(7, "MetaData", "string").start(8, "systemTime", "int64")
    b.string(1, 10, "").string(1, 20, "GACMP").int64(2, 20, match_number).int64(3, 20, match_type)
    b.int64(4, 20, 1).boolean(5, 20, False).int64(6, 20, 2)
    b.string(7, 1, "Project Name: Robot-2026").string(7, 2, "Git Branch: hotfix/dcmp")
    b.string(7, 3, "Build Date: 2026-04-09 17:25:24 EDT").string(
        7, 4, "GitDirty: Uncomitted changes"
    )
    b.string(7, 5, "Note: has: colons")
    b.int64(8, 1_000_000, EPOCH_2026 + 1_000_000).int64(8, 6_000_000, EPOCH_2026 + 6_000_000)
    b.int64(8, 7_000_000, 0)  # pre-sync garbage is ignored
    return b.write(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog")


def test_wpilog_metadata_from_fms_and_build(tmp_path: Path) -> None:
    path = _fms_log(tmp_path)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert meta.fms_event == "GACMP"
    assert (meta.fms_match_type, meta.fms_match_number, meta.fms_replay) == (2, 7, 1)
    assert (meta.fms_red_alliance, meta.fms_station) == (False, 2)
    assert meta.project == "Robot-2026"
    assert meta.git_branch == "hotfix/dcmp"
    assert meta.git_dirty == "Uncomitted changes"
    assert meta.build["Note"] == "has: colons"


def test_absent_fms_values_are_none(tmp_path: Path) -> None:
    path = _fms_log(tmp_path, match_number=0, match_type=0)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)
    assert meta.fms_match_number is None
    assert meta.fms_match_type is None


def test_anchor_from_system_time(tmp_path: Path) -> None:
    path = _fms_log(tmp_path)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert meta.anchor_source == "systemTime"
    assert meta.utc_offset_us == EPOCH_2026
    assert meta.season == "2026"


def test_anchor_falls_back_to_filename(tmp_path: Path) -> None:
    b = WpilogBuilder().start(1, "x", "double").double(1, 5, 1.0)
    path = b.write(tmp_path / "FRC_20250404_221544.wpilog")
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert meta.anchor_source == "filename"
    assert meta.season == "2025"


def test_no_anchor_at_all(tmp_path: Path) -> None:
    path = (
        WpilogBuilder().start(1, "x", "double").double(1, 5, 1.0).write(tmp_path / "Test1.wpilog")
    )
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)
    assert (meta.anchor_source, meta.season) == ("none", "unknown")


def _inventory_log(tmp_path: Path, *payloads: str) -> Path:
    b = WpilogBuilder().start(1, "/Flashpoint/CANInventory", "string")
    for i, payload in enumerate(payloads):
        b.string(1, 100 + i, payload)
    return b.write(tmp_path / "inv.wpilog")


def test_inventory_absent(tmp_path: Path) -> None:
    path = WpilogBuilder().start(1, "x", "double").write(tmp_path / "n.wpilog")
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)
    assert meta.inventory_status == "absent"
    assert meta.inventory == []


def test_inventory_valid_error_and_malformed(tmp_path: Path) -> None:
    good = json.dumps(
        {
            "schema": 1,
            "source": "phoenix-diag",
            "devices": [{"model": "Talon FX", "bus": "rio", "id": 11, "serial": "ABC"}],
        }
    )
    error = json.dumps({"schema": 1, "error": "timed out"})
    path = _inventory_log(tmp_path, good, error, "not json")
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert [(r.valid, r.error) for r in meta.inventory] == [
        (True, None),
        (False, "timed out"),
        (False, "malformed: not JSON"),
    ]
    assert meta.inventory[0].payload == good
    assert meta.inventory_status == "present"
