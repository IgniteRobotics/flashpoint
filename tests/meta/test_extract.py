import json
from datetime import UTC, datetime, timedelta
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


# --- AdvantageKit (mvp-outside-team-logs) -----------------------------------------------------


def _akit_ds_log(
    tmp_path: Path,
    name: str = "akit_26-04-09_21-51-13.wpilog",
    match_type: int | str = "Qualification",
    match_number: int | str = 7,
    event: str | None = "GACMP",
) -> Path:
    b = WpilogBuilder()
    if event is not None:
        b.start(1, "/DriverStation/EventName", "string").string(1, 20, event)
    if isinstance(match_type, str):
        b.start(2, "/DriverStation/MatchType", "string").string(2, 20, match_type)
    else:
        b.start(2, "/DriverStation/MatchType", "int64").int64(2, 20, match_type)
    if isinstance(match_number, str):
        b.start(3, "/DriverStation/MatchNumber", "string").string(3, 20, match_number)
    else:
        b.start(3, "/DriverStation/MatchNumber", "int64").int64(3, 20, match_number)
    b.start(4, "/DriverStation/ReplayNumber", "int64").int64(4, 20, 1)
    b.start(5, "/DriverStation/AllianceStation", "int64").int64(5, 20, 5)  # Blue 2
    return b.write(tmp_path / name)


def test_advantagekit_driver_station_entries(tmp_path: Path) -> None:
    path = _akit_ds_log(tmp_path)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert (meta.fms_event, meta.fms_match_type, meta.fms_match_number) == ("GACMP", 2, 7)
    assert meta.fms_replay == 1
    assert (meta.fms_red_alliance, meta.fms_station) == (False, 2)
    assert meta.fms_origin == "driver-station"


def test_advantagekit_match_type_as_number(tmp_path: Path) -> None:
    path = _akit_ds_log(tmp_path, match_type=3)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)
    assert meta.fms_match_type == 3


@pytest.mark.parametrize("match_type", ["Scrimmage", 0, 7])
def test_unknown_match_type_is_absent(tmp_path: Path, match_type: int | str) -> None:
    path = _akit_ds_log(tmp_path, match_type=match_type)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)
    assert meta.fms_match_type is None
    assert (meta.fms_event, meta.fms_match_number) == ("GACMP", 7)


def test_qualification_match_origin_is_fms_info(tmp_path: Path) -> None:
    path = _fms_log(tmp_path)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)
    assert meta.fms_origin == "fms-info"


def test_no_fms_data_has_no_origin(tmp_path: Path) -> None:
    path = WpilogBuilder().start(1, "x", "double").double(1, 5, 1.0).write(tmp_path / "n.wpilog")
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)
    assert meta.fms_origin is None


def test_both_present_takes_fms_info_as_a_whole(tmp_path: Path) -> None:
    b = WpilogBuilder()
    b.start(1, "NT:/FMSInfo/EventName", "string").start(2, "NT:/FMSInfo/MatchNumber", "int64")
    b.start(3, "NT:/FMSInfo/MatchType", "int64")
    b.string(1, 20, "GACMP").int64(2, 20, 7).int64(3, 20, 2)
    b.start(4, "/DriverStation/EventName", "string").string(4, 20, "GACMP")
    b.start(5, "/DriverStation/MatchNumber", "int64").int64(5, 20, 8)
    b.start(6, "/DriverStation/MatchType", "int64").int64(6, 20, 2)
    b.start(7, "/DriverStation/ReplayNumber", "int64").int64(7, 20, 2)
    path = b.write(tmp_path / "both.wpilog")
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert (meta.fms_event, meta.fms_match_type, meta.fms_match_number) == ("GACMP", 2, 7)
    assert meta.fms_replay is None  # not mixed in from /DriverStation
    assert meta.fms_origin == "fms-info"


def test_wrong_typed_driver_station_entry(tmp_path: Path) -> None:
    path = _akit_ds_log(tmp_path, match_number="7")
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert meta.fms_match_number is None
    assert (meta.fms_event, meta.fms_match_type) == ("GACMP", 2)
    assert meta.fms_origin == "driver-station"


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (
            "akit_26-04-09_21-51-13_GACMP_q7.wpilog",
            ("GACMP", "Q", 7, datetime(2026, 4, 9, 21, 51, 13, tzinfo=UTC)),
        ),
        (
            "akit_26-04-11_19-26-26_gacmp_e10.wpilog",
            ("GACMP", "E", 10, datetime(2026, 4, 11, 19, 26, 26, tzinfo=UTC)),
        ),
        (
            "akit_26-04-09_21-51-13_p2.wpilog",
            (None, "P", 2, datetime(2026, 4, 9, 21, 51, 13, tzinfo=UTC)),
        ),
        (
            "akit_26-04-09_21-51-13_gacmp.wpilog",
            ("GACMP", None, None, datetime(2026, 4, 9, 21, 51, 13, tzinfo=UTC)),
        ),
        (
            "akit_26-04-09_21-51-13.wpilog",
            (None, None, None, datetime(2026, 4, 9, 21, 51, 13, tzinfo=UTC)),
        ),
        ("akit_1a2b3c4d5e6f7a8b_gacmp_q7.wpilog", ("GACMP", "Q", 7, None)),
    ],
)
def test_parse_advantagekit_filename(name: str, expected: tuple[object, ...]) -> None:
    parsed = extract.parse_wpilog_filename(name)
    assert (parsed.event, parsed.match_type, parsed.match_number, parsed.utc) == expected


def _akit_epoch_log(tmp_path: Path, epoch: bool = True, early_1970: bool = False) -> Path:
    """An AdvantageKit-style wpilog: no systemTime, /SystemStats/EpochTimeMicros each loop."""
    b = WpilogBuilder().start(1, "/SystemStats/EpochTimeMicros", "int64")
    b.start(2, "/RealOutputs/x", "double")
    for i in range(10):
        ts = 1_000_000 + i * 20_000
        if epoch:
            value = 5_000_000 + ts if early_1970 and i < 4 else EPOCH_2026 + ts
            b.int64(1, ts, value)
        b.double(2, ts, float(i))
    b.double(2, 120_000_000, 1.0)  # log runs two minutes
    return b.write(tmp_path / "akit_26-04-09_21-51-13.wpilog")


def test_anchor_from_advantagekit_epoch_entry(tmp_path: Path) -> None:
    path = _akit_epoch_log(tmp_path)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert meta.anchor_source == "advantagekit-epoch"
    assert meta.utc_offset_us == EPOCH_2026
    assert meta.utc_start is not None and meta.utc_end is not None
    assert meta.utc_end - meta.utc_start > timedelta(minutes=1)


def test_epoch_entry_before_the_clock_is_set(tmp_path: Path) -> None:
    path = _akit_epoch_log(tmp_path, early_1970=True)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert meta.anchor_source == "advantagekit-epoch"
    assert meta.utc_offset_us == EPOCH_2026


def test_no_anchor_entry_falls_back_to_the_akit_filename(tmp_path: Path) -> None:
    path = _akit_epoch_log(tmp_path, epoch=False)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert meta.anchor_source == "filename"
    assert meta.utc_end is None
    assert meta.season == "2026"


def test_system_time_wins_over_the_epoch_entry(tmp_path: Path) -> None:
    b = (
        WpilogBuilder()
        .start(1, "systemTime", "int64")
        .start(2, "/SystemStats/EpochTimeMicros", "int64")
    )
    b.int64(1, 1_000_000, EPOCH_2026 + 1_000_000).int64(2, 1_000_000, EPOCH_2026 + 61_000_000)
    path = b.write(tmp_path / "both.wpilog")
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)
    assert (meta.anchor_source, meta.utc_offset_us) == ("systemTime", EPOCH_2026)
