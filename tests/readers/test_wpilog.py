from pathlib import Path

import pytest

from flashpoint.readers.wpilog import WpilogError, read_wpilog
from tests.wpilog_builder import WpilogBuilder


def _sample_log() -> WpilogBuilder:
    b = WpilogBuilder(extra_header="hdr")
    b.start(1, "NT:/a/double", "double").start(2, "NT:/a/int", "int64")
    b.start(3, "NT:/a/bool", "boolean").start(4, "NT:/a/str", "string")
    b.start(5, "NT:/a/float", "float").start(6, "NT:/a/pose", "struct:Pose2d")
    b.start(7, "/.schema/struct:Pose2d", "structschema")
    b.double(1, 300, 1.5).double(1, 100, -2.25)  # out of order on purpose
    b.int64(2, 150, -7).boolean(3, 160, True).string(4, 170, "hello")
    b.float(5, 180, 0.5).record(6, 190, b"\x01\x02\x03")
    b.record(7, 5, b"Translation2d translation;Rotation2d rotation")
    return b


def test_reads_header_and_catalog(tmp_path: Path) -> None:
    log = read_wpilog(_sample_log().write(tmp_path / "a.wpilog"))

    assert log.version == 0x0100
    assert log.extra_header == "hdr"
    assert [(e.name, e.type) for e in log.catalog][:2] == [
        ("NT:/a/double", "double"),
        ("NT:/a/int", "int64"),
    ]
    assert log.truncated_bytes == 0


def test_decodes_typed_values_long_format(tmp_path: Path) -> None:
    df = read_wpilog(_sample_log().write(tmp_path / "a.wpilog")).samples()
    rows = {(r["signal"], r["ts_us"]): r for r in df.to_dicts()}

    assert rows[("NT:/a/double", 300)]["v_f64"] == 1.5
    assert rows[("NT:/a/double", 100)]["v_f64"] == -2.25
    assert rows[("NT:/a/int", 150)]["v_i64"] == -7
    assert rows[("NT:/a/bool", 160)]["v_bool"] is True
    assert rows[("NT:/a/str", 170)]["v_str"] == "hello"
    assert rows[("NT:/a/float", 180)]["v_f64"] == 0.5
    assert rows[("NT:/a/pose", 190)]["v_bytes"] == b"\x01\x02\x03"
    assert rows[("NT:/a/pose", 190)]["type"] == "struct:Pose2d"
    assert rows[("NT:/a/double", 300)]["v_bytes"] is None
    assert df.height == 8


def test_struct_schema_is_retrievable(tmp_path: Path) -> None:
    log = read_wpilog(_sample_log().write(tmp_path / "a.wpilog"))
    assert log.schemas() == {"struct:Pose2d": "Translation2d translation;Rotation2d rotation"}


def test_entry_ids_can_be_reused_after_finish(tmp_path: Path) -> None:
    b = WpilogBuilder().start(1, "first", "double").double(1, 10, 1.0).finish(1, 11)
    b.start(1, "second", "int64").int64(1, 20, 5)
    df = read_wpilog(b.write(tmp_path / "r.wpilog")).samples()

    assert df.select("signal", "ts_us").rows() == [("first", 10), ("second", 20)]


def test_data_for_unknown_entry_is_counted_not_emitted(tmp_path: Path) -> None:
    b = WpilogBuilder().start(1, "x", "double").double(1, 1, 1.0).double(9, 2, 2.0)
    log = read_wpilog(b.write(tmp_path / "u.wpilog"))

    assert log.samples().height == 1
    assert log.orphan_records == 1


def test_set_metadata_updates_catalog(tmp_path: Path) -> None:
    b = WpilogBuilder().start(1, "x", "double", metadata="m0").set_metadata(1, "m1")
    log = read_wpilog(b.write(tmp_path / "m.wpilog"))
    assert log.catalog[0].metadata == "m1"


def test_truncated_tail_keeps_complete_records(tmp_path: Path) -> None:
    b = WpilogBuilder().start(1, "x", "double").double(1, 1, 1.0).double(1, 2, 2.0)
    log = read_wpilog(b.write(tmp_path / "t.wpilog", truncate_tail=3))

    assert log.samples().height == 1
    assert log.truncated_bytes > 0


def test_size_mismatch_goes_to_bytes(tmp_path: Path) -> None:
    b = WpilogBuilder().start(1, "x", "double").record(1, 1, b"\x00\x01")
    row = read_wpilog(b.write(tmp_path / "s.wpilog")).samples().to_dicts()[0]
    assert row["v_f64"] is None
    assert row["v_bytes"] == b"\x00\x01"


def test_invalid_header_rejected(tmp_path: Path) -> None:
    path = tmp_path / "bad.wpilog"
    path.write_bytes(b"NOTLOG" + b"\x00" * 10)
    with pytest.raises(WpilogError, match="invalid-header"):
        read_wpilog(path)


def test_unsupported_version_rejected(tmp_path: Path) -> None:
    path = WpilogBuilder(version=0x0200).write(tmp_path / "v.wpilog")
    with pytest.raises(WpilogError, match="unsupported-version"):
        read_wpilog(path)


def test_empty_log_has_no_samples(tmp_path: Path) -> None:
    log = read_wpilog(WpilogBuilder().write(tmp_path / "e.wpilog"))
    assert log.samples().height == 0
