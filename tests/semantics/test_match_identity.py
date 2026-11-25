from pathlib import Path
from typing import Any

import pytest

from flashpoint.meta import extract
from flashpoint.readers.wpilog import read_wpilog
from flashpoint.semantics.match_identity import MatchIdentity, identify, match_key
from tests.wpilog_builder import WpilogBuilder


@pytest.mark.parametrize(
    ("args", "key"),
    [
        (("2026", "GACMP", "qm", 7, 1), "2026gacmp_qm7"),
        (("2026", "GACMP", "pm", 2, 1), "2026gacmp_pm2"),
        (("2026", "GACMP", "e", 10, None), "2026gacmp_e10"),
        (("2026", "GACMP", "qm", 7, 2), "2026gacmp_qm7r2"),
    ],
)
def test_match_key_format(args: tuple[str, str, str, int, int | None], key: str) -> None:
    assert match_key(*args) == key


def test_fms_identity() -> None:
    ident = identify("2026", fms_event="GACMP", fms_match_type=2, fms_match_number=7, fms_replay=1)
    assert (ident.key, ident.source, ident.kind, ident.warnings) == (
        "2026gacmp_qm7",
        "fms",
        "match",
        (),
    )


def test_filename_fallback() -> None:
    ident = identify("2026", file_event="GACMP", file_match_type="E", file_match_number=10)
    assert (ident.key, ident.source) == ("2026gacmp_e10", "filename")


def test_fms_wins_over_conflicting_filename() -> None:
    ident = identify(
        "2026", fms_event="GACMP", fms_match_type=2, fms_match_number=7,
        file_event="GACMP", file_match_type="Q", file_match_number=8,
    )  # fmt: skip
    assert ident.key == "2026gacmp_qm7"
    assert ident.warnings == ("match-identity-conflict",)


def test_non_match_log() -> None:
    ident = identify("2025", fms_event="GACMP")  # event set by the DS, but no match
    assert (ident.key, ident.kind, ident.source) == (None, "non-match", "none")


def test_unknown_season_gives_no_key() -> None:
    ident = identify("unknown", fms_event="GACMP", fms_match_type=2, fms_match_number=7)
    assert ident.key is None
    assert ident.warnings == ("no-season",)


# --- AdvantageKit and hoot labels (mvp-outside-team-logs) --------------------------------------


def _identify_wpilog(
    tmp_path: Path, builder: WpilogBuilder, name: str, **hoot: Any
) -> MatchIdentity:
    path = builder.write(tmp_path / name)
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)
    return identify(
        "2026", meta.fms_event, meta.fms_match_type, meta.fms_match_number, meta.fms_replay,
        meta.file.event, meta.file.match_type, meta.file.match_number, **hoot,
    )  # fmt: skip


def _ds(event: str | None, match_type: int | str, number: int) -> WpilogBuilder:
    b = WpilogBuilder()
    if event is not None:
        b.start(1, "/DriverStation/EventName", "string").string(1, 20, event)
    if isinstance(match_type, str):
        b.start(2, "/DriverStation/MatchType", "string").string(2, 20, match_type)
    else:
        b.start(2, "/DriverStation/MatchType", "int64").int64(2, 20, match_type)
    b.start(3, "/DriverStation/MatchNumber", "int64").int64(3, 20, number)
    return b


def test_advantagekit_driver_station_fields(tmp_path: Path) -> None:
    ident = _identify_wpilog(tmp_path, _ds("GACMP", 2, 7), "akit_26-04-09_21-51-13.wpilog")
    assert (ident.key, ident.source, ident.kind) == ("2026gacmp_qm7", "fms", "match")


def test_advantagekit_filename(tmp_path: Path) -> None:
    b = WpilogBuilder().start(1, "x", "double").double(1, 5, 1.0)
    ident = _identify_wpilog(tmp_path, b, "akit_26-04-09_21-51-13_GACMP_q7.wpilog")
    assert (ident.key, ident.source) == ("2026gacmp_qm7", "filename")


@pytest.mark.parametrize("level", ["q", "Q"])
def test_filename_level_is_case_insensitive(level: str) -> None:
    ident = identify("2026", file_event="GACMP", file_match_type=level, file_match_number=7)
    assert (ident.key, ident.source) == ("2026gacmp_qm7", "filename")


def test_partial_driver_station_fields_fall_to_the_hoot_label(tmp_path: Path) -> None:
    ident = _identify_wpilog(
        tmp_path, _ds(None, 0, 7), "akit_26-04-09_21-51-13.wpilog",
        hoot_event="GACMP", hoot_match_type="Q", hoot_match_number=7,
    )  # fmt: skip
    assert (ident.key, ident.source, ident.kind) == ("2026gacmp_qm7", "hoot-filename", "match")


def test_unknown_match_type(tmp_path: Path) -> None:
    ident = _identify_wpilog(tmp_path, _ds("GACMP", "Scrimmage", 7), "Test1.wpilog")
    assert (ident.key, ident.kind, ident.source) == (None, "non-match", "none")


def test_hoot_label_only_when_the_wpilog_has_nothing() -> None:
    ident = identify(
        "2026", fms_event="GACMP", fms_match_type=2, fms_match_number=7,
        hoot_event="GACMP", hoot_match_type="Q", hoot_match_number=8, hoot_conflict=True,
    )  # fmt: skip
    assert (ident.key, ident.source, ident.warnings) == ("2026gacmp_qm7", "fms", ())


def test_conflicting_hoot_labels_warn_when_used() -> None:
    ident = identify(
        "2026", hoot_event="GACMP", hoot_match_type="Q", hoot_match_number=7, hoot_conflict=True
    )
    assert (ident.key, ident.source) == ("2026gacmp_qm7", "hoot-filename")
    assert ident.warnings == ("match-identity-conflict",)
