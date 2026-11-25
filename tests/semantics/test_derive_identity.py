"""Match identity from the hoots' filename label when the wpilog has none (match-identity)."""

from datetime import UTC, datetime
from pathlib import Path

from flashpoint.lake.paths import LakePaths
from tests.synthetic_lake import add_hoot, derive, ingest_wpilogs, query
from tests.wpilog_builder import WpilogBuilder

START = datetime(2026, 4, 9, 21, 51, 13, tzinfo=UTC)
EPOCH_US = int(START.timestamp() * 1_000_000)
CANIVORE = "6E9415C3394C485320202050101C18FF"


def _unnamed_wpilog(tmp_path: Path, seconds: int = 300) -> Path:
    """Anchored by systemTime, but no FMS fields and no event in the filename."""
    b = WpilogBuilder().start(1, "systemTime", "int64")
    for i in range(seconds):
        b.int64(1, i * 1_000_000, EPOCH_US + i * 1_000_000)
    (tmp_path / "in").mkdir(exist_ok=True)
    return b.write(tmp_path / "in" / "FRC_20260409_215113.wpilog")


def _session(lake: LakePaths) -> dict[str, object]:
    rows = query(lake, "SELECT * FROM sessions WHERE wpilog_id IS NOT NULL")
    assert len(rows) == 1
    return rows[0]


def test_hoot_label_rescues_an_unnamed_wpilog(tmp_path: Path) -> None:
    lake = ingest_wpilogs(tmp_path, [_unnamed_wpilog(tmp_path)])
    add_hoot(lake, "rio", "GACMP_Q7_rio_2026-04-09_21-51-06.hoot")
    add_hoot(lake, "canivore", f"GACMP_Q7_{CANIVORE}_2026-04-09_21-51-06.hoot")
    derive(lake, tmp_path / "config")

    session = _session(lake)
    assert (session["match_key"], session["match_source"], session["kind"]) == (
        "2026gacmp_qm7",
        "hoot-filename",
        "match",
    )
    assert session["warnings"] is None
    grouped = query(lake, "SELECT session_id FROM session_hoots")
    assert {r["session_id"] for r in grouped} == {session["session_id"]}


def test_hoot_groups_disagree(tmp_path: Path) -> None:
    lake = ingest_wpilogs(tmp_path, [_unnamed_wpilog(tmp_path)])
    add_hoot(lake, "q8", "GACMP_Q8_rio_2026-04-09_21-53-30.hoot")
    add_hoot(lake, "q7", "GACMP_Q7_rio_2026-04-09_21-51-20.hoot")
    derive(lake, tmp_path / "config")

    session = _session(lake)
    assert (session["match_key"], session["match_source"]) == ("2026gacmp_qm7", "hoot-filename")
    assert session["warnings"] == "match-identity-conflict"


def test_hoot_only_session_source_is_hoot_filename(tmp_path: Path) -> None:
    lake = ingest_wpilogs(tmp_path, [])
    add_hoot(lake, "rio", "GACMP_Q7_rio_2026-04-09_21-51-06.hoot")
    derive(lake, tmp_path / "config")

    (session,) = query(lake, "SELECT * FROM sessions")
    assert (session["match_key"], session["match_source"]) == ("2026gacmp_qm7", "hoot-filename")
