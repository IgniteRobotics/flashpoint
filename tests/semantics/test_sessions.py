"""Session grouping of AdvantageKit wpilogs with hoots (log-metadata "Wall-clock anchor")."""

from datetime import UTC, datetime
from pathlib import Path

from flashpoint.meta import extract
from flashpoint.readers.wpilog import read_wpilog
from flashpoint.semantics.sessions import HootLog, WpilogLog, group_sessions
from tests.wpilog_builder import WpilogBuilder

START = datetime(2026, 4, 9, 21, 51, 13, tzinfo=UTC)
EPOCH_US = int(START.timestamp() * 1_000_000)
HOOT_STAMP = "2026-04-09_21-51-20"  # the hoot logger starts 7 s into the wpilog


def _akit_log(tmp_path: Path, epoch: bool) -> WpilogLog:
    b = WpilogBuilder().start(1, "/SystemStats/EpochTimeMicros", "int64")
    b.start(2, "/RealOutputs/x", "double")
    for i in range(0, 150):
        ts = i * 1_000_000
        if epoch:
            b.int64(1, ts, EPOCH_US + ts)
        b.double(2, ts, float(i))
    path = b.write(tmp_path / "akit_26-04-09_21-51-13.wpilog")
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)
    return WpilogLog("akit", meta.utc_start, meta.utc_end, meta.file.event, None)


def _hoots() -> list[HootLog]:
    return [
        HootLog("rio-hoot", HOOT_STAMP, "rio"),
        HootLog("canivore-hoot", HOOT_STAMP, "6E9415C3394C485320202050101C18FF"),
    ]


def test_epoch_anchored_wpilog_groups_with_its_hoots(tmp_path: Path) -> None:
    sessions = group_sessions([_akit_log(tmp_path, epoch=True)], _hoots())

    assert [s.session_id for s in sessions] == ["akit"]
    assert [g.stamp for g in sessions[0].hoot_groups] == [HOOT_STAMP]
    assert set(sessions[0].hoot_groups[0].log_ids) == {"rio-hoot", "canivore-hoot"}


def test_no_anchor_entry_makes_a_hoot_only_session(tmp_path: Path) -> None:
    sessions = group_sessions([_akit_log(tmp_path, epoch=False)], _hoots())

    by_id = {s.session_id: s for s in sessions}
    assert by_id["akit"].hoot_groups == []
    hoot_only = [s for s in sessions if s.wpilog_id is None]
    assert len(hoot_only) == 1
    assert hoot_only[0].session_id.startswith(f"hoot-{HOOT_STAMP}")
