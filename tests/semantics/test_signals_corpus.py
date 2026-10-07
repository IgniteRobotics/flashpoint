from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.query import connect
from flashpoint.semantics.robot_config import load_robots, load_seasons

pytestmark = pytest.mark.corpus


@pytest.fixture(scope="module")
def signals(derived_corpus_lake: Any) -> dict[str, Any]:
    con = connect(derived_corpus_lake)
    sessions = {
        name: sid
        for sid, name in con.sql(
            "SELECT s.session_id, l.filename FROM meta_sessions s"
            " JOIN meta_logs l ON l.log_id = s.wpilog_id"
        ).fetchall()
    }
    rows = con.sql(
        "SELECT session_id, signal_id, count(*) AS n, count(match_time_us) AS timed"
        " FROM signals GROUP BY ALL"
    ).fetchall()
    missing = con.sql("SELECT session_id, signal_id, reason FROM meta_missing_signals").fetchall()
    return {"sessions": sessions, "rows": rows, "missing": missing}


def _declared(robot_id: str) -> set[str]:
    root = config.config_root()
    seasons = load_seasons(root / "seasons")
    robot = next(r for r in load_robots(root / "robots", seasons) if r.robot == robot_id)
    return {s.id for s in robot.signals}


def _by_signal(signals: dict[str, Any], wpilog: str) -> dict[str, tuple[int, int]]:
    sid = signals["sessions"][wpilog]
    return {s: (n, timed) for session, s, n, timed in signals["rows"] if session == sid}


def _missing(signals: dict[str, Any], wpilog: str) -> list[tuple[str, str]]:
    sid = signals["sessions"][wpilog]
    return [(s, r) for session, s, r in signals["missing"] if session == sid]


@pytest.mark.parametrize(
    ("wpilog", "robot"),
    [
        ("FRC_20260409_215113_GACMP_Q7.wpilog", "2026-comp"),
        ("FRC_20250307_213719_GADAL_Q30.wpilog", "2025-comp"),
    ],
)
def test_every_declared_signal_matches(signals: dict[str, Any], wpilog: str, robot: str) -> None:
    assert _missing(signals, wpilog) == []
    assert set(_by_signal(signals, wpilog)) == _declared(robot)


def test_match_signals_carry_match_time(signals: dict[str, Any]) -> None:
    q7 = _by_signal(signals, "FRC_20260409_215113_GACMP_Q7.wpilog")
    assert all(timed == n for n, timed in q7.values())


def test_session_without_hoots_gets_signals(signals: dict[str, Any]) -> None:
    p2 = _by_signal(signals, "FRC_20260409_171208_GACMP_P2.wpilog")
    assert sum(n for n, _ in p2.values()) > 0


def test_practice_session_has_empty_match_time(signals: dict[str, Any]) -> None:
    nofms = _by_signal(signals, "FRC_20250404_221544.wpilog")
    assert sum(n for n, _ in nofms.values()) > 0
    assert {timed for _, timed in nofms.values()} == {0}
