import struct
from collections.abc import Sequence
from datetime import UTC, datetime

import polars as pl
import pytest

from flashpoint.semantics.alignment import (
    MIN_PAYLOAD_MATCHES,
    align_by_enable_edges,
    align_by_payload,
    hoot_bus_agreement_us,
)
from flashpoint.semantics.sessions import HootLog, WpilogLog, group_sessions

OFFSET = 19_563_500  # wpilog ts = hoot ts + OFFSET


def _frame(
    rows: Sequence[tuple[object, ...]],
) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={"signal": pl.String, "type": pl.String, "ts_us": pl.Int64, "v_f64": pl.Float64,
                "v_bytes": pl.Binary, "v_bool": pl.Boolean},
        orient="row",
    )  # fmt: skip


def _pose(i: int) -> bytes:
    return struct.pack("<3d", i * 0.01, i * 0.02, i * 0.001)


def _pose_logs(n: int, jitter_us: int = 0) -> tuple[pl.DataFrame, pl.DataFrame]:
    hoot = [
        ("DriveState/Pose", "struct:Pose2d", 1_000_000 + i * 6_667, None, _pose(i), None)
        for i in range(n)
    ]
    wpi = [
        (
            "NT:/DriveState/Pose",
            "struct:Pose2d",
            ts + OFFSET + (jitter_us if i % 2 else 0),
            None,
            b,
            None,
        )
        for i, (_, _, ts, _, b, _) in enumerate(hoot)
        if i % 3 == 0  # NetworkTables publishes at a lower rate
    ]
    return _frame(wpi), _frame(hoot)


def test_payload_match_recovers_offset_exactly() -> None:
    wpi, hoot = _pose_logs(600)
    result = align_by_payload(wpi, hoot)
    assert result is not None
    assert (result.method, result.confidence) == ("payload-match", "high")
    assert result.offset_us == OFFSET
    assert result.spread_us == 0
    assert result.matches == 200


def test_payload_match_ignores_repeated_values() -> None:
    wpi, hoot = _pose_logs(600)
    still = struct.pack("<3d", 0.0, 0.0, 0.0)  # robot standing still: same pose many times
    hoot = pl.concat(
        [
            hoot,
            _frame(
                [("DriveState/Pose", "struct:Pose2d", 5 + i, None, still, None) for i in range(50)]
            ),
        ]
    )
    wpi = pl.concat(
        [
            wpi,
            _frame(
                [
                    ("NT:/DriveState/Pose", "struct:Pose2d", 9 + i, None, still, None)
                    for i in range(50)
                ]
            ),
        ]
    )
    result = align_by_payload(wpi, hoot)
    assert result is not None and result.offset_us == OFFSET


def test_payload_match_needs_enough_points_and_tight_spread() -> None:
    wpi, hoot = _pose_logs(3 * (MIN_PAYLOAD_MATCHES - 1))
    assert align_by_payload(wpi, hoot) is None
    noisy_wpi, noisy_hoot = _pose_logs(600, jitter_us=20_000)
    assert align_by_payload(noisy_wpi, noisy_hoot) is None


def _edges(signal: str, ts: list[int], start_on: bool = False) -> pl.DataFrame:
    rows = [(signal, "boolean", t, None, None, (i % 2 == 0) != start_on) for i, t in enumerate(ts)]
    return _frame(rows)


def test_enable_edges_low_confidence() -> None:
    hoot_edges = [100_000_000, 115_000_000, 132_000_000, 272_000_000]
    wpi = _edges(
        "DS:enabled",
        [t + OFFSET + d for t, d in zip(hoot_edges, [30_000, -40_000, 450_000, 0], strict=True)],
    )
    hoot = _edges("RobotEnable", hoot_edges)
    # booleans flip on each edge starting True; prepend initial False state
    wpi = pl.concat([_frame([("DS:enabled", "boolean", 0, None, None, False)]), wpi])
    hoot = pl.concat([_frame([("RobotEnable", "boolean", 0, None, None, False)]), hoot])
    result = align_by_enable_edges(wpi, hoot)
    assert result is not None
    assert (result.method, result.confidence) == ("enable-edges", "low")
    assert abs(result.offset_us - OFFSET) < 100_000
    assert align_by_enable_edges(wpi, hoot, wall_clock_us=OFFSET + 3_000_000) is not None
    assert align_by_enable_edges(wpi, hoot, wall_clock_us=OFFSET + 60_000_000) is None


def test_single_wpilog_edge_is_rejected() -> None:
    # Corpus E10 restart: one wpilog edge mis-paired with unrelated hoot edges (gave 127.8 s).
    wpi = _frame(
        [
            ("DS:enabled", "boolean", 0, None, None, False),
            ("DS:enabled", "boolean", 130_660_000, None, None, True),
        ]
    )
    hoot = pl.concat(
        [
            _frame([("RobotEnable", "boolean", 0, None, None, False)]),
            _edges("RobotEnable", [1_980_000, 2_170_000, 3_700_000]),
        ]
    )
    assert align_by_enable_edges(wpi, hoot) is None


def test_bus_agreement_from_shared_enable_edges() -> None:
    edges = [100_000_000, 115_000_000]
    rio = pl.concat(
        [_frame([("RobotEnable", "boolean", 0, None, None, False)]), _edges("RobotEnable", edges)]
    )
    can = pl.concat(
        [
            _frame([("RobotEnable", "boolean", 0, None, None, False)]),
            _edges("RobotEnable", [e + 300 for e in edges]),
        ]
    )
    assert hoot_bus_agreement_us(rio, can) == 300


# --- grouping ----------------------------------------------------------------------------


def _w(
    log_id: str, start: str, end: str, event: str | None = "GACMP", match: str | None = "Q7"
) -> WpilogLog:
    return WpilogLog(
        log_id, datetime.fromisoformat(start), datetime.fromisoformat(end), event, match
    )


def _h(
    log_id: str, stamp: str, bus: str, event: str | None = "GACMP", match: str | None = "Q7"
) -> HootLog:
    return HootLog(log_id, stamp, bus, event, match)


def test_qual_session_groups_wpilog_and_both_buses() -> None:
    sessions = group_sessions(
        [_w("w", "2026-04-09T21:50:42+00:00", "2026-04-09T21:54:15+00:00")],
        [_h("r", "2026-04-09_21-51-06", "rio"), _h("c", "2026-04-09_21-51-06", "CAFE" * 8)],
    )
    assert len(sessions) == 1
    (session,) = sessions
    assert session.wpilog_id == "w"
    assert [sorted(g.log_ids) for g in session.hoot_groups] == [["c", "r"]]


def test_restart_inside_wpilog_span_gives_two_hoot_groups() -> None:
    sessions = group_sessions(
        [_w("w", "2026-04-11T19:26:00+00:00", "2026-04-11T19:31:00+00:00", match="E10")],
        [
            _h("r1", "2026-04-11_19-26-19", "rio", match="E10"),
            _h("r2", "2026-04-11_19-29-15", "rio", match="E10"),
        ],
    )
    assert len(sessions) == 1
    assert [g.stamp for g in sessions[0].hoot_groups] == [
        "2026-04-11_19-26-19",
        "2026-04-11_19-29-15",
    ]


def test_hoot_group_after_wpilog_end_is_its_own_session() -> None:
    # Corpus E10: the wpilog ended 19:28:10, and the hoot logger restarted at 19:29:15.
    sessions = group_sessions(
        [_w("w", "2026-04-11T19:25:48+00:00", "2026-04-11T19:28:10+00:00", match="E10")],
        [
            _h("r1", "2026-04-11_19-26-19", "rio", match="E10"),
            _h("r2", "2026-04-11_19-29-15", "rio", match="E10"),
        ],
    )
    by_wpilog = {s.wpilog_id: [g.stamp for g in s.hoot_groups] for s in sessions}
    assert by_wpilog == {"w": ["2026-04-11_19-26-19"], None: ["2026-04-11_19-29-15"]}


def test_mismatched_match_label_or_far_time_makes_hoot_only_session() -> None:
    sessions = group_sessions(
        [_w("w", "2026-04-09T21:50:42+00:00", "2026-04-09T21:54:15+00:00")],
        [_h("x", "2026-04-09_21-51-06", "rio", match="Q8"), _h("y", "2026-04-09_23-00-00", "rio")],
    )
    kinds = sorted(
        (s.wpilog_id or "", tuple(sorted(i for g in s.hoot_groups for i in g.log_ids)))
        for s in sessions
    )
    assert kinds == [("", ("x",)), ("", ("y",)), ("w", ())]


def test_stamp_is_utc() -> None:
    assert _h("r", "2026-04-09_21-51-06", "rio").start == datetime(
        2026, 4, 9, 21, 51, 6, tzinfo=UTC
    )


@pytest.mark.parametrize("bus", ["rio", "6E9415C3394C485320202050101C18FF"])
def test_bus_kept(bus: str) -> None:
    (session,) = group_sessions([], [_h("r", "2026-04-09_21-51-06", bus)])
    assert session.hoot_groups[0].buses == {"r": bus}
