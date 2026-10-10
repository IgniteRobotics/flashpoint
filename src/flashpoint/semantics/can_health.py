"""CAN health from hoot frame clocks: device reboots, frame gaps, and whether a gap was one
device's (isolated) or the whole bus's (bus-wide, which the logger causes).

Each Phoenix 6 device logs a `Timestamp` signal, one sample per received status frame. A gap
is an interval of at least GAP_FACTOR times the device's nominal period (its median interval
while enabled). Siblings are the other devices on the same hoot, mapped to a slot or not.
Overlap is decided on time intervals, never on equal timestamps (pitfalls #14, #38).
"""

from dataclasses import dataclass, replace

import duckdb

from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.alignment import HOOT_ENABLE

US_PER_S = 1_000_000
GAP_FACTOR = 2.5
STARTUP_SKIP_US = 5 * US_PER_S  # a hoot's first seconds: devices still booting and configuring
REBOOT_MERGE_US = 2 * US_PER_S  # a reset-count rise and a boot-during-enable rise: one reboot
_FRAME = r"^Phoenix6/([^/]+)/Timestamp$"
_RESET = r"^Phoenix6/([^/]+)/ResetCount$"
_BOOT = r"^Phoenix6/([^/]+)/Fault_BootDuringEnable$"


@dataclass(frozen=True)
class Gap:
    device: str  # e.g. "TalonFX-2"
    start_us: int  # hoot clock: the last frame before the silence
    end_us: int  # the next frame, or where a trailing silence is cut off
    missed: int  # frames that should have arrived
    bus_wide: bool


@dataclass(frozen=True)
class Reboot:
    device: str
    t_us: int  # hoot clock
    enabled: bool
    reset_before: int | None  # None: the hoot logs no reset count
    reset_after: int | None


@dataclass(frozen=True)
class HootHealth:
    enabled: list[tuple[int, int]]  # [start, end) on the hoot clock
    nominal_us: dict[str, int]  # every device measured (the siblings), by name
    gaps: list[Gap]  # gaps starting while enabled, after start-up
    bus_gaps: list[tuple[int, int]]  # bus-wide gaps merged where they overlap
    reboots: list[Reboot]
    reset_count_logged: bool
    first_us: int | None  # first and last sample of any signal in the hoot
    last_us: int | None
    last_frame_us: int | None  # last frame of any device


def _source(lake: LakePaths, log_id: str) -> str:
    pattern = str(lake.bronze / "season=*" / f"log_id={log_id}" / "*.parquet")
    return "read_parquet('" + pattern.replace("'", "''") + "')"


def device_name(model: str, can_id: int) -> str:
    return f"{model}-{can_id}"


def _enabled(con: duckdb.DuckDBPyConnection, source: str, last_us: int) -> list[tuple[int, int]]:
    """Runs of RobotEnable = true; each sample holds until the next, the last until the hoot
    ends."""
    rows = con.execute(
        f"SELECT ts_us, v_bool FROM {source} WHERE signal = ? ORDER BY ts_us",  # noqa: S608
        [HOOT_ENABLE],
    ).fetchall()
    windows: list[tuple[int, int]] = []
    start: int | None = None
    for ts, on in rows:
        if on and start is None:
            start = ts
        elif not on and start is not None:
            windows.append((start, ts))
            start = None
    if start is not None and last_us > start:
        windows.append((start, last_us))
    return windows


def _in(windows: list[tuple[int, int]], t: int) -> bool:
    return any(start <= t < end for start, end in windows)


def _reboots(
    con: duckdb.DuckDBPyConnection, source: str, enabled: list[tuple[int, int]]
) -> tuple[list[Reboot], bool]:
    resets = con.execute(
        f"""
        SELECT regexp_extract(signal, '{_RESET}', 1) AS device, ts_us,
            lag(v_i64) OVER (PARTITION BY signal ORDER BY ts_us) AS before, v_i64 AS after
        FROM {source} WHERE regexp_matches(signal, '{_RESET}')
        """  # noqa: S608 - internal path and constant patterns
    ).fetchall()
    boots = con.execute(
        f"""
        WITH s AS (
            SELECT regexp_extract(signal, '{_BOOT}', 1) AS device, ts_us, v_f64 > 0 AS on_,
                lag(v_f64 > 0) OVER (PARTITION BY signal ORDER BY ts_us) AS was_on
            FROM {source} WHERE regexp_matches(signal, '{_BOOT}')
        )
        SELECT device, ts_us FROM s WHERE on_ AND was_on = false
        """  # noqa: S608 - internal path and constant patterns
    ).fetchall()
    found = [
        Reboot(device, t, _in(enabled, t), before, after)
        for device, t, before, after in resets
        if before is not None and after > before
    ] + [Reboot(device, t, _in(enabled, t), None, None) for device, t in boots]
    # Reset-count rises sort first at equal times, so they keep their counts in a merge.
    found.sort(key=lambda r: (r.device, r.t_us, r.reset_before is None))
    reboots: list[Reboot] = []
    for reboot in found:
        last = reboots[-1] if reboots else None
        if (
            last is None
            or last.device != reboot.device
            or reboot.t_us - last.t_us > REBOOT_MERGE_US
        ):
            reboots.append(reboot)
        elif last.reset_before is None and reboot.reset_before is not None:
            reboots[-1] = replace(
                last, reset_before=reboot.reset_before, reset_after=reboot.reset_after
            )
    return sorted(reboots, key=lambda r: (r.t_us, r.device)), bool(resets)


def analyse_hoot(con: duckdb.DuckDBPyConnection, lake: LakePaths, log_id: str) -> HootHealth:
    """Reboots and frame gaps of every device in one hoot, on the hoot's own clock."""
    source = _source(lake, log_id)
    first_us, last_us = con.execute(
        f"SELECT min(ts_us), max(ts_us) FROM {source}"  # noqa: S608 - internal path
    ).fetchone() or (None, None)
    if first_us is None or last_us is None:
        return HootHealth([], {}, [], [], [], False, None, None, None)
    enabled = _enabled(con, source, last_us)
    reboots, reset_logged = _reboots(con, source, enabled)
    # Gaps only count after start-up; the windows here are what gaps must start in.
    counted = [
        (max(start, first_us + STARTUP_SKIP_US), end)
        for start, end in enabled
        if end > first_us + STARTUP_SKIP_US
    ]
    con.execute("DROP TABLE IF EXISTS can_windows")
    con.execute("CREATE TEMP TABLE can_windows (w0 BIGINT, w1 BIGINT)")
    if counted:
        con.executemany("INSERT INTO can_windows VALUES (?, ?)", counted)
    con.execute("DROP TABLE IF EXISTS can_intervals")
    con.execute(
        f"""
        CREATE TEMP TABLE can_intervals AS
        WITH f AS (
            SELECT DISTINCT regexp_extract(signal, '{_FRAME}', 1) AS device, ts_us
            FROM {source} WHERE regexp_matches(signal, '{_FRAME}')
        )
        SELECT device, ts_us AS g0, lead(ts_us) OVER (PARTITION BY device ORDER BY ts_us) AS g1
        FROM f
        """  # noqa: S608 - internal path and constant patterns
    )
    last_frame_us = con.execute("SELECT max(g0) FROM can_intervals").fetchone()
    last_frame = last_frame_us[0] if last_frame_us else None
    devices = [r[0] for r in con.execute("SELECT DISTINCT device FROM can_intervals").fetchall()]
    nominal = dict(
        con.execute(
            """
            SELECT i.device, CAST(round(median(i.g1 - i.g0)) AS BIGINT)
            FROM can_intervals i JOIN can_windows w ON i.g0 >= w.w0 AND i.g0 < w.w1
            WHERE i.g1 IS NOT NULL GROUP BY i.device
            """
        ).fetchall()
    )
    # A device that stops early gets a trailing gap, cut at the window end and at the hoot's
    # last frame (the end of a hoot is never a gap).
    rows = con.execute(
        """
        WITH n AS (SELECT * FROM (VALUES {}) AS t(device, med)),
        gaps AS (
            SELECT i.device, i.g0,
                CASE WHEN i.g1 IS NULL THEN least(w.w1, ?) ELSE i.g1 END AS g1, n.med
            FROM can_intervals i
            JOIN n ON n.device = i.device
            JOIN can_windows w ON i.g0 >= w.w0 AND i.g0 < w.w1
        ),
        g AS (SELECT * FROM gaps WHERE g1 - g0 >= ? * med)
        SELECT a.device, a.g0, a.g1, CAST(round((a.g1 - a.g0) / a.med) AS BIGINT) - 1 AS missed,
            count(DISTINCT b.device) AS overlapping
        FROM g a LEFT JOIN g b ON b.device <> a.device AND b.g0 < a.g1 AND a.g0 < b.g1
        GROUP BY ALL ORDER BY a.g0, a.device
        """.format(  # noqa: S608 - values are bound below
            ", ".join(["(?, ?)"] * len(nominal)) or "(NULL::VARCHAR, NULL::BIGINT)"
        ),
        [v for item in nominal.items() for v in item] + [last_frame, GAP_FACTOR],
    ).fetchall()
    siblings = len(devices) - 1
    gaps = [
        Gap(d, g0, g1, max(missed, 0), siblings > 0 and 2 * overlapping >= siblings)
        for d, g0, g1, missed, overlapping in rows
        if g1 > g0
    ]
    return HootHealth(
        enabled=enabled,
        nominal_us=nominal,
        gaps=gaps,
        bus_gaps=_merge([(g.start_us, g.end_us) for g in gaps if g.bus_wide]),
        reboots=reboots,
        reset_count_logged=reset_logged,
        first_us=first_us,
        last_us=last_us,
        last_frame_us=last_frame,
    )


def _merge(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if merged and start < merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged
