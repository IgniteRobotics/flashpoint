"""CAN health from hoot frame clocks: device reboots, frame gaps, and whether a gap was one
device's (isolated) or the whole bus's (bus-wide, which the logger causes).

Each Phoenix 6 device logs a `Timestamp` signal, one sample per received status frame. A gap
is an interval of at least GAP_FACTOR times the device's nominal period (its median interval
while enabled). Siblings are the other devices on the same hoot, mapped to a slot or not.
Overlap is decided on time intervals, never on equal timestamps (pitfalls #14, #38).
"""

from dataclasses import dataclass, field, replace
from typing import Any

import duckdb
import numpy as np
import polars as pl

from flashpoint import config as fp_config
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.alignment import HOOT_ENABLE
from flashpoint.semantics.framing import Framing
from flashpoint.semantics.gold import _periods
from flashpoint.semantics.identity import Observation
from flashpoint.semantics.robot_config import RobotConfig
from flashpoint.semantics.signals import signals_dir

US_PER_S = 1_000_000
GAP_FACTOR = 2.5
STARTUP_SKIP_US = 5 * US_PER_S  # a hoot's first seconds: devices still booting and configuring
REBOOT_MERGE_US = 2 * US_PER_S  # a reset-count rise and a boot-during-enable rise: one reboot
EVENT_FLOOR_US = round(fp_config.CAN_EVENT_FLOOR_S * US_PER_S)
TABLE, BUS_TABLE, EVENTS_TABLE = "can_health", "can_bus_health", "can_events"
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
    counted: list[tuple[int, int]]  # enabled after start-up: where gaps are counted
    devices: list[str]  # every device with frames
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


def _device(timestamp_signal: str) -> str:
    """ "Phoenix6/TalonFX-2/Timestamp" -> "TalonFX-2"."""
    return timestamp_signal.split("/")[1]


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


def _inside(t: np.ndarray, windows: list[tuple[int, int]]) -> np.ndarray:
    mask = np.zeros(len(t), bool)
    for start, end in windows:
        mask |= (t >= start) & (t < end)
    return mask


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
        return HootHealth([], [], [], {}, [], [], [], False, None, None, None)
    enabled = _enabled(con, source, last_us)
    reboots, reset_logged = _reboots(con, source, enabled)
    # Gaps only count after start-up; the windows here are what gaps must start in.
    counted = [
        (max(start, first_us + STARTUP_SKIP_US), end)
        for start, end in enabled
        if end > first_us + STARTUP_SKIP_US
    ]
    signals = [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT signal FROM {source} WHERE signal LIKE 'Phoenix6/%/Timestamp'"  # noqa: S608
        ).fetchall()
    ]
    # One device at a time: bronze is sorted by (signal, ts_us), so each read touches few row
    # groups and memory stays at one device's frames.
    frames = {
        _device(signal): con.execute(
            f"SELECT ts_us FROM {source} WHERE signal = ? ORDER BY ts_us",  # noqa: S608
            [signal],
        ).fetchnumpy()["ts_us"]
        for signal in sorted(signals)
    }
    devices = sorted(frames)
    last_frame = max((int(f[-1]) for f in frames.values() if len(f)), default=None)
    nominal: dict[str, int] = {}
    starts: list[int] = []
    ends: list[int] = []
    owners: list[int] = []
    for index, device in enumerate(devices):
        ts = np.unique(frames[device])  # two status groups can share a frame time
        g0, g1 = ts[:-1], ts[1:]
        inside = _inside(g0, counted)
        if not inside.any():
            continue
        med = int(round(float(np.median(g1[inside] - g0[inside]))))
        nominal[device] = med
        keep = inside & (g1 - g0 >= GAP_FACTOR * med)
        starts += g0[keep].tolist()
        ends += g1[keep].tolist()
        owners += [index] * int(keep.sum())
        # A device that stops early: a trailing gap, cut at the window end and at the hoot's
        # last frame (the end of a hoot is never a gap).
        last = int(ts[-1])
        for w0, w1 in counted:
            if w0 <= last < w1 and last_frame is not None:
                end = min(w1, last_frame)
                if end - last >= GAP_FACTOR * med:
                    starts.append(last)
                    ends.append(end)
                    owners.append(index)
    gap_start, gap_end, gap_owner = np.array(starts), np.array(ends), np.array(owners)
    siblings = len(devices) - 1
    gaps = []
    for i in np.lexsort((gap_owner, gap_start)):
        overlap = (gap_start < gap_end[i]) & (gap_start[i] < gap_end) & (gap_owner != gap_owner[i])
        others = len(np.unique(gap_owner[overlap]))
        device = devices[gap_owner[i]]
        length = int(gap_end[i] - gap_start[i])
        gaps.append(
            Gap(
                device,
                int(gap_start[i]),
                int(gap_end[i]),
                max(int(round(length / nominal[device])) - 1, 0),
                siblings > 0 and 2 * others >= siblings,
            )
        )
    return HootHealth(
        enabled=enabled,
        counted=counted,
        devices=sorted(devices),
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


# --- per-session gold -----------------------------------------------------------------------

SCHEMA: dict[str, pl.DataType] = {
    "match_key": pl.String(),
    "session_id": pl.String(),
    "season": pl.String(),
    "robot": pl.String(),
    "phase": pl.String(),
    "slot_id": pl.String(),
    "unit_id": pl.String(),
    "subsystem": pl.String(),
    "role": pl.String(),
    "bus": pl.String(),
    "reboots": pl.Int64(),
    "reboots_enabled": pl.Int64(),
    "isolated_gaps": pl.Int64(),
    "missed_per_min": pl.Float64(),
    "longest_gap_ms": pl.Float64(),
    "reset_count_logged": pl.Boolean(),
    "alignment": pl.String(),
    "source_logs": pl.String(),
}
BUS_SCHEMA: dict[str, pl.DataType] = {
    "match_key": pl.String(),
    "session_id": pl.String(),
    "season": pl.String(),
    "robot": pl.String(),
    "phase": pl.String(),
    "bus": pl.String(),
    "bus_name": pl.String(),
    "devices": pl.Int64(),
    "logger_gaps": pl.Int64(),
    "logger_gap_s": pl.Float64(),
    "longest_logger_gap_ms": pl.Float64(),
    "util_max": pl.Float64(),
    "util_mean": pl.Float64(),
    "bus_off_delta": pl.Int64(),
    "tx_full_delta": pl.Int64(),
    "rx_err_max": pl.Int64(),
    "tx_err_max": pl.Int64(),
    "alignment": pl.String(),
    "source_logs": pl.String(),
}
EVENTS_SCHEMA: dict[str, pl.DataType] = {
    "session_id": pl.String(),
    "season": pl.String(),
    "match_key": pl.String(),
    "robot": pl.String(),
    "log_id": pl.String(),
    "bus": pl.String(),
    "device": pl.String(),  # None for a bus gap
    "slot_id": pl.String(),
    "unit_id": pl.String(),
    "kind": pl.String(),  # reboot | dropout | bus_gap
    "hoot_t_us": pl.Int64(),
    "t_us": pl.Int64(),  # lake (wpilog) clock; None when the hoot is not aligned
    "match_time_us": pl.Int64(),
    "duration_ms": pl.Float64(),
    "enabled": pl.Boolean(),
    "reset_before": pl.Int64(),
    "reset_after": pl.Int64(),
}
_STATUS = ("utilization", "bus_off_count", "tx_full_count", "receive_error_count",
           "transmit_error_count")  # fmt: skip


@dataclass(frozen=True)
class HootInput:
    log_id: str
    bus: str  # "rio" or the CANivore's hex id
    offset_us: int | None  # lake time = hoot time + offset; None: not aligned


@dataclass
class SessionCanHealth:
    slots: list[dict[str, Any]] = field(default_factory=list)
    buses: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)


def _model_id(device: str) -> tuple[str, int] | None:
    model, _, can_id = device.rpartition("-")
    return (model, int(can_id)) if model and can_id.isdigit() else None


def _overlap(a: tuple[int, int], b: tuple[int, int]) -> int:
    return max(0, min(a[1], b[1]) - max(a[0], b[0]))


def _unit_ranges(
    observations: list[Observation], slot_id: str
) -> list[tuple[str | None, int, int]]:
    """(unit, from, to) on the lake clock; one open range with no unit if none is observed."""
    lo, hi = -(2**62), 2**62
    ranges = [
        (o.unit_id, lo if o.from_ts_us is None else o.from_ts_us,
         hi if o.to_ts_us is None else o.to_ts_us)
        for o in observations if o.slot_id == slot_id
    ]  # fmt: skip
    return sorted(ranges, key=lambda r: r[1]) or [(None, lo, hi)]


def _unit_at(ranges: list[tuple[str | None, int, int]], t_us: int | None) -> str | None:
    if t_us is None:
        return ranges[0][0] if len(ranges) == 1 else None
    return next((u for u, start, end in ranges if start <= t_us < end), None)


def _held(points: list[tuple[int, float]], start: int, end: int) -> list[tuple[int, int, float]]:
    """(from, to, value) pieces inside [start, end), each sample held until the next; the value
    in force at `start` is the last sample at or before it."""
    pieces = []
    for i, (t, v) in enumerate(points):
        nxt = points[i + 1][0] if i + 1 < len(points) else end
        a, z = max(t, start), min(nxt, end)
        if z > a:
            pieces.append((a, z, v))
    return pieces


def _status(series: dict[str, list[tuple[int, float]]], start: int, end: int) -> dict[str, Any]:
    out: dict[str, Any] = dict.fromkeys(
        ("util_max", "util_mean", "bus_off_delta", "tx_full_delta", "rx_err_max", "tx_err_max")
    )
    util = _held(series.get("utilization", []), start, end)
    if util:
        span = sum(z - a for a, z, _ in util)
        out["util_max"] = max(v for *_, v in util)
        out["util_mean"] = sum((z - a) * v for a, z, v in util) / span
    for metric, key in (("bus_off_count", "bus_off_delta"), ("tx_full_count", "tx_full_delta")):
        pieces = _held(series.get(metric, []), start, end)
        if pieces:
            out[key] = int(pieces[-1][2] - pieces[0][2])
    for metric, key in (("receive_error_count", "rx_err_max"),
                        ("transmit_error_count", "tx_err_max")):  # fmt: skip
        pieces = _held(series.get(metric, []), start, end)
        if pieces:
            out[key] = int(max(v for *_, v in pieces))
    return out


def _bus_status(
    con: duckdb.DuckDBPyConnection, lake: LakePaths, session: dict[str, Any], bus_name: str
) -> dict[str, list[tuple[int, float]]]:
    path = signals_dir(lake) / f"season={session['season']}" / f"session_id={session['session_id']}"
    if not any(path.glob("*.parquet")):
        return {}
    source = "read_parquet('" + str(path / "*.parquet").replace("'", "''") + "')"
    rows = con.execute(
        f"SELECT metric, t_us, value FROM {source}"  # noqa: S608 - internal path
        " WHERE subsystem = 'can' AND component = ? ORDER BY metric, t_us",
        [bus_name],
    ).fetchall()
    series: dict[str, list[tuple[int, float]]] = {}
    for metric, t, value in rows:
        if metric in _STATUS:
            series.setdefault(metric, []).append((t, value))
    return series


@dataclass
class _Context:
    """What one session's rows share: the session, its framing, robot, and unit ranges."""

    session: dict[str, Any]
    framing: Framing
    robot: RobotConfig | None
    observations: list[Observation]
    alignment: str
    source_logs: str
    periods: dict[str, tuple[int, int]] = field(init=False)
    ranges: dict[str, list[tuple[str | None, int, int]]] = field(init=False)

    def __post_init__(self) -> None:
        keyed = self.session["match_key"] and self.framing.match_start_us is not None
        self.periods = _periods(self.framing) if keyed else {}
        slots = self.robot.slots if self.robot is not None else []
        self.ranges = {s.id: _unit_ranges(self.observations, s.id) for s in slots}

    def base(self) -> dict[str, Any]:
        s = self.session
        return {"session_id": s["session_id"], "season": s["season"], "robot": s["robot"],
                "match_key": s["match_key"]}  # fmt: skip

    def slot_of(self, bus: str, device: str) -> str | None:
        parsed = _model_id(device)
        slot = self.robot.slot_for(bus, *parsed) if self.robot and parsed else None
        return slot.id if slot else None


def _events(ctx: _Context, hoot: HootInput, health: HootHealth) -> list[dict[str, Any]]:
    start_us = ctx.framing.match_start_us

    def event(kind: str, device: str | None, t: int, **extra: Any) -> dict[str, Any]:
        slot = ctx.slot_of(hoot.bus, device) if device else None
        lake_t = None if hoot.offset_us is None else t + hoot.offset_us
        timed = lake_t is not None and ctx.periods and start_us is not None
        return {
            **ctx.base(), "log_id": hoot.log_id, "bus": hoot.bus, "device": device,
            "slot_id": slot, "unit_id": _unit_at(ctx.ranges[slot], lake_t) if slot else None,
            "kind": kind, "hoot_t_us": t, "t_us": lake_t,
            "match_time_us": lake_t - start_us if timed else None,  # type: ignore[operator]
            "duration_ms": None, "enabled": None, "reset_before": None, "reset_after": None,
            **extra,
        }  # fmt: skip

    events = [
        event("reboot", r.device, r.t_us, enabled=r.enabled, reset_before=r.reset_before,
              reset_after=r.reset_after)
        for r in health.reboots
    ]  # fmt: skip
    events += [
        event("dropout", g.device, g.start_us, duration_ms=(g.end_us - g.start_us) / 1000,
              enabled=True)
        for g in health.gaps
        if not g.bus_wide and g.end_us - g.start_us >= EVENT_FLOOR_US
    ]  # fmt: skip
    events += [
        event("bus_gap", None, a, duration_ms=(z - a) / 1000, enabled=True)
        for a, z in health.bus_gaps
        if z - a >= EVENT_FLOOR_US
    ]
    return events


def _bus_rows(
    ctx: _Context,
    con: duckdb.DuckDBPyConnection,
    lake: LakePaths,
    hoot: HootInput,
    health: HootHealth,
    offset: int,
) -> list[dict[str, Any]]:
    bus_name = ctx.robot.bus_name_for(hoot.bus) if ctx.robot is not None else None
    status = _bus_status(con, lake, ctx.session, bus_name) if bus_name else {}
    rows = []
    for phase, (p0, p1) in ctx.periods.items():
        gaps = [(a, z) for a, z in health.bus_gaps if p0 - offset <= a < p1 - offset]
        rows.append(
            {
                **ctx.base(), "phase": phase, "bus": hoot.bus, "bus_name": bus_name,
                "devices": len(health.devices), "logger_gaps": len(gaps),
                "logger_gap_s": sum(z - a for a, z in gaps) / US_PER_S,
                "longest_logger_gap_ms": max((z - a for a, z in gaps), default=0) / 1000,
                **_status(status, p0, p1),
                "alignment": ctx.alignment, "source_logs": ctx.source_logs,
            }
        )  # fmt: skip
    return rows


def _slot_rows(
    ctx: _Context, hoot: HootInput, health: HootHealth, offset: int
) -> list[dict[str, Any]]:
    assert ctx.robot is not None
    slots = {s.id: s for s in ctx.robot.slots}
    rows = []
    for phase, (p0, p1) in ctx.periods.items():
        for device in health.devices:
            slot_id = ctx.slot_of(hoot.bus, device)
            if slot_id is None:
                continue
            for unit, u0, u1 in ctx.ranges[slot_id]:
                span = (max(p0, u0) - offset, min(p1, u1) - offset)  # on the hoot clock
                if span[1] <= span[0]:
                    continue
                gaps = [g for g in health.gaps if g.device == device and not g.bus_wide
                        and span[0] <= g.start_us < span[1]]  # fmt: skip
                reboots = [r for r in health.reboots
                           if r.device == device and span[0] <= r.t_us < span[1]]  # fmt: skip
                enabled_min = sum(_overlap(w, span) for w in health.counted) / 60 / US_PER_S
                missed = sum(g.missed for g in gaps)
                rows.append(
                    {
                        **ctx.base(), "phase": phase, "slot_id": slot_id, "unit_id": unit,
                        "subsystem": slots[slot_id].subsystem, "role": slots[slot_id].role,
                        "bus": hoot.bus, "reboots": len(reboots),
                        "reboots_enabled": sum(r.enabled for r in reboots),
                        "isolated_gaps": len(gaps),
                        "missed_per_min": missed / enabled_min if enabled_min else None,
                        "longest_gap_ms": max((g.end_us - g.start_us for g in gaps), default=0)
                        / 1000,
                        "reset_count_logged": health.reset_count_logged,
                        "alignment": ctx.alignment, "source_logs": ctx.source_logs,
                    }
                )  # fmt: skip
    return rows


def session_can_health(
    con: duckdb.DuckDBPyConnection,
    lake: LakePaths,
    session: dict[str, Any],
    framing: Framing,
    robot: RobotConfig | None,
    hoots: list[HootInput],
    observations: list[Observation],
    alignment: str,
    source_logs: str,
) -> SessionCanHealth:
    """Events for every hoot; slot and bus rows per match phase for the aligned hoots of a
    framed, match-keyed session."""
    ctx = _Context(session, framing, robot, observations, alignment, source_logs)
    result = SessionCanHealth()
    for hoot in sorted(hoots, key=lambda h: h.log_id):
        health = analyse_hoot(con, lake, hoot.log_id)
        result.events += _events(ctx, hoot, health)
        if hoot.offset_us is None or not ctx.periods:
            continue
        result.buses += _bus_rows(ctx, con, lake, hoot, health, hoot.offset_us)
        if robot is not None:
            result.slots += _slot_rows(ctx, hoot, health, hoot.offset_us)
    result.slots.sort(key=lambda r: (r["phase"], r["slot_id"], r["unit_id"] or ""))
    result.buses.sort(key=lambda r: (r["phase"], r["bus"]))
    result.events.sort(key=lambda e: (e["hoot_t_us"], e["log_id"], e["kind"], e["device"] or ""))
    return result
