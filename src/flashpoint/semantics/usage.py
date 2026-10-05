"""Gold unit usage: one row per (session, slot, unit) for every session with silver.

Odometry (powered-on hours, energy, stall time, thermal cycles) must count practice and
testing too, and gold match features only exist for match-keyed sessions.
"""

from collections.abc import Sequence
from typing import Any

import duckdb
import polars as pl

from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.framing import Framing
from flashpoint.semantics.physics import DEFAULT_CONFIG, US_PER_S, PhysicsConfig, motor_features
from flashpoint.semantics.robot_config import RobotConfig
from flashpoint.semantics.silver import silver_dir

TABLE = "unit_usage"
_MOTOR_METRICS = (
    "supply_current",
    "supply_voltage",
    "stator_current",
    "motor_voltage",
    "rotor_velocity_rps",
    "temp_c",
    "motor_kv_rpm_per_v",
    "stall_current",
)
_END_OF_TIME = 2**63 - 1
SCHEMA: dict[str, pl.DataType] = {
    "session_id": pl.String(),
    "season": pl.String(),
    "robot": pl.String(),
    "match_key": pl.String(),
    "alignment": pl.String(),
    "session_start": pl.String(),
    "slot_id": pl.String(),
    "unit_id": pl.String(),
    "subsystem": pl.String(),
    "role": pl.String(),
    "powered_s": pl.Float64(),
    "enabled_s": pl.Float64(),
    "supply_energy_wh": pl.Float64(),
    "motor_energy_wh": pl.Float64(),
    "stall_s": pl.Float64(),
    "thermal_cycles": pl.Int64(),
    "temp_max_c": pl.Float64(),
    "flags": pl.String(),
}


def thermal_cycles(
    temps: Sequence[float],
    rise_c: float = DEFAULT_CONFIG.thermal_rise_c,
    fall_c: float = DEFAULT_CONFIG.thermal_fall_c,
) -> int | None:
    """Hysteresis count over temperatures in time order (None: no temperature logged).

    A cycle arms once the reading is `rise_c` above the lowest reading since the last cycle,
    and counts when it falls `fall_c` below its peak, or when the session ends while armed
    (a robot powered off hot still did the work).
    """
    if not temps:
        return None
    count, low, peak = 0, temps[0], None
    for value in temps:
        if peak is None:
            low = min(low, value)
            if value >= low + rise_c:
                peak = value
        else:
            peak = max(peak, value)
            if value <= peak - fall_c:
                count, low, peak = count + 1, value, None
    return count + (peak is not None)


def _quote(path: object) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _powered(
    con: duckdb.DuckDBPyConnection, source: str, framing: Framing, cap_us: int
) -> list[tuple[str, str, int, int, int, int]]:
    """(slot, unit, powered µs, enabled µs, first t, last t) per unit.

    Each sample holds until the slot's next sample (capped), so a mid-session swap credits
    each unit with only its own span and the two spans sum to the slot's span.
    """
    con.register(
        "enabled_runs",
        pl.DataFrame(
            {
                "start_us": [s for s, _ in framing.enabled],
                "end_us": [e if e is not None else _END_OF_TIME for _, e in framing.enabled],
            },
            schema={"start_us": pl.Int64, "end_us": pl.Int64},
        ),
    )
    rows = con.execute(
        f"""
        WITH s AS NOT MATERIALIZED (SELECT slot_id, unit_id, metric, t_us FROM {source}),
        with_volts AS (SELECT DISTINCT slot_id FROM s WHERE metric = 'supply_voltage'),
        ts AS (
            SELECT slot_id, unit_id, t_us FROM s WHERE metric = 'supply_voltage'
            UNION
            SELECT slot_id, unit_id, t_us FROM s
            WHERE slot_id NOT IN (SELECT slot_id FROM with_volts)
        ),
        held AS (
            SELECT slot_id, unit_id, t_us, least(coalesce(
                lead(t_us) OVER (PARTITION BY slot_id ORDER BY t_us) - t_us, 0), ?) AS w
            FROM ts
        ),
        enabled AS (
            SELECT h.slot_id, h.unit_id, sum(greatest(0,
                least(h.t_us + h.w, e.end_us) - greatest(h.t_us, e.start_us))) AS enabled_us
            FROM held h JOIN enabled_runs e ON e.start_us < h.t_us + h.w AND e.end_us > h.t_us
            WHERE h.unit_id IS NOT NULL
            GROUP BY ALL
        ),
        powered AS (
            SELECT slot_id, unit_id, sum(w) AS powered_us
            FROM held WHERE unit_id IS NOT NULL GROUP BY ALL
        ),
        span AS (
            SELECT slot_id, unit_id, min(t_us) AS first_us, max(t_us) AS last_us
            FROM s WHERE unit_id IS NOT NULL GROUP BY ALL
        )
        SELECT p.slot_id, p.unit_id, p.powered_us::BIGINT, coalesce(e.enabled_us, 0)::BIGINT,
            sp.first_us, sp.last_us
        FROM powered p JOIN span sp USING (slot_id, unit_id)
        LEFT JOIN enabled e USING (slot_id, unit_id)
        ORDER BY p.slot_id, p.unit_id
        """,  # noqa: S608 - internal path
        [cap_us],
    ).fetchall()
    con.unregister("enabled_runs")
    return rows


def session_usage(
    con: duckdb.DuckDBPyConnection,
    lake: LakePaths,
    session: dict[str, Any],
    framing: Framing,
    robot: RobotConfig,
    alignment: str,
    session_start: str | None,
    config: PhysicsConfig = DEFAULT_CONFIG,
) -> list[dict[str, Any]]:
    path = silver_dir(lake) / f"season={session['season']}" / f"session_id={session['session_id']}"
    if not path.is_dir():
        return []
    source = f"read_parquet({_quote(path / '*.parquet')})"
    slots = {s.id: s for s in robot.slots}
    names = ", ".join(f"'{m}'" for m in _MOTOR_METRICS)
    units = _powered(con, source, framing, config.hold_cap_us)
    rows = []
    for slot_id, unit_id, powered_us, enabled_us, first_us, last_us in units:
        # One unit at a time keeps memory bounded by a unit's samples, not the session's.
        # (execute() with parameters: a parameterised con.sql() relation is ~25x slower here.)
        samples = con.execute(
            f"SELECT metric, t_us, value FROM {source}"  # noqa: S608
            f" WHERE slot_id = ? AND unit_id = ? AND metric IN ({names})",
            [slot_id, unit_id],
        ).pl()
        features = motor_features(samples, first_us, last_us + 1, config)
        temps = samples.filter(pl.col("metric") == "temp_c").sort("t_us").get_column("value")
        flags = [f for f in features["flags"].split(",") if f]
        if temps.is_empty():
            flags.append("no-temperature")
        slot = slots.get(slot_id)
        rows.append(
            {
                "session_id": session["session_id"],
                "season": session["season"],
                "robot": session["robot"],
                "match_key": session["match_key"],
                "alignment": alignment,
                "session_start": session_start,
                "slot_id": slot_id,
                "unit_id": unit_id,
                "subsystem": slot.subsystem if slot else None,
                "role": slot.role if slot else None,
                "powered_s": powered_us / US_PER_S,
                "enabled_s": enabled_us / US_PER_S,
                "supply_energy_wh": features["supply_energy_wh"],
                "motor_energy_wh": features["motor_energy_wh"],
                "stall_s": features["stall_s"],
                "thermal_cycles": thermal_cycles(
                    temps.to_list(), config.thermal_rise_c, config.thermal_fall_c
                ),
                "temp_max_c": features["temp_max_c"],
                "flags": ",".join(flags),
            }
        )
    return rows
