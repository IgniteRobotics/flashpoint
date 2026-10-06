"""History queries over the lake: parameterised DuckDB SQL, read-only, testable without HTTP.

Reads gold (`match_features`, `unit_usage`) and the meta Parquet snapshots only; it never
opens the SQLite ledger. Every filter value binds as a `?` parameter, and metric names map to
columns through a fixed whitelist, so request values are always data, never query text.
"""

import difflib
import threading
from collections.abc import Iterable
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

import duckdb

from flashpoint.lake.ledger import INCOMPLETE_READ
from flashpoint.lake.paths import LakePaths
from flashpoint.report.names import download_name
from flashpoint.report.settings import ReportConfig
from flashpoint.semantics.robot_config import RobotConfig


class QueryError(ValueError):
    """A request the queries refuse (unknown metric, phase, or grouping)."""


@dataclass(frozen=True)
class Metric:
    column: str
    label: str
    unit: str
    reference: str | None = None  # name of the ReportConfig limit drawn as a reference line


METRICS = {
    "peak_temp": Metric("temp_max_c", "Peak temperature", "°C", "temp_warn_c"),
    "min_voltage": Metric("supply_voltage_min", "Lowest supply voltage", "V", "voltage_danger_v"),
    "supply_wh": Metric("supply_energy_wh", "Supply energy", "Wh"),
    "stall_s": Metric("stall_s", "Stall time", "s"),
    "current_p95": Metric("supply_current_p95", "Supply current P95", "A"),
}
PHASES = ("match", "auto", "teleop")
LOW_ALIGNMENT = ("low", "very-low", "none")

_FEATURE_COLUMNS = {
    "match_key": "VARCHAR",
    "session_id": "VARCHAR",
    "robot": "VARCHAR",
    "phase": "VARCHAR",
    "slot_id": "VARCHAR",
    "unit_id": "VARCHAR",
    "subsystem": "VARCHAR",
    "role": "VARCHAR",
    "alignment": "VARCHAR",
    **{m.column: "DOUBLE" for m in METRICS.values()},
}
_USAGE_COLUMNS = {
    "session_id": "VARCHAR",
    "robot": "VARCHAR",
    "match_key": "VARCHAR",
    "session_start": "VARCHAR",
    "slot_id": "VARCHAR",
    "unit_id": "VARCHAR",
    "subsystem": "VARCHAR",
    "role": "VARCHAR",
    "powered_s": "DOUBLE",
    "enabled_s": "DOUBLE",
    "supply_energy_wh": "DOUBLE",
    "motor_energy_wh": "DOUBLE",
    "stall_s": "DOUBLE",
    "thermal_cycles": "BIGINT",
    "temp_max_c": "DOUBLE",
}
_META_COLUMNS = {
    "sessions": {
        "session_id": "VARCHAR",
        "wpilog_id": "VARCHAR",
        "robot": "VARCHAR",
        "match_key": "VARCHAR",
    },
    "logs": {"log_id": "VARCHAR", "kind": "VARCHAR", "filename": "VARCHAR", "utc_start": "VARCHAR"},
    "slot_observations": {
        "session_id": "VARCHAR",
        "slot_id": "VARCHAR",
        "unit_id": "VARCHAR",
        "source": "VARCHAR",
        "from_ts_us": "BIGINT",
    },
    "session_hoots": {"log_id": "VARCHAR", "session_id": "VARCHAR", "bus": "VARCHAR"},
    "files": {"sha256": "VARCHAR", "kind": "VARCHAR", "size": "BIGINT", "stage": "VARCHAR"},
}


@dataclass(frozen=True)
class Filters:
    season: str | None = None  # None: all time
    robot: str | None = None
    event: str | None = None  # e.g. 2026gacmp
    match_type: str | None = None  # pm | qm | e
    phase: str = "match"
    subsystem: str | None = None
    slot: str | None = None
    unit: str | None = None

    @classmethod
    def names(cls) -> set[str]:
        return {f.name for f in fields(cls)}


# Filters that pick matches (the x axis); the rest pick series within them.
_MATCH_SCOPE = ("season", "robot", "event", "match_type")


def _where(filters: Filters, keys: Iterable[str], table: str) -> tuple[str, list[Any]]:
    column = {
        "season": f"{table}.season",
        "robot": f"{table}.robot",
        "event": f"{table}.event",
        "match_type": f"{table}.match_type",
        "phase": f"{table}.phase",
        "subsystem": f"{table}.subsystem",
        "slot": f"{table}.slot_id",
        "unit": f"{table}.unit_id",
    }
    clauses, params = [], []
    for key in keys:
        value = getattr(filters, key)
        if value is not None:
            clauses.append(f"{column[key]} = ?")
            params.append(value)
    return (" AND ".join(clauses) or "true"), params


def _quote(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def serial_of(unit_id: str) -> str:
    return unit_id.removeprefix("ctre:") if unit_id.startswith("ctre:") else unit_id


class HistoryQueries:
    """Lazily opens an in-memory DuckDB over the lake's gold and meta Parquet."""

    def __init__(
        self,
        lake: LakePaths,
        robots: list[RobotConfig] | None = None,
        report: ReportConfig | None = None,
    ) -> None:
        self.lake = lake
        self.robots = {r.robot: r for r in robots or []}
        self.report = report or ReportConfig()
        self.last_sql = ""  # the most recent statement issued (tests check its predicates)
        self._con: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.Lock()

    # --- connection ----------------------------------------------------------------------

    def _view(self, con: duckdb.DuckDBPyConnection, name: str, glob: Path | None,
              files: list[Path], columns: dict[str, str], hive: bool) -> None:  # fmt: skip
        if files:
            source = (
                f"read_parquet({_quote(glob)}, hive_partitioning = true,"
                " hive_types_autocast = false, union_by_name = true)"
                if hive and glob is not None
                else f"read_parquet({_quote(files[0])})"
            )
            present = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()}
            select = ", ".join(
                f'"{c}"' if c in present else f'NULL::{t} AS "{c}"' for c, t in columns.items()
            )
            extra = ", season" if hive else ""
            con.execute(f"CREATE VIEW {name} AS SELECT {select}{extra} FROM {source}")
        else:
            select = ", ".join(f'NULL::{t} AS "{c}"' for c, t in columns.items())
            extra = ", NULL::VARCHAR AS season" if hive else ""
            con.execute(f"CREATE VIEW {name} AS SELECT {select}{extra} WHERE false")

    def _connect(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.execute("SET memory_limit = '256MB'")
        gold = self.lake.root / "gold"
        for name, table, columns in (
            ("features_raw", "match_features", _FEATURE_COLUMNS),
            ("usage_raw", "unit_usage", _USAGE_COLUMNS),
        ):
            glob = gold / table / "season=*" / "session_id=*" / "*.parquet"
            files = sorted((gold / table).glob("season=*/session_id=*/*.parquet"))
            self._view(con, name, glob, files, columns, hive=True)
        for name, columns in _META_COLUMNS.items():
            path = self.lake.meta / f"{name}.parquet"
            self._view(con, f"meta_{name}", None, [path] if path.is_file() else [], columns, False)
        # One row per session: its time (wpilog start, else the hoot stamp) and match parts.
        con.execute(
            r"""
            CREATE VIEW session_times AS
            SELECT s.session_id, s.robot, s.match_key,
                split_part(s.match_key, '_', 1) AS event,
                nullif(regexp_extract(s.match_key, '_([a-z]+)\d', 1), '') AS match_type,
                coalesce(
                    l.utc_start,
                    strftime(try_strptime(substr(s.session_id, 6, 19), '%Y-%m-%d_%H-%M-%S'),
                             '%Y-%m-%dT%H:%M:%S+00:00')
                ) AS t
            FROM meta_sessions s LEFT JOIN meta_logs l ON l.log_id = s.wpilog_id
            """
        )
        for name, raw in (("features", "features_raw"), ("usage", "usage_raw")):
            con.execute(
                f"""
                CREATE VIEW {name} AS
                SELECT r.*, st.event, st.match_type, st.t
                FROM {raw} r LEFT JOIN session_times st USING (session_id)
                """  # noqa: S608 - fixed view names
            )
        return con

    def _rows(self, sql: str, params: list[Any]) -> list[dict[str, Any]]:
        with self._lock:
            if self._con is None:
                self._con = self._connect()
            self.last_sql = sql
            cursor = self._con.execute(sql, params)
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def close(self) -> None:
        with self._lock:
            if self._con is not None:
                self._con.close()
                self._con = None

    @property
    def is_open(self) -> bool:
        return self._con is not None

    # --- validation ----------------------------------------------------------------------

    @staticmethod
    def metric(name: str) -> Metric:
        if name not in METRICS:
            raise QueryError(f"unknown metric {name!r}; one of {sorted(METRICS)}")
        return METRICS[name]

    @staticmethod
    def _check(filters: Filters) -> None:
        if filters.phase not in PHASES:
            raise QueryError(f"unknown phase {filters.phase!r}; one of {list(PHASES)}")

    def reference(self, metric: Metric) -> float | None:
        return getattr(self.report, metric.reference) if metric.reference else None

    # --- queries -------------------------------------------------------------------------

    def is_empty(self) -> bool:
        return not self._rows("SELECT 1 FROM features LIMIT 1", [])

    def filter_values(self) -> dict[str, Any]:
        rows = self._rows(
            "SELECT DISTINCT season, robot, event, match_type, phase, subsystem, slot_id, unit_id"
            " FROM features",
            [],
        )

        def values(key: str) -> list[Any]:
            return sorted({r[key] for r in rows if r[key] is not None})

        return {
            "season": values("season"),
            "robot": values("robot"),
            "event": values("event"),
            "match_type": values("match_type"),
            "phase": [p for p in PHASES if p in values("phase")] or list(PHASES),
            "subsystem": values("subsystem"),
            "slot": values("slot_id"),
            "unit": values("unit_id"),
            "metric": [{"id": k, "label": m.label, "unit": m.unit} for k, m in METRICS.items()],
        }

    def matches(self, filters: Filters) -> list[dict[str, Any]]:
        """The x axis: every match in range, in time order."""
        self._check(filters)
        where, params = _where(filters, (*_MATCH_SCOPE, "phase"), "f")
        return self._rows(
            f"SELECT DISTINCT f.match_key, f.t, f.event, f.robot FROM features f"  # noqa: S608
            f" WHERE {where} ORDER BY f.t, f.match_key",
            params,
        )

    def trend(self, metric_name: str, by: str, filters: Filters) -> dict[str, Any]:
        metric = self.metric(metric_name)
        self._check(filters)
        if by not in ("unit", "slot"):
            raise QueryError(f"unknown grouping {by!r}; one of ['unit', 'slot']")
        series = "f.unit_id" if by == "unit" else "f.robot || '/' || f.slot_id"
        matches = self.matches(filters)
        where, params = _where(filters, Filters.names(), "f")
        points = self._rows(
            f"""
            SELECT {series} AS series, f.match_key, f.t, f.{metric.column} AS value,
                f.alignment, f.robot, f.slot_id, f.role, f.unit_id
            FROM features f
            WHERE {where}
            ORDER BY series, f.t, f.match_key
            """,  # noqa: S608 - series and column come from fixed whitelists
            params,
        )
        previous: dict[str, tuple[str, str]] = {}
        for point in points:
            place = (point["robot"], point["slot_id"])
            point["slot_changed"] = (
                point["series"] in previous and previous[point["series"]] != place
            )
            previous[point["series"]] = place
            point["low_alignment"] = point["alignment"] in LOW_ALIGNMENT
        return {
            "metric": metric_name,
            "label": metric.label,
            "unit": metric.unit,
            "reference": self.reference(metric),
            "by": by,
            "matches": matches,
            "points": points,
        }

    def _health(self, temp_max: float | None) -> str:
        if temp_max is None:
            return "NO-DATA"
        if temp_max >= self.report.temp_fault_c:
            return "HOT"
        if temp_max >= self.report.temp_warn_c:
            return "WARN"
        return "OK"

    def devices(self, metric_name: str, filters: Filters) -> list[dict[str, Any]]:
        """One row per unit in range, by unit id."""
        metric = self.metric(metric_name)
        self._check(filters)
        where, params = _where(filters, Filters.names(), "f")
        rows = self._rows(
            f"""
            SELECT f.unit_id,
                arg_max(f.slot_id, f.t) AS slot_id, arg_max(f.role, f.t) AS role,
                arg_max(f.robot, f.t) AS robot, arg_max(f.subsystem, f.t) AS subsystem,
                min(f.t) AS first_t, max(f.t) AS last_t, count(DISTINCT f.match_key) AS matches,
                max(f.temp_max_c) AS temp_max_c,
                list(f.{metric.column} ORDER BY f.t, f.match_key) AS spark
            FROM features f
            WHERE {where}
            GROUP BY f.unit_id
            ORDER BY f.unit_id
            """,  # noqa: S608 - column from the metric whitelist
            params,
        )
        for row in rows:
            values = [v for v in row["spark"] if v is not None]
            row["serial"] = serial_of(row["unit_id"])
            row["latest"] = values[-1] if values else None
            row["change"] = values[-1] - values[0] if len(values) >= 2 else None
            row["health"] = self._health(row["temp_max_c"])
        return rows

    def summary(self, filters: Filters) -> dict[str, Any]:
        self._check(filters)
        where, params = _where(filters, Filters.names(), "f")
        counts = self._rows(
            f"SELECT count(DISTINCT f.unit_id) AS units, count(DISTINCT f.match_key) AS matches"
            f" FROM features f WHERE {where}",  # noqa: S608
            params,
        )[0]
        usage_keys = [k for k in Filters.names() if k != "phase"]
        uwhere, uparams = _where(filters, usage_keys, "u")
        hours = self._rows(
            f"SELECT coalesce(sum(u.powered_s), 0) / 3600 AS powered_h FROM usage u"  # noqa: S608
            f" WHERE {uwhere}",
            uparams,
        )[0]["powered_h"]
        health = self._rows(
            f"SELECT f.unit_id, max(f.temp_max_c) AS temp_max_c FROM features f"  # noqa: S608
            f" WHERE {where} GROUP BY f.unit_id",
            params,
        )
        flagged = [r for r in health if self._health(r["temp_max_c"]) in ("WARN", "HOT")]
        return {
            "units": counts["units"],
            "matches": counts["matches"],
            "powered_h": hours,
            "units_warn_hot": len(flagged),
        }

    # --- raw logs ------------------------------------------------------------------------

    def sources(self, match_key: str) -> list[dict[str, Any]]:
        """Raw logs of every session with this match key (for AdvantageScope downloads)."""
        rows = self._rows(
            """
            WITH ids AS (
                SELECT s.session_id, s.wpilog_id AS log_id, 'wpilog' AS part
                FROM meta_sessions s WHERE s.match_key = ? AND s.wpilog_id IS NOT NULL
                UNION ALL
                SELECT s.session_id, h.log_id, coalesce(h.bus, 'hoot') AS part
                FROM meta_sessions s JOIN meta_session_hoots h USING (session_id)
                WHERE s.match_key = ?
            )
            SELECT ids.session_id, ids.log_id AS sha256, ids.part,
                coalesce(l.kind, f.kind) AS kind, l.filename AS name, f.size, f.stage
            FROM ids LEFT JOIN meta_logs l USING (log_id)
            LEFT JOIN meta_files f ON f.sha256 = ids.log_id
            ORDER BY ids.session_id, ids.part = 'wpilog' DESC, ids.log_id
            """,
            [match_key, match_key],
        )
        for row in rows:
            row["kind"] = row["kind"] or "hoot"
            row["name"] = row["name"] or f"{row['sha256']}.{row['kind']}"
            row["download"] = download_name(match_key, row["part"], row["name"])
            row["incomplete"] = row.pop("stage") == INCOMPLETE_READ
        return rows

    def raw_hashes(self) -> set[str]:
        """Every file hash the ledger snapshot knows (the only raw files served)."""
        return {r["sha256"] for r in self._rows("SELECT sha256 FROM meta_files", [])}

    def raw_kind(self, sha256: str) -> str | None:
        rows = self._rows("SELECT kind FROM meta_files WHERE sha256 = ?", [sha256])
        return rows[0]["kind"] if rows else None

    # --- units ---------------------------------------------------------------------------

    def unit_ids(self) -> list[str]:
        rows = self._rows(
            "SELECT unit_id FROM meta_slot_observations UNION SELECT unit_id FROM usage"
            " UNION SELECT unit_id FROM features",
            [],
        )
        return sorted(r["unit_id"] for r in rows if r["unit_id"] is not None)

    def closest_units(self, unit_id: str, limit: int = 5) -> list[str]:
        known = self.unit_ids()
        needle = unit_id.lower()
        contains = [u for u in known if needle and needle in u.lower()]
        close = difflib.get_close_matches(unit_id, known, n=limit, cutoff=0.4)
        return list(dict.fromkeys([*contains, *close]))[:limit]

    def _slot_info(self, robot: str | None, slot_id: str | None) -> dict[str, Any]:
        config = self.robots.get(robot or "")
        slot = next((s for s in config.slots if s.id == slot_id), None) if config else None
        return {
            "robot": robot,
            "slot_id": slot_id,
            "role": slot.role if slot else None,
            "bus": slot.bus if slot else None,
            "can_id": slot.can_id if slot else None,
            "model": slot.model if slot else None,
        }

    def unit(
        self, unit_id: str, season: str | None = None, robot: str | None = None
    ) -> dict[str, Any]:
        observed = self._rows(
            """
            SELECT o.session_id, o.slot_id, o.unit_id, o.source, o.from_ts_us,
                st.robot, st.t, st.match_key
            FROM meta_slot_observations o JOIN session_times st USING (session_id)
            ORDER BY st.t, o.session_id, coalesce(o.from_ts_us, -1)
            """,
            [],
        )
        mine = [o for o in observed if o["unit_id"] == unit_id]
        usage_where, usage_params = _where(
            Filters(season=season, robot=robot, unit=unit_id), ("season", "robot", "unit"), "u"
        )
        totals = self._rows(
            f"""
            SELECT count(*) AS usage_rows,
                sum(u.powered_s) / 3600 AS powered_h, sum(u.enabled_s) / 3600 AS enabled_h,
                sum(u.supply_energy_wh) AS supply_wh, sum(u.motor_energy_wh) AS motor_wh,
                sum(u.thermal_cycles) AS thermal_cycles, sum(u.stall_s) AS stall_s,
                max(u.temp_max_c) AS temp_max_c, count(DISTINCT u.session_id) AS sessions,
                count(DISTINCT u.match_key) AS matches,
                min(coalesce(u.t, u.session_start)) AS first_seen,
                max(coalesce(u.t, u.session_start)) AS last_seen
            FROM usage u WHERE {usage_where}
            """,  # noqa: S608
            usage_params,
        )[0]
        if not mine and not totals["usage_rows"]:
            return {"found": False, "unit_id": unit_id, "suggestions": self.closest_units(unit_id)}
        aligned = {
            r["session_id"]
            for r in self._rows(
                "SELECT DISTINCT session_id FROM usage WHERE unit_id = ?", [unit_id]
            )
        }
        feature_temp = self._rows(
            "SELECT max(temp_max_c) AS t FROM features WHERE unit_id = ?", [unit_id]
        )[0]["t"]
        latest = mine[-1] if mine else None
        info = self._slot_info(latest["robot"], latest["slot_id"]) if latest else {}
        return {
            "found": True,
            "unit_id": unit_id,
            "serial": serial_of(unit_id),
            "identity": "legacy" if unit_id.startswith("legacy:") else "inventory",
            "model": info.get("model"),
            "current": info,
            "health": self._health(feature_temp),
            "totals": {k: v for k, v in totals.items() if k != "usage_rows"},
            "sessions_without_aligned_samples": len({o["session_id"] for o in mine} - aligned),
            "lifeline": self._lifeline(unit_id, observed),
        }

    def _lifeline(self, unit_id: str, observed: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Events newest first: first seen, moved, replaced by, last seen."""
        stints: list[dict[str, Any]] = []
        for index, o in enumerate(observed):
            if o["unit_id"] != unit_id:
                continue
            place = (o["robot"], o["slot_id"])
            if stints and stints[-1]["place"] == place:
                stints[-1]["last"] = o
                stints[-1]["last_index"] = index
            else:
                stints.append({"place": place, "first": o, "last": o, "last_index": index})
        events = []

        def event(kind: str, o: dict[str, Any], other: str | None = None) -> dict[str, Any]:
            return {
                "kind": kind,
                "t": o["t"],
                "session_id": o["session_id"],
                "match_key": o["match_key"],
                "source": o["source"],
                "other_unit": other,
                **self._slot_info(o["robot"], o["slot_id"]),
            }

        for i, stint in enumerate(stints):
            events.append(event("first-seen" if i == 0 else "moved", stint["first"]))
        if stints:
            last = stints[-1]
            events.append(event("last-seen", last["last"]))
            replacement = next(
                (
                    o
                    for o in observed[last["last_index"] + 1 :]
                    if (o["robot"], o["slot_id"]) == last["place"] and o["unit_id"] != unit_id
                ),
                None,
            )
            if replacement is not None:
                events.append(event("replaced-by", replacement, replacement["unit_id"]))
        return list(reversed(events))
