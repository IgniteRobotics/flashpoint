"""Derived layers from bronze + configuration: sessions, alignment, identity, framing, silver, gold.

Everything here is rebuildable without raw logs or network access.
"""

import hashlib
import json
import re
import shutil
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from flashpoint import config as fp_config
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.query import connect
from flashpoint.meta.extract import parse_hoot_filename
from flashpoint.semantics import gold
from flashpoint.semantics.alignment import (
    HOOT_ENABLE,
    WPILOG_ENABLE,
    Alignment,
    align_by_enable_edges,
    align_by_payload,
    hoot_bus_agreement_us,
)
from flashpoint.semantics.framing import Framing, frame, modes_from_ds
from flashpoint.semantics.identity import (
    DeviceSighting,
    InventoryEvent,
    Observation,
    resolve_identity,
)
from flashpoint.semantics.match_identity import identify
from flashpoint.semantics.robot_config import RobotConfig, load_robots, select_robot
from flashpoint.semantics.sessions import HootGroup, HootLog, Session, WpilogLog, group_sessions
from flashpoint.semantics.silver import HootInSession, write_session

DUCKDB_MEMORY_LIMIT = "512MB"
_SAMPLE_COLUMNS = "signal::VARCHAR AS signal, type::VARCHAR AS type, ts_us, v_f64, v_bytes, v_bool"


@dataclass
class GroupAlignment:
    group: HootGroup
    reference: str | None  # log_id the offset was measured on
    alignment: Alignment | None
    bus_agreement_us: dict[str, int | None]


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class Deriver:
    def __init__(self, lake: LakePaths, config_dir: Path) -> None:
        self.lake = lake
        self.ledger = Ledger(lake.ledger)
        self.robots: list[RobotConfig] = (
            load_robots(config_dir / "robots") if (config_dir / "robots").is_dir() else []
        )
        self._con: duckdb.DuckDBPyConnection | None = None

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            self._con = connect(self.lake)
            # Bounded memory: DuckDB defaults to 80% of RAM; spill large sorts/joins to the lake.
            spill = self.lake.root / "tmp" / "duckdb"
            spill.mkdir(parents=True, exist_ok=True)
            self._con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
            self._con.execute(f"SET temp_directory = '{spill}'")
            self._con.execute("SET preserve_insertion_order = false")
        return self._con

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
        self.ledger.export_snapshots(self.lake.meta)
        self.ledger.close()

    # --- loading -------------------------------------------------------------------------

    def _logs(self) -> tuple[list[WpilogLog], list[HootLog], dict[str, dict[str, Any]]]:
        rows = self.ledger.query(
            "SELECT l.*, h.bus, h.session_stamp FROM logs l JOIN files f ON f.sha256 = l.log_id"
            " LEFT JOIN hoot_logs h USING (log_id) WHERE f.stage = 'success'"
        )
        by_id = {r["log_id"]: r for r in rows}
        wpilogs = [
            WpilogLog(
                r["log_id"],
                _dt(r["utc_start"]),
                _dt(r["utc_end"]),
                r["file_event"],
                f"{r['file_match_type']}{r['file_match_number']}" if r["file_match_type"] else None,
            )
            for r in rows
            if r["kind"] == "wpilog"
        ]
        hoots = []
        for r in rows:
            if r["kind"] == "hoot" and r["session_stamp"]:
                parsed = parse_hoot_filename(r["filename"] or "")
                hoots.append(
                    HootLog(r["log_id"], r["session_stamp"], r["bus"], parsed.event, parsed.match)
                )
        return wpilogs, hoots, by_id

    def _samples(self, log_id: str, where: str) -> pl.DataFrame:
        return self.con.sql(
            f"SELECT {_SAMPLE_COLUMNS} FROM samples WHERE log_id = ? AND ({where})",  # noqa: S608
            params=[log_id],
        ).pl()

    def _anchor_signals(self, log_id: str) -> list[str]:
        rows = self.ledger.query(
            "SELECT DISTINCT name FROM entries WHERE log_id = ? AND name NOT LIKE 'Phoenix6/%'"
            " AND name NOT LIKE '/.schema/%' AND type LIKE 'struct:%'",
            (log_id,),
        )
        return [r["name"] for r in rows]

    # --- alignment -----------------------------------------------------------------------

    def align_group(
        self, wpilog_id: str | None, group: HootGroup, logs: dict[str, dict[str, Any]]
    ) -> GroupAlignment:
        enable = f"signal = '{HOOT_ENABLE}'"
        hoot_frames = {log_id: self._samples(log_id, enable) for log_id in group.log_ids}
        best: tuple[str | None, Alignment | None] = (None, None)
        if wpilog_id is not None:
            wpi_enable = self._samples(wpilog_id, f"signal = '{WPILOG_ENABLE}'")
            for log_id in group.log_ids:
                anchors = self._anchor_signals(log_id)
                if not anchors:
                    continue
                names = ", ".join("'" + a.replace("'", "''") + "'" for a in anchors)
                nt_names = ", ".join("'NT:/" + a.replace("'", "''") + "'" for a in anchors)
                hoot = self._samples(log_id, f"signal IN ({names})")
                wpi = self._samples(wpilog_id, f"signal IN ({nt_names}) OR signal IN ({names})")
                found = align_by_payload(wpi, hoot)
                if found and (best[1] is None or found.matches > best[1].matches):
                    best = (log_id, found)
            if best[1] is None:
                for log_id in group.log_ids:
                    wall = self._wall_clock(wpilog_id, log_id, group, logs)
                    found = align_by_enable_edges(
                        wpi_enable, hoot_frames[log_id], wall.offset_us if wall else None
                    )
                    if found:
                        best = (log_id, found)
                        break
            if best[1] is None:
                best = (
                    group.log_ids[0],
                    self._wall_clock(wpilog_id, group.log_ids[0], group, logs),
                )
        reference = best[0]
        agreement = {
            log_id: (
                0
                if log_id == reference or reference is None
                else hoot_bus_agreement_us(hoot_frames[reference], hoot_frames[log_id])
            )
            for log_id in group.log_ids
        }
        return GroupAlignment(group, reference, best[1], agreement)

    @staticmethod
    def _wall_clock(
        wpilog_id: str, hoot_id: str, group: HootGroup, logs: dict[str, dict[str, Any]]
    ) -> Alignment | None:
        wpi = logs[wpilog_id]
        if wpi["utc_offset_us"] is None or logs[hoot_id]["first_ts_us"] is None:
            return None
        stamp_us = int(group.start.timestamp() * 1_000_000)
        offset = stamp_us - logs[hoot_id]["first_ts_us"] - wpi["utc_offset_us"]
        return Alignment(offset, "wall-clock", "very-low", None, 0)

    # --- sessions ------------------------------------------------------------------------

    def derive_sessions(self) -> tuple[list[Session], dict[str, GroupAlignment]]:
        wpilogs, hoots, logs = self._logs()
        sessions = group_sessions(wpilogs, hoots)
        session_rows, hoot_rows = [], []
        aligned: dict[str, GroupAlignment] = {}
        for session in sessions:
            w = logs.get(session.wpilog_id or "")
            season = (
                w["season"]
                if w
                else (session.hoot_groups[0].stamp[:4] if session.hoot_groups else "unknown")
            )
            robot = select_robot(self.robots, w["project"] if w else None, season)
            if w:
                ident = identify(
                    season,
                    w["fms_event"],
                    w["fms_match_type"],
                    w["fms_match_number"],
                    w["fms_replay"],
                    w["file_event"],
                    w["file_match_type"],
                    w["file_match_number"],
                )
            else:
                group = session.hoot_groups[0]
                label = group.match or ""
                ident = identify(
                    season,
                    file_event=group.event,
                    file_match_type=label[:1] or None,
                    file_match_number=int(label[1:]) if label[1:].isdigit() else None,
                )
            session_rows.append(
                {
                    "session_id": session.session_id,
                    "wpilog_id": session.wpilog_id,
                    "robot": robot.robot if robot else "unknown",
                    "season": season,
                    "match_key": ident.key,
                    "match_source": ident.source,
                    "kind": ident.kind,
                    "warnings": ",".join(ident.warnings) or None,
                }
            )
            for group in session.hoot_groups:
                result = self.align_group(session.wpilog_id, group, logs)
                aligned[group.stamp + session.session_id] = result
                a = result.alignment
                for log_id, bus in group.buses.items():
                    hoot_rows.append(
                        {
                            "log_id": log_id,
                            "session_id": session.session_id,
                            "hoot_group": group.stamp,
                            "bus": bus,
                            "offset_us": a.offset_us if a else None,
                            "method": a.method if a else None,
                            "confidence": a.confidence if a else None,
                            "spread_us": a.spread_us if a else None,
                            "matches": a.matches if a else None,
                            "bus_agreement_us": result.bus_agreement_us.get(log_id),
                        }
                    )
        self.ledger.replace_rows("sessions", session_rows)
        self.ledger.replace_rows("session_hoots", hoot_rows)
        return sessions, aligned

    # --- device identity -----------------------------------------------------------------

    def _sightings(self, session_id: str) -> list[DeviceSighting]:
        rows = self.ledger.query(
            "SELECT DISTINCT sh.bus, e.name FROM session_hoots sh JOIN entries e USING (log_id)"
            " WHERE sh.session_id = ? AND e.name LIKE 'Phoenix6/%'",
            (session_id,),
        )
        found = set()
        for row in rows:
            match = _DEVICE.match(row["name"])
            if match:
                found.add(DeviceSighting(row["bus"], match["model"], int(match["id"])))
        return sorted(found, key=lambda d: (d.bus, d.model, d.can_id))

    def derive_identity(self) -> None:
        robots = {r.robot: r for r in self.robots}
        observations, swaps, unmapped = [], [], []
        for session in self.ledger.query(
            "SELECT s.*, l.utc_start FROM sessions s LEFT JOIN logs l ON l.log_id = s.wpilog_id"
        ):
            robot = robots.get(session["robot"])
            if robot is None:
                continue
            day = _session_day(session)
            inventories = [
                InventoryEvent(r["ts_us"], r["payload"])
                for r in self.ledger.query(
                    "SELECT ts_us, payload FROM inventory WHERE log_id = ? ORDER BY ts_us",
                    (session["wpilog_id"],),
                )
            ]
            result = resolve_identity(
                session["session_id"],
                robot,
                day,
                self._sightings(session["session_id"]),
                inventories,
            )
            observations += [o.__dict__ for o in result.observations]
            swaps += [
                {
                    "session_id": session["session_id"],
                    "slot_id": s,
                    "ts_us": t,
                    "old_unit": a,
                    "new_unit": b,
                }
                for s, t, a, b in result.swaps
            ]
            unmapped += [
                {
                    "session_id": session["session_id"],
                    "bus": b,
                    "model": m,
                    "can_id": i,
                    "source": src,
                }
                for b, m, i, src in result.unmapped
            ]
        self.ledger.replace_rows("slot_observations", observations)
        self.ledger.replace_rows("unit_swaps", swaps)
        self.ledger.replace_rows("unmapped_devices", unmapped)

    # --- match framing -------------------------------------------------------------------

    def framing_for(self, session_id: str) -> Framing:
        hoots = self.ledger.query(
            "SELECT log_id, offset_us FROM session_hoots"
            " WHERE session_id = ? AND offset_us IS NOT NULL",
            (session_id,),
        )
        transitions: list[tuple[int, str]] = []
        for hoot in hoots:
            modes = self.con.sql(
                "SELECT ts_us, v_str FROM samples"
                " WHERE log_id = ? AND signal = 'RobotMode' ORDER BY ts_us",
                params=[hoot["log_id"]],
            ).fetchall()
            transitions += [(ts + hoot["offset_us"], mode) for ts, mode in modes if mode]
        if transitions:
            return frame(transitions, "hoot-robot-mode")
        session = self.ledger.query(
            "SELECT wpilog_id FROM sessions WHERE session_id = ?", (session_id,)
        )
        wpilog_id = session[0]["wpilog_id"] if session else None
        if wpilog_id is None:
            return frame([], "none")
        ds = self._samples(wpilog_id, "signal IN ('DS:enabled', 'DS:autonomous')")
        enabled = (
            ds.filter(pl.col("signal") == "DS:enabled")
            .sort("ts_us")
            .select("ts_us", "v_bool")
            .rows()
        )
        auto = (
            ds.filter(pl.col("signal") == "DS:autonomous")
            .sort("ts_us")
            .select("ts_us", "v_bool")
            .rows()
        )
        return frame(modes_from_ds(enabled, auto), "ds")

    def derive_framing(self) -> dict[str, Framing]:
        framings: dict[str, Framing] = {}
        rows = []
        for session in self.ledger.query("SELECT session_id FROM sessions"):
            framing = self.framing_for(session["session_id"])
            framings[session["session_id"]] = framing
            rows += [
                {
                    "session_id": session["session_id"],
                    "phase": p.name,
                    "start_us": p.start_us,
                    "end_us": p.end_us,
                    "source": framing.source,
                }
                for p in framing.phases
            ]
        self.ledger.replace_rows("match_phases", rows)
        return framings

    # --- silver ---------------------------------------------------------------------------

    def derive_silver(
        self, framings: dict[str, Framing], run_id: str, only: set[str] | None = None
    ) -> dict[str, int]:
        robots = {r.robot: r for r in self.robots}
        counts: dict[str, int] = {}
        for session in self.ledger.query("SELECT * FROM sessions WHERE robot != 'unknown'"):
            if only is not None and session["session_id"] not in only:
                continue
            robot = robots.get(session["robot"])
            hoot_rows = self.ledger.query(
                "SELECT log_id, bus, offset_us FROM session_hoots"
                " WHERE session_id = ? AND offset_us IS NOT NULL",
                (session["session_id"],),
            )
            if robot is None or not hoot_rows:
                continue
            hoots = []
            for row in hoot_rows:
                slots = {}
                for sighting in self._sightings(session["session_id"]):
                    if sighting.bus != row["bus"]:
                        continue
                    slot = robot.slot_for(sighting.bus, sighting.model, sighting.can_id)
                    if slot is not None:
                        slots[(sighting.model, sighting.can_id)] = slot.id
                hoots.append(HootInSession(row["log_id"], row["offset_us"], slots))
            observations = [
                Observation(**o)
                for o in self.ledger.query(
                    "SELECT session_id, slot_id, unit_id, source, from_ts_us, to_ts_us"
                    " FROM slot_observations WHERE session_id = ?",
                    (session["session_id"],),
                )
            ]
            counts[session["session_id"]] = write_session(
                self.con, self.lake, session["session_id"], session["season"], hoots,
                observations, framings[session["session_id"]], run_id,
            )  # fmt: skip
        return counts

    # --- full run ------------------------------------------------------------------------

    def _fingerprint(self, session_id: str, framing: Framing) -> str:
        parts = {
            "pipeline": fp_config.PIPELINE_VERSION,
            "session": self.ledger.query(
                "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
            ),
            "hoots": self.ledger.query(
                "SELECT * FROM session_hoots WHERE session_id = ? ORDER BY log_id", (session_id,)
            ),
            "units": self.ledger.query(
                "SELECT * FROM slot_observations WHERE session_id = ? ORDER BY slot_id, from_ts_us",
                (session_id,),
            ),
            "phases": [p.__dict__ for p in framing.phases],
            "robots": [r.model_dump(mode="json") for r in self.robots],
        }
        return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()

    def run(self, force: bool = False) -> dict[str, int]:
        """Sessions, identity, framing, then silver and gold for sessions whose inputs changed."""
        run_id = f"{time.strftime('%Y%m%dT%H%M%S')}-derive"
        self.derive_sessions()
        self.derive_identity()
        framings = self.derive_framing()
        previous = {
            r["session_id"]: r["fingerprint"]
            for r in self.ledger.query("SELECT * FROM derived_state")
        }
        current = {sid: self._fingerprint(sid, f) for sid, f in framings.items()}
        stale = {sid for sid, fp in current.items() if force or previous.get(sid) != fp}
        silver_counts = self.derive_silver(
            {sid: framings[sid] for sid in stale}, run_id, only=stale
        )
        robots = {r.robot: r for r in self.robots}
        state = {
            sid: r
            for sid, r in (
                (r["session_id"], r) for r in self.ledger.query("SELECT * FROM derived_state")
            )
        }
        for session in self.ledger.query("SELECT * FROM sessions"):
            sid = session["session_id"]
            if sid not in stale:
                continue
            robot = robots.get(session["robot"])
            hoots = self.ledger.query(
                "SELECT log_id, confidence FROM session_hoots WHERE session_id = ?", (sid,)
            )
            rank = {"high": 0, "low": 1, "very-low": 2, None: 3}
            alignment = max(
                (h["confidence"] for h in hoots), key=lambda c: rank.get(c, 3), default=None
            )
            sources = ",".join(
                sorted(
                    [h["log_id"] for h in hoots]
                    + ([session["wpilog_id"]] if session["wpilog_id"] else [])
                )
            )
            rows = (
                gold.session_features(
                    self.con, self.lake, session, framings[sid], robot, alignment or "none", sources
                )
                if robot is not None and session["match_key"]
                else []
            )
            gold.write_session(self.lake, session["season"], sid, rows, run_id)
            state[sid] = {
                "session_id": sid,
                "fingerprint": current[sid],
                "silver_rows": silver_counts.get(sid, 0),
                "gold_rows": len(rows),
            }
        state = {sid: row for sid, row in state.items() if sid in current}
        self.ledger.replace_rows("derived_state", list(state.values()))
        for staging in (
            self.lake.root / "silver" / "_staging",
            self.lake.root / "gold" / "_staging",
        ):
            shutil.rmtree(staging, ignore_errors=True)
        return {"sessions": len(current), "rebuilt": len(stale)}


_DEVICE = re.compile(r"^Phoenix6/(?P<model>[A-Za-z0-9]+)-(?P<id>\d+)/")


def _session_day(session: dict[str, Any]) -> date:
    if session.get("utc_start"):
        return datetime.fromisoformat(session["utc_start"]).date()
    stamp = session["session_id"].removeprefix("hoot-")[:10]
    return date.fromisoformat(stamp)
