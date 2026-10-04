"""Derived layers from bronze + configuration: sessions, alignment, identity, framing, silver, gold.

Everything here is rebuildable without raw logs or network access.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.query import connect
from flashpoint.meta.extract import parse_hoot_filename
from flashpoint.semantics.alignment import (
    HOOT_ENABLE,
    WPILOG_ENABLE,
    Alignment,
    align_by_enable_edges,
    align_by_payload,
    hoot_bus_agreement_us,
)
from flashpoint.semantics.match_identity import identify
from flashpoint.semantics.robot_config import RobotConfig, load_robots, select_robot
from flashpoint.semantics.sessions import HootGroup, HootLog, Session, WpilogLog, group_sessions

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
