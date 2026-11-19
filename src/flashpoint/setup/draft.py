"""Draft a robot file from an imported session (team-setup design D-h).

Devices come from the session's hoots (the ledger's hoot entries, the same device list device
identity uses), serials from the session's CAN inventory (D11), bus names from the bus-status
entries robot code writes to the wpilog. The draft is a valid robot whose slots the user then
names; saving goes through `flashpoint.setup.editor` like any other edit.
"""

import datetime as dt
import json
import os
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths

_DEVICE = re.compile(r"^Phoenix6/(?P<model>[A-Za-z0-9]+)-(?P<id>\d+)/")
_BUS_STATUS = re.compile(r"^/Flashpoint/CANBus/(?P<name>[^/]+)/")


class DraftError(ValueError):
    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


class ReadOnlyConfigError(DraftError):
    """A robot should be drafted, but the configuration root can't be written."""


ReadOnlyConfig = ReadOnlyConfigError


@dataclass(frozen=True)
class Drafted:
    robot: str
    season: int
    project: str
    source: str  # the match key, else the session id
    devices: int
    path: str  # relative to the configuration root


def _query(lake: LakePaths, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    if not lake.ledger.is_file():
        return []
    ledger = Ledger(lake.ledger)
    try:
        return ledger.query(sql, args)
    finally:
        ledger.close()


def list_sessions(lake: LakePaths) -> list[dict[str, Any]]:
    """Sessions to draft from, newest first, with how many hoots each has."""
    return _query(
        lake,
        "SELECT s.session_id, s.match_key, s.season, l.project, s.robot,"
        " (SELECT count(*) FROM session_hoots h WHERE h.session_id = s.session_id) AS hoots,"
        " l.filename"
        " FROM sessions s LEFT JOIN logs l ON l.log_id = s.wpilog_id"
        " ORDER BY coalesce(l.utc_start, '') DESC, s.session_id",
    )


def import_command(lake: str, system: str) -> str:
    """The command that imports one match's logs into `lake`, quoted for the platform's shell:
    shlex on POSIX, double quotes on Windows (both PowerShell and cmd take them)."""
    quoted = f'"{lake}"' if system == "Windows" else shlex.quote(lake)
    return f"flashpoint ingest --lake {quoted} <folder>"


def quarantined_count(lake: LakePaths) -> int:
    rows = _query(lake, "SELECT count(*) AS n FROM files WHERE stage = 'quarantined'")
    return int(rows[0]["n"]) if rows else 0


def _hoot_bus(bus: str, canivores: set[str]) -> str:
    if bus == "rio":
        return "rio"
    return "canivore" if len(canivores) <= 1 else bus


def _serials(lake: LakePaths, wpilog_id: str | None) -> dict[tuple[bool, str, int], str]:
    """(is rio, model, id) -> serial, from the session's last valid inventory."""
    from flashpoint.meta.extract import validate_inventory  # polars: only when drafting

    if wpilog_id is None:
        return {}
    rows = _query(
        lake,
        "SELECT payload FROM inventory WHERE log_id = ? AND valid = 1 ORDER BY ts_us DESC",
        (wpilog_id,),
    )
    for row in rows:
        if validate_inventory(row["payload"])[0]:
            devices = json.loads(row["payload"])["devices"]
            return {
                (str(d["bus"]).lower() == "rio", str(d["model"]).replace(" ", ""), int(d["id"])):
                str(d["serial"])
                for d in devices
            }  # fmt: skip
    return {}


def _robot_id(config_root: Path, season: int) -> str:
    taken = {p.stem for p in (config_root / "robots").glob("*.toml")}
    for candidate in (f"{season}-comp", f"{season}-practice"):
        if candidate not in taken:
            return candidate
    n = 2
    while f"{season}-robot-{n}" in taken:
        n += 1
    return f"{season}-robot-{n}"


def draft_robot(lake: LakePaths, config_root: Path, session_id: str) -> dict[str, Any]:
    sessions = _query(
        lake,
        "SELECT s.session_id, s.wpilog_id, s.season, s.match_key, l.project"
        " FROM sessions s LEFT JOIN logs l ON l.log_id = s.wpilog_id WHERE s.session_id = ?",
        (session_id,),
    )
    if not sessions:
        raise DraftError(f"no session {session_id!r} in this lake", 404)
    session = sessions[0]
    rows = _query(
        lake,
        "SELECT DISTINCT sh.bus, e.name FROM session_hoots sh JOIN entries e USING (log_id)"
        " WHERE sh.session_id = ? AND e.name LIKE 'Phoenix6/%'",
        (session_id,),
    )
    sightings = _sightings(rows)
    if not sightings:
        raise DraftError("this session has no CAN devices to draft from (no hoot files)")
    return _draft(lake, config_root, session, sightings)


def _sightings(rows: list[dict[str, Any]]) -> set[tuple[str, str, int]]:
    """(bus, model, CAN id) for each `Phoenix6/<model>-<id>/` entry in rows of (bus, name)."""
    found = set()
    for row in rows:
        match = _DEVICE.match(row["name"])
        if match:
            found.add((str(row["bus"]), match["model"], int(match["id"])))
    return found


def _draft(
    lake: LakePaths,
    config_root: Path,
    session: dict[str, Any],
    sightings: set[tuple[str, str, int]],
) -> dict[str, Any]:
    session_id = session["session_id"]
    canivores = {bus for bus, _, _ in sightings if bus != "rio"}
    serials = _serials(lake, session["wpilog_id"])
    slots, devices = [], []
    for bus, model, can_id in sorted(sightings, key=lambda s: (s[0] != "rio", s[0], s[1], s[2])):
        slot_id = f"{model.lower()}-{can_id}"
        if len(canivores) > 1 and bus != "rio":
            slot_id = f"{slot_id}-{bus[:6]}"
        slots.append({"id": slot_id, "bus": _hoot_bus(bus, canivores), "model": model,
                      "can_id": can_id, "subsystem": "", "role": ""})  # fmt: skip
        devices.append({"slot": slot_id, "serial": serials.get((bus == "rio", model, can_id))})

    bus_names = sorted({
        m["name"]
        for r in _query(lake, "SELECT name FROM entries WHERE log_id = ?",
                        (session["wpilog_id"],))
        if (m := _BUS_STATUS.match(r["name"]))
    }, key=lambda name: (name != "rio", name))  # fmt: skip
    buses = [{"name": name, "hoot": "rio" if name == "rio" else "canivore"} for name in bus_names]
    if len(canivores) > 1:
        buses = [b for b in buses if b["hoot"] == "rio"]  # several CANivores: user ties names

    season = int(session["season"]) if str(session["season"]).isdigit() else 0
    robot = {
        "robot": _robot_id(config_root, season),
        "season": season,
        "project": session["project"] or "",
        "slot": slots,
        "bus": buses,
    }
    return {
        "session_id": session_id,
        "match_key": session["match_key"],
        "robot": robot,
        "devices": devices,
        "inventory": bool(serials),
        "bus_status": bool(bus_names),
    }


# --- the first derive drafts the robot (team-setup, "First derive drafts the robot") -------

DRAFT_COMMENT = (
    "# Drafted automatically by flashpoint from {source} on {day}."
    " Name each slot's subsystem and role in Setup > Robots."
)


def _when(value: str | None) -> dt.datetime | None:
    return dt.datetime.fromisoformat(value) if value else None


def _candidates(lake: LakePaths, skip: set[str]) -> dict[tuple[str, str | None], list[Any]]:
    """Sessions with a wpilog and hoots, by (season, project), for seasons not in `skip`.

    Grouped from the ledger's logs the way derive groups them, without aligning anything:
    the first derive needs its robot before it has written any session.
    """
    from flashpoint.semantics.match_identity import identify
    from flashpoint.semantics.sessions import HootLog, WpilogLog, group_sessions

    rows = _query(
        lake,
        "SELECT l.*, h.bus, h.session_stamp FROM logs l JOIN files f ON f.sha256 = l.log_id"
        " LEFT JOIN hoot_logs h USING (log_id) WHERE f.stage = 'success'",
    )
    logs = {r["log_id"]: r for r in rows}
    wpilogs = [
        WpilogLog(r["log_id"], _when(r["utc_start"]), _when(r["utc_end"]), r["file_event"],
                  f"{r['file_match_type']}{r['file_match_number']}"
                  if r["file_match_type"] else None)
        for r in rows if r["kind"] == "wpilog"
    ]  # fmt: skip
    from flashpoint.meta.extract import parse_hoot_filename

    hoots = []
    for r in rows:
        if r["kind"] == "hoot" and r["session_stamp"]:
            name = parse_hoot_filename(r["filename"] or "")
            hoots.append(HootLog(r["log_id"], r["session_stamp"], r["bus"], name.event, name.match))
    found: dict[tuple[str, str | None], list[Any]] = {}
    for session in group_sessions(wpilogs, hoots):
        w = logs.get(session.wpilog_id or "")
        if w is None or not session.hoot_groups:
            continue  # hoot-only: no project to match a robot by
        season = str(w["season"])
        if season in skip or not season.isdigit():
            continue
        buses = {log_id: bus for g in session.hoot_groups for log_id, bus in g.buses.items()}
        marks = ", ".join("?" for _ in buses)
        entries = _query(
            lake,
            f"SELECT DISTINCT log_id, name FROM entries WHERE log_id IN ({marks})"  # noqa: S608 - placeholders only
            " AND name LIKE 'Phoenix6/%'",
            tuple(buses),
        )
        sightings = _sightings([{"bus": buses[e["log_id"]], "name": e["name"]} for e in entries])
        if not sightings:
            continue
        ident = identify(
            season, w["fms_event"], w["fms_match_type"], w["fms_match_number"], w["fms_replay"],
            w["file_event"], w["file_match_type"], w["file_match_number"],
        )  # fmt: skip
        found.setdefault((season, w["project"]), []).append(
            {"session_id": session.session_id, "wpilog_id": session.wpilog_id, "season": season,
             "match_key": ident.key, "project": w["project"], "utc_start": w["utc_start"] or "",
             "sightings": sightings}
        )  # fmt: skip
    return found


def _writable(root: Path) -> bool:
    robots = root / "robots"
    target = robots if robots.is_dir() else root
    return os.access(target, os.W_OK)


def auto_draft(lake: LakePaths, config_root: Path, today: dt.date | None = None) -> list[Drafted]:
    """Draft and save a robot for each robot-code project of each season in the lake that has
    no robot file, from its session with the most CAN devices (newest on a tie).

    Only with a configuration (`acquire.toml`, as `init` writes it); a season with any robot
    file is never touched. Raises ReadOnlyConfig when a draft is due but can't be written.
    """
    from flashpoint.acquire.config import CONFIG_FILE
    from flashpoint.semantics.robot_config import ConfigError, load_config_seasons, load_robots
    from flashpoint.setup import editor

    if not (config_root / CONFIG_FILE).is_file() or not lake.ledger.is_file():
        return []
    try:
        seasons = load_config_seasons(config_root)
        robots_dir = config_root / "robots"
        robots = load_robots(robots_dir, seasons) if robots_dir.is_dir() else []
    except ConfigError:
        return []  # derive names the broken file; drafting next to it would only add noise
    groups = _candidates(lake, {str(r.season) for r in robots})
    if not groups:
        return []
    if not _writable(config_root):
        raise ReadOnlyConfigError(f"{config_root} is read-only: no robot drafted from the new logs")

    def order(key: tuple[str, str | None]) -> tuple[str, bool, int, str]:
        season, project = key
        practice = "practice" in (project or "").lower()  # a practice robot takes -practice
        return season, practice, -len(groups[key]), project or ""

    day = (today or dt.datetime.now().astimezone().date()).isoformat()
    drafted = []
    for key in sorted(groups, key=order):
        best = max(groups[key], key=lambda s: (len(s["sightings"]), s["utc_start"]))
        payload = _draft(lake, config_root, best, best["sightings"])
        robot = payload["robot"]
        source = best["match_key"] or best["session_id"]
        result = editor.save_robot(
            config_root, robot["robot"], robot, None, new=True,
            header=DRAFT_COMMENT.format(source=source, day=day),
        )  # fmt: skip
        drafted.append(
            Drafted(robot["robot"], robot["season"], robot["project"], source,
                    len(robot["slot"]), result.path)
        )  # fmt: skip
    return drafted
