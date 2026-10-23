"""Draft a robot file from an imported session (team-setup design D-h).

Devices come from the session's hoots (the ledger's hoot entries, the same device list device
identity uses), serials from the session's CAN inventory (D11), bus names from the bus-status
entries robot code writes to the wpilog. The draft is a valid robot whose slots the user then
names; saving goes through `flashpoint.setup.editor` like any other edit.
"""

import json
import re
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
    sightings = set()
    for row in rows:
        match = _DEVICE.match(row["name"])
        if match:
            sightings.add((str(row["bus"]), match["model"], int(match["id"])))
    if not sightings:
        raise DraftError("this session has no CAN devices to draft from (no hoot files)")
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
