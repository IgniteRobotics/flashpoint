import json
import shutil
import time
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.robot_config import RobotConfig
from flashpoint.setup import editor
from flashpoint.setup.draft import DraftError, draft_robot, list_sessions

DRAFT_BUDGET_S = 1.0
CANIVORE = "0123456789abcdef0123456789abcdef"


def _ledger(lake: LakePaths) -> Ledger:
    lake.meta.mkdir(parents=True, exist_ok=True)
    return Ledger(lake.ledger)


def _insert(ledger: Ledger, table: str, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        columns = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        ledger.execute(f"INSERT INTO {table} ({columns}) VALUES ({marks})", tuple(row.values()))


def _synthetic(tmp_path: Path, inventory: bool = True, bus_status: bool = True) -> LakePaths:
    """One session: a wpilog plus a rio and a CANivore hoot, three devices."""
    lake = LakePaths(tmp_path / "lake")
    ledger = _ledger(lake)
    _insert(ledger, "logs", [
        {"log_id": "w1", "kind": "wpilog", "season": "2026", "filename": "FRC_Q1.wpilog",
         "project": "Robot-2026"},
        {"log_id": "h1", "kind": "hoot", "season": "2026", "filename": "rio.hoot"},
        {"log_id": "h2", "kind": "hoot", "season": "2026", "filename": "can.hoot"},
    ])  # fmt: skip
    _insert(ledger, "sessions", [
        {"session_id": "s1", "wpilog_id": "w1", "robot": None, "season": "2026",
         "match_key": "2026syn_qm1", "kind": "match"},
    ])  # fmt: skip
    _insert(ledger, "session_hoots", [
        {"log_id": "h1", "session_id": "s1", "hoot_group": "g", "bus": "rio"},
        {"log_id": "h2", "session_id": "s1", "hoot_group": "g", "bus": CANIVORE},
    ])  # fmt: skip
    entries = [
        ("h1", "Phoenix6/TalonFX-5/Timestamp"),
        ("h1", "Phoenix6/TalonFX-5/DeviceTemp"),
        ("h2", "Phoenix6/TalonFX-11/Timestamp"),
        ("h2", "Phoenix6/Pigeon2-10/Timestamp"),
    ]
    if bus_status:
        entries += [("w1", "/Flashpoint/CANBus/rio/Utilization"),
                    ("w1", "/Flashpoint/CANBus/DriveTrain/Utilization")]  # fmt: skip
    _insert(ledger, "entries", [
        {"log_id": log, "idx": i, "entry_id": i, "name": name, "type": "double"}
        for i, (log, name) in enumerate(entries)
    ])  # fmt: skip
    if inventory:
        payload = {"schema": 1, "devices": [
            {"bus": "rio", "model": "Talon FX", "id": 5, "serial": "AAA5"},
            {"bus": "DriveTrain", "model": "Talon FX", "id": 11, "serial": "AAA11"},
        ]}  # fmt: skip
        _insert(ledger, "inventory", [
            {"log_id": "w1", "ts_us": 1, "payload": json.dumps(payload), "valid": 1},
        ])  # fmt: skip
    ledger.close()
    return lake


def test_draft_from_a_session(tmp_path: Path) -> None:
    root = tmp_path / "cfg"
    root.mkdir()
    draft = draft_robot(_synthetic(tmp_path), root, "s1")
    robot = draft["robot"]
    assert (robot["season"], robot["project"]) == (2026, "Robot-2026")
    slots = {s["id"]: s for s in robot["slot"]}
    assert set(slots) == {"talonfx-5", "talonfx-11", "pigeon2-10"}
    assert slots["talonfx-11"] | {"subsystem": "", "role": ""} == slots["talonfx-11"]
    assert (slots["talonfx-11"]["bus"], slots["talonfx-5"]["bus"]) == ("canivore", "rio")
    serials = {d["slot"]: d["serial"] for d in draft["devices"]}
    assert serials == {"talonfx-5": "AAA5", "talonfx-11": "AAA11", "pigeon2-10": None}
    assert robot["bus"] == [{"name": "rio", "hoot": "rio"},
                            {"name": "DriveTrain", "hoot": "canivore"}]  # fmt: skip
    assert draft["inventory"] and draft["bus_status"]
    RobotConfig.model_validate(robot)  # a valid robot once saved, before the user completes it


def test_draft_without_inventory_or_bus_status(tmp_path: Path) -> None:
    lake = _synthetic(tmp_path, inventory=False, bus_status=False)
    draft = draft_robot(lake, tmp_path / "cfg", "s1")
    assert not draft["inventory"] and not draft["bus_status"]
    assert all(d["serial"] is None for d in draft["devices"])
    assert draft["robot"]["bus"] == []


def test_suggested_robot_id_is_free(tmp_path: Path) -> None:
    root = tmp_path / "cfg"
    shutil.copytree(config.config_root(), root)
    draft = draft_robot(_synthetic(tmp_path), root, "s1")
    assert draft["robot"]["robot"] not in {"2026-comp", "2025-comp"}
    assert draft["robot"]["robot"].startswith("2026-")


def test_unknown_session(tmp_path: Path) -> None:
    with pytest.raises(DraftError) as caught:
        draft_robot(_synthetic(tmp_path), tmp_path / "cfg", "nope")
    assert caught.value.status == 404


def test_session_list(tmp_path: Path) -> None:
    sessions = list_sessions(_synthetic(tmp_path))
    assert sessions == [{"session_id": "s1", "match_key": "2026syn_qm1", "season": "2026",
                         "project": "Robot-2026", "robot": None, "hoots": 2,
                         "filename": "FRC_Q1.wpilog"}]  # fmt: skip
    assert list_sessions(LakePaths(tmp_path / "empty")) == []


# --- corpus -------------------------------------------------------------------------------


def _session(lake: LakePaths, filename: str) -> str:
    ledger = Ledger(lake.ledger)
    try:
        rows = ledger.query(
            "SELECT s.session_id FROM sessions s JOIN logs l ON l.log_id = s.wpilog_id"
            " WHERE l.filename = ?",
            (filename,),
        )
    finally:
        ledger.close()
    return str(rows[0]["session_id"])


@pytest.mark.corpus
def test_draft_from_q7(derived_corpus_lake: LakePaths, tmp_path: Path) -> None:
    session = _session(derived_corpus_lake, "FRC_20260409_215113_GACMP_Q7.wpilog")
    started = time.perf_counter()
    draft = draft_robot(derived_corpus_lake, config.config_root(), session)
    elapsed = time.perf_counter() - started
    robot = draft["robot"]
    assert len(robot["slot"]) == 22
    assert {s["bus"] for s in robot["slot"]} == {"rio", "canivore"}
    assert (robot["project"], robot["season"]) == ("Robot-2026", 2026)
    assert elapsed < DRAFT_BUDGET_S, f"draft took {elapsed:.2f} s"


@pytest.mark.corpus
def test_log_without_hoots(derived_corpus_lake: LakePaths) -> None:
    session = _session(derived_corpus_lake, "FRC_20260409_171208_GACMP_P2.wpilog")
    with pytest.raises(DraftError, match="no CAN devices"):
        draft_robot(derived_corpus_lake, config.config_root(), session)


@pytest.mark.corpus
def test_draft_saved_as_an_existing_robot_id(
    derived_corpus_lake: LakePaths, tmp_path: Path
) -> None:
    root = tmp_path / "cfg"
    shutil.copytree(config.config_root(), root)
    before = (root / "robots" / "2026-comp.toml").read_bytes()
    session = _session(derived_corpus_lake, "FRC_20260409_215113_GACMP_Q7.wpilog")
    robot: dict[str, Any] = draft_robot(derived_corpus_lake, root, session)["robot"]
    robot = robot | {"robot": "2026-comp"}
    with pytest.raises(editor.SaveError, match="2026-comp"):
        editor.save_robot(root, "2026-comp", robot, None, new=True)
    assert (root / "robots" / "2026-comp.toml").read_bytes() == before
