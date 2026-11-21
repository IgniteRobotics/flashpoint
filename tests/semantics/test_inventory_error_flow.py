"""An inventory that reports an error behaves as robot-side/can-inventory.md says.

Spec project-documentation, "Inventory error behaves as documented": a wpilog whose
`/Flashpoint/CANInventory` holds `{"schema": 1, "error": "..."}` is ingested and derived;
Flashpoint records the error and keeps the log's history by swap dates.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from flashpoint.cli import main
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.readers import hoot
from flashpoint.readers.hoot import OwletRegistry
from tests.wpilog_builder import WpilogBuilder

START = datetime(2026, 4, 9, 21, 51, 10, tzinfo=UTC)
ERROR = "the diagnostics server listed no devices"
ROBOT = """
robot = "bot"
season = 2026
project = "P"

[[slot]]
id = "hood"
bus = "rio"
model = "TalonFX"
can_id = 5
subsystem = "shooter"
role = "hood"
swaps = ["2026-03-20"]
"""


def _fake_convert(
    hoot_path: Path, out_dir: Path, registry: OwletRegistry, profile: str = "health"
) -> hoot.HootConversion:
    """A rio hoot holding one Talon FX (CAN id 5), without running owlet."""
    out_dir.mkdir(parents=True, exist_ok=True)
    b = WpilogBuilder().start(1, "Phoenix6/TalonFX-5/SupplyCurrent", "double")
    for i in range(10):
        b.double(1, 1_000_000 + i * 20_000, 2.0)
    wpilog = b.write(out_dir / "converted.wpilog")
    return hoot.HootConversion(wpilog, 1, "fake", False, profile, 1, "rio")


@pytest.fixture
def config_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "config"
    (root / "robots").mkdir(parents=True)
    (root / "robots" / "bot.toml").write_text(ROBOT, encoding="utf-8")
    (root / "acquire.toml").write_text("team = 6829\n", encoding="utf-8")
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    return root


def test_inventory_error_is_recorded_and_history_uses_swap_dates(
    tmp_path: Path, config_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hoot, "convert", _fake_convert)
    logs = tmp_path / "logs"
    logs.mkdir()
    epoch_us = int(START.timestamp() * 1_000_000)
    b = (
        WpilogBuilder()
        .start(1, "systemTime", "int64")
        .start(2, "/Flashpoint/CANInventory", "string")
        .int64(1, 1_000_000, epoch_us)
        .string(2, 6_000_000, json.dumps({"schema": 1, "error": ERROR}))
        .int64(1, 20_000_000, epoch_us + 19_000_000)
    )
    wpilog = b.write(logs / "FRC_20260409_215110.wpilog")
    rio = logs / "rio_2026-04-09_21-51-13.hoot"
    rio.write_bytes(b"not read by the fake converter")
    lake = tmp_path / "lake"

    assert main(["ingest", "--lake", str(lake), str(wpilog), str(rio)]) == 0

    ledger = Ledger(LakePaths(lake).ledger)
    try:
        inventory = ledger.query("SELECT valid, error FROM inventory")
        units = ledger.query("SELECT slot_id, unit_id, source FROM slot_observations")
    finally:
        ledger.close()
    assert [(r["valid"], r["error"]) for r in inventory] == [(0, ERROR)]
    # One swap date (2026-03-20) on or before the log's day: epoch 1, not a ctre: serial.
    assert [(r["slot_id"], r["unit_id"], r["source"]) for r in units] == [
        ("hood", "legacy:bot:hood:1", "legacy")
    ]
