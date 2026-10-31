import contextlib
import hashlib
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.live.fake_robot import (
    FakeRobot,
    Restart,
    SetBus,
    SetDevice,
    SetEnabled,
    SetVersion,
    Step,
    WrongType,
)
from flashpoint.live.service import LiveCanService
from flashpoint.live.slots import load_resolver
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.server import FlashpointServer
from tests.live.test_fake_robot import DEVICES, free_port
from tests.web.anomaly_lake import build_filter_lake
from tests.web.test_live_api import Stream, port_of

# Steps in one group go out in the same NetworkTables update.
EVERY_STEP: list[list[Step]] = [
    [SetDevice("DriveTrain", "TalonFX-11", Dropouts=2, MaxAgeMs=48.0)],
    [SetEnabled(True)],
    [SetDevice("rio", "TalonFX-2", Resets=1, Faults=0b10)],
    [
        SetDevice("DriveTrain", "TalonFX-12", Dropouts=1),
        SetDevice("DriveTrain", "CANcoder-13", Dropouts=1),
    ],  # 2 of 3: bus-wide
    [SetBus("rio", BusOffCount=1, ReceiveErrorCount=9)],
    [SetDevice("rio", "TalonFX-8", Connected=False)],
    [SetDevice("rio", "TalonFX-8", Connected=True)],
    [WrongType("rio/TalonFX-8/Dropouts", "lots")],
    [Restart()],
    [SetVersion(2)],
    [SetVersion(1)],
    [SetDevice("rio", "TalonFX-2", Dropouts=1)],
]


def fingerprint(root: Path) -> dict[str, str]:
    """sha256 of every file under root, by relative path (new or missing files show too)."""
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


EXPECTED_KINDS = {
    "dropout",
    "bus-dropout",
    "reboot",
    "fault",
    "bus-errors",
    "disconnected",
    "reconnected",
    "restart",
}
STEP_GAP_S = 0.25  # each step its own NetworkTables update


def run_live_session(lake: LakePaths) -> set[str]:
    """Serve `lake` with a Live CAN view open through every step; the event kinds seen."""
    with FakeRobot(DEVICES, port=free_port()) as robot:
        live = LiveCanService(
            ["127.0.0.1"],
            port=robot.port,
            resolver=load_resolver(config.config_root()),
            linger_s=0.2,
        )
        server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake), live=live)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        stream = Stream(port_of(server))
        kinds: set[str] = set()
        last_dropout = threading.Event()

        def read() -> None:
            with contextlib.suppress(EOFError, OSError, ValueError):
                while True:
                    kind, message, _ = stream.next()
                    if kind == "event":
                        kinds.add(message["kind"])
                        if message["kind"] == "dropout" and message["device"] == "TalonFX-2":
                            last_dropout.set()

        try:
            stream.until("snapshot", lambda s: s["status"] == "ok")
            reader = threading.Thread(target=read, daemon=True)
            reader.start()
            for group in EVERY_STEP:
                robot.run(group)
                time.sleep(STEP_GAP_S)
            assert last_dropout.wait(5), sorted(kinds)
        finally:
            stream.close()
            server.shutdown()
            server.server_close()
            server.queries.close()
        assert not live.is_open
        return kinds


def check_unchanged(lake: LakePaths) -> None:
    config_dir = config.config_root()
    lake_before, config_before = fingerprint(lake.root), fingerprint(config_dir)
    assert lake_before  # a real lake, not an empty folder
    assert run_live_session(lake) >= EXPECTED_KINDS
    assert fingerprint(lake.root) == lake_before
    assert fingerprint(config_dir) == config_before


def test_live_session_leaves_the_lake_unchanged(tmp_path: Path) -> None:
    lake, _ = build_filter_lake(tmp_path / "lake", replay=True)
    check_unchanged(lake)


@pytest.mark.corpus
def test_live_session_leaves_the_corpus_lake_unchanged(derived_corpus_lake: Any) -> None:
    check_unchanged(derived_corpus_lake)
