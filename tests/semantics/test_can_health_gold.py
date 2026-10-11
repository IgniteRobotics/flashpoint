"""Per-match CAN health rows and events (spec: can-health, "Per-match CAN health in gold",
"Bus status per match")."""

from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pytest

from flashpoint.lake.paths import LakePaths
from flashpoint.semantics import can_health
from flashpoint.semantics.can_health import HootInput, SessionCanHealth
from flashpoint.semantics.framing import Framing, frame
from flashpoint.semantics.identity import Observation
from flashpoint.semantics.robot_config import RobotConfig
from flashpoint.semantics.signals import signals_dir
from tests import can_health_builder as b

S = b.S
ROBOT = RobotConfig.model_validate(
    {
        "robot": "2026-comp",
        "season": 2026,
        "project": "Robot-2026",
        "slot": [
            {"id": f"motor-{i}", "bus": "rio", "model": "TalonFX", "can_id": i,
             "subsystem": "intake" if i == 2 else "other", "role": f"motor {i}"}
            for i in range(1, 9)  # TalonFX-9 and -10 are unmapped siblings
        ],
        "bus": [{"name": "rio", "hoot": "rio"}, {"name": "DriveTrain", "hoot": "canivore"}],
    }
)  # fmt: skip


def _framing(auto: tuple[float, float], teleop: tuple[float, float]) -> Framing:
    return frame(
        [
            (0, "Disabled"),
            (int(auto[0] * S), "Autonomous"),
            (int(auto[1] * S), "Disabled"),
            (int(teleop[0] * S), "Teleop"),
            (int(teleop[1] * S), "Disabled"),
        ]
    )


def _units(*slots: str) -> list[Observation]:
    return [Observation("s1", s, f"ctre:{s}", "inventory") for s in slots]


ALL_UNITS = _units(*(f"motor-{i}" for i in range(1, 9)))


def _run(
    tmp_path: Path,
    hoot: b.Hoot,
    framing: Framing,
    offset_us: int | None = 0,
    match_key: str | None = "2026johnson_qm105",
    observations: list[Observation] | None = None,
    robot: RobotConfig | None = ROBOT,
    signals: list[dict[str, Any]] | None = None,
) -> SessionCanHealth:
    lake = LakePaths(tmp_path / "lake")
    b.write_hoot(lake, "h1", hoot)
    if signals:
        path = signals_dir(lake) / "season=2026" / "session_id=s1"
        path.mkdir(parents=True)
        pl.DataFrame(signals).with_columns(pl.lit("s1").alias("session_id")).write_parquet(
            path / "part-0.parquet"
        )
    session = {"session_id": "s1", "season": "2026", "robot": "2026-comp", "match_key": match_key}
    return can_health.session_can_health(
        duckdb.connect(),
        lake,
        session,
        framing,
        robot,
        [HootInput("h1", "rio", offset_us)],
        ALL_UNITS if observations is None else observations,
        "high",
        "h1,w1",
    )


def _row(result: SessionCanHealth, slot: str, phase: str, unit: str | None = None) -> Any:
    (row,) = [
        r for r in result.slots
        if r["slot_id"] == slot and r["phase"] == phase and (unit is None or r["unit_id"] == unit)
    ]  # fmt: skip
    return row


# --- 4.1 slot rows ------------------------------------------------------------------------


def test_q105_like_rows(tmp_path: Path) -> None:
    result = _run(tmp_path, b.q105_like(), _framing((200, 215), (218, 355)))
    match = _row(result, "motor-2", "match")
    assert (match["reboots"], match["reboots_enabled"]) == (1, 1)
    assert match["isolated_gaps"] >= 1
    assert match["longest_gap_ms"] == pytest.approx(820, abs=8)
    assert match["reset_count_logged"] is True
    assert (match["unit_id"], match["bus"], match["robot"]) == ("ctre:motor-2", "rio", "2026-comp")
    assert (match["alignment"], match["source_logs"]) == ("high", "h1,w1")
    assert match["missed_per_min"] > 0
    assert _row(result, "motor-2", "teleop")["reboots"] == 1
    auto = _row(result, "motor-2", "auto")
    assert (auto["reboots"], auto["isolated_gaps"], auto["longest_gap_ms"]) == (0, 0, 0.0)
    quiet = _row(result, "motor-1", "match")
    assert (quiet["reboots"], quiet["isolated_gaps"]) == (0, 0)


def test_one_row_per_mapped_device_and_phase(tmp_path: Path) -> None:
    result = _run(tmp_path, b.bus_wide(), _framing((6, 10), (11, 29)))
    keys = {(r["slot_id"], r["phase"]) for r in result.slots}
    assert keys == {(f"motor-{i}", p) for i in range(1, 9) for p in ("auto", "teleop", "match")}
    assert all(r["reboots"] == 0 for r in result.slots)
    assert all(r["isolated_gaps"] == 0 for r in result.slots)  # the 60 ms gap is bus-wide


def test_missed_frames_per_enabled_minute(tmp_path: Path) -> None:
    """One 40 ms gap on an 8 ms clock misses 4 frames; the match is enabled for 0.3 min."""
    devices = [b.Device(f"TalonFX-{i}") for i in range(1, 11)]
    devices[0].silences = [(20.0, 0.040)]
    hoot = b.Hoot(devices, duration_s=30.0, enabled=[(10.0, 28.0)])
    result = _run(tmp_path, hoot, _framing((10, 13), (13, 28)))
    row = _row(result, "motor-1", "match")
    assert row["isolated_gaps"] == 1
    assert row["missed_per_min"] == pytest.approx(4 / (18 / 60), rel=0.01)


def test_mid_session_swap(tmp_path: Path) -> None:
    devices = [b.Device(f"TalonFX-{i}") for i in range(1, 11)]
    devices[0].silences = [(20.0, 0.4)]
    hoot = b.Hoot(devices, duration_s=30.0, enabled=[(6.0, 29.0)])
    observations = [
        Observation("s1", "motor-1", "ctre:A", "inventory", None, 15 * S),
        Observation("s1", "motor-1", "ctre:B", "inventory", 15 * S, None),
        *_units(*(f"motor-{i}" for i in range(2, 9))),
    ]
    result = _run(tmp_path, hoot, _framing((6, 10), (11, 29)), observations=observations)
    assert _row(result, "motor-1", "match", "ctre:B")["isolated_gaps"] == 1
    assert _row(result, "motor-1", "match", "ctre:A")["isolated_gaps"] == 0
    (event,) = [e for e in result.events if e["kind"] == "dropout"]
    assert event["unit_id"] == "ctre:B"


def test_hoot_offset_maps_onto_lake_time(tmp_path: Path) -> None:
    """Lake (wpilog) time = hoot time + offset; framing is on the lake clock."""
    devices = [b.Device(f"TalonFX-{i}") for i in range(1, 11)]
    devices[0].silences = [(20.0, 0.4)]
    hoot = b.Hoot(devices, duration_s=30.0, enabled=[(6.0, 29.0)])
    result = _run(tmp_path, hoot, _framing((11, 15), (16, 34)), offset_us=5 * S)
    assert _row(result, "motor-1", "teleop")["isolated_gaps"] == 1
    assert _row(result, "motor-1", "auto")["isolated_gaps"] == 0
    (event,) = result.events
    assert (event["hoot_t_us"], event["t_us"], event["match_time_us"]) == (20 * S, 25 * S, 14 * S)


def test_no_match_key_no_rows_but_events(tmp_path: Path) -> None:
    result = _run(tmp_path, b.q105_like(), _framing((200, 215), (218, 355)), match_key=None)
    assert result.slots == [] and result.buses == []
    (reboot,) = [e for e in result.events if e["kind"] == "reboot"]
    assert reboot["t_us"] == 344_960_000 and reboot["match_time_us"] is None


def test_unaligned_hoot_keeps_hoot_time(tmp_path: Path) -> None:
    result = _run(tmp_path, b.q105_like(), frame([]), offset_us=None, match_key=None)
    (reboot,) = [e for e in result.events if e["kind"] == "reboot"]
    assert (reboot["hoot_t_us"], reboot["t_us"]) == (344_960_000, None)
    assert reboot["slot_id"] == "motor-2"


def test_unknown_robot_events_have_no_slot(tmp_path: Path) -> None:
    result = _run(tmp_path, b.q105_like(), _framing((200, 215), (218, 355)), robot=None)
    assert result.slots == []
    (reboot,) = [e for e in result.events if e["kind"] == "reboot"]
    assert (reboot["device"], reboot["slot_id"], reboot["unit_id"]) == ("TalonFX-2", None, None)


# --- 4.2 bus rows -------------------------------------------------------------------------


def test_bus_rows_count_logger_gaps(tmp_path: Path) -> None:
    result = _run(tmp_path, b.bus_wide(), _framing((6, 10), (11, 29)))
    rows = {r["phase"]: r for r in result.buses}
    assert set(rows) == {"auto", "teleop", "match"}
    teleop = rows["teleop"]
    assert (teleop["bus"], teleop["bus_name"], teleop["devices"]) == ("rio", "rio", 10)
    assert teleop["logger_gaps"] == 1 and rows["auto"]["logger_gaps"] == 0
    assert teleop["longest_logger_gap_ms"] == pytest.approx(65, abs=10)
    assert teleop["logger_gap_s"] == pytest.approx(teleop["longest_logger_gap_ms"] / 1000)
    assert teleop["util_max"] is None and teleop["tx_err_max"] is None  # not logged


def _status(metric: str, points: list[tuple[float, float]], bus: str = "rio") -> list[Any]:
    return [
        {"signal_id": f"can-{bus.lower()}-{metric}", "subsystem": "can", "component": bus,
         "metric": metric, "t_us": int(t * S), "value": v}
        for t, v in points
    ]  # fmt: skip


def test_bus_status_logged(tmp_path: Path) -> None:
    signals = (
        _status("utilization", [(5, 0.30), (12, 0.40), (20, 0.62)])
        + _status("transmit_error_count", [(5, 0.0), (15, 12.0), (25, 3.0)])
        + _status("receive_error_count", [(5, 1.0)])
        + _status("bus_off_count", [(5, 2.0), (22, 3.0)])
        + _status("tx_full_count", [(5, 0.0)])
        + _status("utilization", [(12, 0.9)], bus="DriveTrain")  # another bus: ignored
    )
    result = _run(tmp_path, b.bus_wide(), _framing((6, 10), (12, 28)), signals=signals)
    teleop = next(r for r in result.buses if r["phase"] == "teleop")
    assert teleop["util_max"] == pytest.approx(0.62)
    # 0.40 held for 8 s, then 0.62 for 8 s
    assert teleop["util_mean"] == pytest.approx(0.51)
    assert teleop["tx_err_max"] == 12 and teleop["rx_err_max"] == 1
    assert teleop["bus_off_delta"] == 1 and teleop["tx_full_delta"] == 0
    auto = next(r for r in result.buses if r["phase"] == "auto")
    assert auto["util_max"] == pytest.approx(0.30)  # held from before auto started


def test_bus_without_a_configured_name_has_no_status(tmp_path: Path) -> None:
    robot = ROBOT.model_copy(update={"buses": []})
    signals = _status("utilization", [(5, 0.3)])
    result = _run(tmp_path, b.bus_wide(), _framing((6, 10), (12, 28)), robot=robot,
                  signals=signals)  # fmt: skip
    teleop = next(r for r in result.buses if r["phase"] == "teleop")
    assert teleop["bus_name"] is None and teleop["util_max"] is None


# --- 4.3 events ---------------------------------------------------------------------------


def test_events_keep_reboots_and_long_gaps_only(tmp_path: Path) -> None:
    result = _run(tmp_path, b.q105_like(), _framing((200, 215), (218, 355)))
    kinds = sorted((e["kind"], e["device"]) for e in result.events)
    assert kinds == [("dropout", "TalonFX-2"), ("reboot", "TalonFX-2")]
    dropout = next(e for e in result.events if e["kind"] == "dropout")
    assert dropout["duration_ms"] == pytest.approx(820, abs=8)
    assert dropout["slot_id"] == "motor-2" and dropout["unit_id"] == "ctre:motor-2"
    assert dropout["match_time_us"] == dropout["t_us"] - 200 * S
    reboot = next(e for e in result.events if e["kind"] == "reboot")
    assert (reboot["enabled"], reboot["reset_before"], reboot["reset_after"]) == (True, 0, 1)
    assert reboot["duration_ms"] is None


def test_short_bus_gap_is_not_an_event(tmp_path: Path) -> None:
    assert _run(tmp_path, b.bus_wide(), _framing((6, 10), (11, 29))).events == []


def test_whole_bus_silence_is_one_bus_event(tmp_path: Path) -> None:
    result = _run(tmp_path, b.whole_bus(), _framing((6, 10), (11, 29)))
    (event,) = result.events
    assert (event["kind"], event["device"], event["slot_id"], event["bus"]) == (
        "bus_gap", None, None, "rio"
    )  # fmt: skip
    assert event["duration_ms"] >= 1400


def test_rows_are_deterministic(tmp_path: Path) -> None:
    first = _run(tmp_path / "a", b.q105_like(), _framing((200, 215), (218, 355)))
    second = _run(tmp_path / "b", b.q105_like(), _framing((200, 215), (218, 355)))
    assert first == second


# --- 4.4 derive wiring ---------------------------------------------------------------------


@pytest.mark.parametrize("ds", [True, False], ids=["framed", "unframed"])
def test_session_without_hoots_derives_with_no_can_rows(tmp_path: Path, ds: bool) -> None:
    from flashpoint.readers.hoot import OwletRegistry
    from flashpoint.semantics.gold import gold_dir
    from tests.semantics.test_signals import SIGNALS, _config, _derive, _ingest, _wpilog

    manifest = tmp_path / "empty-manifest.toml"
    manifest.write_text("")
    lake = LakePaths(tmp_path / "lake")
    wpilog = _wpilog(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog", ds=ds)
    _ingest(lake, OwletRegistry(manifest, tmp_path / "owlet"), wpilog)
    result = _derive(lake, _config(tmp_path / "config", {"r": (2026, SIGNALS)}))
    assert result["sessions"] == 1
    for table in (can_health.TABLE, can_health.BUS_TABLE, can_health.EVENTS_TABLE):
        assert not any(gold_dir(lake, table).glob("season=*/session_id=*"))


def test_hoot_without_reset_count(tmp_path: Path) -> None:
    result = _run(tmp_path, b.no_reset_count(), _framing((6, 10), (11, 29)))
    (reboot,) = [e for e in result.events if e["kind"] == "reboot"]
    assert (reboot["slot_id"], reboot["reset_before"], reboot["reset_after"]) == (
        "motor-2", None, None
    )  # fmt: skip
    assert result.slots and all(r["reset_count_logged"] is False for r in result.slots)
    assert _row(result, "motor-2", "teleop")["reboots"] == 1
