from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from flashpoint.ingest import Ingestor
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.query import connect
from flashpoint.readers.hoot import OwletRegistry
from flashpoint.semantics.derive import Deriver
from flashpoint.semantics.signals import signals_dir
from tests.wpilog_builder import WpilogBuilder

ROBOT_ROOT = "NT:Robot/m_robotContainer/"
SEASON = """
season = {season}
[roots]
robot = "NT:Robot/m_robotContainer/"
photonvision = "NT:/photonvision/"
camera_publisher = "NT:/CameraPublisher/"
preferences = "NT:/Preferences/"
fms = "NT:/FMSInfo/"
"""
ROBOT = """
robot = "{robot}"
season = {season}
project = "Robot-{season}"
[[slot]]
id = "a"
bus = "rio"
model = "TalonFX"
can_id = 1
subsystem = "s"
role = "x"
"""
SIGNALS = """
[[signal]]
id = "shooter-velocity-setpoint"
root = "robot"
entry = "Shooter/Velocity Setpoint"
subsystem = "shooter"
metric = "velocity_setpoint"

[[signal]]
id = "shooter-shot-count"
root = "robot"
entry = "Shooter/Shot Count"
subsystem = "shooter"
metric = "shot_count"

[[signal]]
id = "intake-beam"
root = "robot"
entry = "Intake/Beam/Is Detected"
subsystem = "intake"
component = "beam-break"
metric = "is_detected"

[[signal]]
id = "front-target-pitch"
root = "photonvision"
entry = "FRONT/targetPitch"
subsystem = "vision"
component = "front"
metric = "target_pitch"
"""
EXTRA_SIGNALS = """
[[signal]]
id = "intake-renamed"
root = "robot"
entry = "Intake/Old Name"
subsystem = "intake"
metric = "is_detected"

[[signal]]
id = "intake-state"
root = "robot"
entry = "Intake/State"
subsystem = "intake"
metric = "state"

[[signal]]
id = "front-target-pose"
root = "photonvision"
entry = "FRONT/targetPose"
subsystem = "vision"
component = "front"
metric = "target_pose"
"""


def _wpilog(path: Path, ds: bool = True, shots: int = 3) -> Path:
    b = (
        WpilogBuilder()
        .start(1, ROBOT_ROOT + "Shooter/Velocity Setpoint", "double")
        .start(2, ROBOT_ROOT + "Shooter/Shot Count", "int64")
        .start(3, ROBOT_ROOT + "Intake/Beam/Is Detected", "boolean")
        .start(4, "NT:/SmartDashboard/Foo", "double")
        .start(7, "NT:/photonvision/FRONT/targetPitch", "double")
        .start(8, "NT:/photonvision/FRONT/targetPose", "struct:Transform3d")
        .start(9, ROBOT_ROOT + "Intake/State", "string")
    )
    b.double(1, 1_000, 10.0).double(1, 5_000, 20.0)
    b.int64(2, 2_000, shots)
    for ts, value in ((1_000, False), (2_000, True), (3_000, False), (4_000, True), (5_000, False)):
        b.boolean(3, ts, value)
    b.double(4, 1_000, 1.0)
    b.double(7, 2_500, 1.5)
    b.record(8, 2_500, b"\x00" * 56)
    b.string(9, 2_500, "IDLE")
    if ds:
        b.start(5, "DS:enabled", "boolean").start(6, "DS:autonomous", "boolean")
        b.boolean(5, 1_500, True).boolean(6, 1_500, True)
        b.boolean(6, 3_000, False)
        b.boolean(5, 4_500, False)
    return b.write(path)


def _config(
    root: Path, robots: dict[str, tuple[int, str]], seasons: tuple[int, ...] = (2026,)
) -> Path:
    """robots: robot id -> (season, extra TOML for its [[signal]] tables)."""
    (root / "robots").mkdir(parents=True, exist_ok=True)
    (root / "seasons").mkdir(exist_ok=True)
    for season in seasons:
        (root / "seasons" / f"{season}.toml").write_text(SEASON.format(season=season))
    for robot, (season, signals) in robots.items():
        text = ROBOT.format(robot=robot, season=season) + signals
        (root / "robots" / f"{robot}.toml").write_text(text)
    return root


@pytest.fixture
def registry(tmp_path: Path) -> OwletRegistry:
    manifest = tmp_path / "empty-manifest.toml"
    manifest.write_text("")
    return OwletRegistry(manifest, tmp_path / "owlet-cache")


def _ingest(lake: LakePaths, registry: OwletRegistry, *paths: Path) -> None:
    ingestor = Ingestor(lake, registry)
    try:
        ingestor.ingest(paths)
    finally:
        ingestor.close()


def _derive(lake: LakePaths, config: Path) -> dict[str, int]:
    deriver = Deriver(lake, config)
    try:
        return deriver.run()
    finally:
        deriver.close()


def _sessions(lake: LakePaths) -> dict[str, dict[str, Any]]:
    ledger = Ledger(lake.ledger)
    try:
        return {r["session_id"]: r for r in ledger.query("SELECT * FROM sessions")}
    finally:
        ledger.close()


def _signal_rows(lake: LakePaths) -> list[dict[str, Any]]:
    parts = sorted(signals_dir(lake).glob("season=*/session_id=*"))
    return [row for part in parts for row in pq.read_table(part).to_pylist()]


def _missing(lake: LakePaths) -> set[tuple[str, str]]:
    ledger = Ledger(lake.ledger)
    try:
        rows = ledger.query("SELECT signal_id, reason FROM missing_signals")
    finally:
        ledger.close()
    return {(r["signal_id"], r["reason"]) for r in rows}


@pytest.fixture
def lake(tmp_path: Path) -> LakePaths:
    return LakePaths(tmp_path / "lake")


def test_numeric_and_boolean_signals_with_labels_and_framing(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    _ingest(lake, registry, _wpilog(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog"))
    _derive(lake, _config(tmp_path / "config", {"r": (2026, SIGNALS)}))

    rows = _signal_rows(lake)
    by_signal: dict[str, list[tuple[Any, ...]]] = {}
    for r in sorted(rows, key=lambda r: (r["signal_id"], r["t_us"])):
        by_signal.setdefault(r["signal_id"], []).append(
            (r["t_us"], r["match_time_us"], r["phase"], r["value"])
        )
    # Match starts at the first enabled sample (auto at 1500).
    assert by_signal["shooter-velocity-setpoint"] == [
        (1_000, -500, "pre", 10.0),
        (5_000, 3_500, "post", 20.0),
    ]
    assert by_signal["shooter-shot-count"] == [(2_000, 500, "auto", 3.0)]
    assert [(t, v) for t, _, _, v in by_signal["intake-beam"]] == [
        (1_000, 0.0), (2_000, 1.0), (3_000, 0.0), (4_000, 1.0), (5_000, 0.0),
    ]  # fmt: skip
    assert by_signal["front-target-pitch"] == [(2_500, 1_000, "auto", 1.5)]
    beam = next(r for r in rows if r["signal_id"] == "intake-beam")
    assert (beam["subsystem"], beam["component"], beam["metric"]) == (
        "intake",
        "beam-break",
        "is_detected",
    )
    shot = next(r for r in rows if r["signal_id"] == "shooter-shot-count")
    assert shot["component"] is None
    assert {r["session_id"] for r in rows} == set(_sessions(lake))


def test_undeclared_entry_stays_in_bronze_only(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    _ingest(lake, registry, _wpilog(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog"))
    _derive(lake, _config(tmp_path / "config", {"r": (2026, SIGNALS)}))

    assert {r["signal_id"] for r in _signal_rows(lake)} == {
        "shooter-velocity-setpoint", "shooter-shot-count", "intake-beam", "front-target-pitch",
    }  # fmt: skip
    con = connect(lake)
    assert con.sql(
        "SELECT count(*) FROM samples WHERE signal = 'NT:/SmartDashboard/Foo'"
    ).fetchone() == (1,)


def test_unknown_robot_gets_no_signals(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    _ingest(lake, registry, _wpilog(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog"))
    # Two robots in the season and no project in the log: the session's robot is unknown.
    _derive(lake, _config(tmp_path / "config", {"r": (2026, SIGNALS), "s": (2026, SIGNALS)}))

    assert [s["robot"] for s in _sessions(lake).values()] == ["unknown"]
    assert _signal_rows(lake) == []
    assert _missing(lake) == set()


@pytest.mark.parametrize(
    ("name", "ds"),
    [
        ("FRC_20260409_215113.wpilog", True),  # practice: DS-framed but no match identity
        ("FRC_20260409_215113_GACMP_Q7.wpilog", False),  # a match with no DS enable data
    ],
)
def test_unframed_session_has_empty_match_time_and_phase(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path, name: str, ds: bool
) -> None:
    _ingest(lake, registry, _wpilog(tmp_path / name, ds=ds))
    _derive(lake, _config(tmp_path / "config", {"r": (2026, SIGNALS)}))

    rows = _signal_rows(lake)
    assert len(rows) == 9
    assert {(r["match_time_us"], r["phase"]) for r in rows} == {(None, None)}


def test_editing_one_signal_rederives_only_that_robots_sessions(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    _ingest(
        lake,
        registry,
        _wpilog(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog"),
        _wpilog(tmp_path / "FRC_20250307_213719_GADAL_Q30.wpilog", shots=4),
    )
    robots = {"r26": (2026, SIGNALS), "r25": (2025, SIGNALS)}
    config = _config(tmp_path / "config", robots, seasons=(2025, 2026))
    assert _derive(lake, config) == {"sessions": 2, "rebuilt": 2}
    assert _derive(lake, config) == {"sessions": 2, "rebuilt": 0}

    edited = SIGNALS.replace('metric = "shot_count"', 'metric = "shots"')
    _config(config, {"r26": (2026, edited)})
    assert _derive(lake, config) == {"sessions": 2, "rebuilt": 1}
    metrics = {(r["signal_id"], r["metric"]) for r in _signal_rows(lake)}
    assert ("shooter-shot-count", "shots") in metrics
    assert ("shooter-shot-count", "shot_count") in metrics  # the 2025 session is untouched


def test_season_root_change_rederives_that_season(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    _ingest(lake, registry, _wpilog(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog"))
    config = _config(tmp_path / "config", {"r": (2026, SIGNALS)})
    _derive(lake, config)
    season = config / "seasons" / "2026.toml"
    season.write_text(season.read_text().replace("NT:/photonvision/", "NT:/photon/"))

    assert _derive(lake, config) == {"sessions": 1, "rebuilt": 1}
    assert "front-target-pitch" not in {r["signal_id"] for r in _signal_rows(lake)}


def test_deleting_all_signals_leaves_no_partitions(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    _ingest(lake, registry, _wpilog(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog"))
    config = _config(tmp_path / "config", {"r": (2026, SIGNALS)})
    _derive(lake, config)
    assert list(signals_dir(lake).glob("season=*/session_id=*"))

    _config(config, {"r": (2026, "")})
    _derive(lake, config)
    assert not list(signals_dir(lake).glob("season=*/session_id=*"))
    assert not (lake.root / "silver" / "_staging").exists()


def test_signals_view(lake: LakePaths, registry: OwletRegistry, tmp_path: Path) -> None:
    assert connect(lake).sql("SELECT count(*) FROM signals").fetchone() == (0,)
    _ingest(lake, registry, _wpilog(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog"))
    _derive(lake, _config(tmp_path / "config", {"r": (2026, SIGNALS)}))

    rows = (
        connect(lake)
        .sql("SELECT season, signal_id, count(*) FROM signals GROUP BY ALL ORDER BY signal_id")
        .fetchall()
    )
    assert rows == [
        ("2026", "front-target-pitch", 1),
        ("2026", "intake-beam", 5),
        ("2026", "shooter-shot-count", 1),
        ("2026", "shooter-velocity-setpoint", 2),
    ]


# --- missing and unusable signals (group 3) ------------------------------------------------


def test_missing_and_non_numeric_signals_reported(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _ingest(lake, registry, _wpilog(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog"))
    with caplog.at_level("WARNING", logger="flashpoint.semantics.signals"):
        _derive(lake, _config(tmp_path / "config", {"r": (2026, SIGNALS + EXTRA_SIGNALS)}))

    assert _missing(lake) == {
        ("intake-renamed", "missing"),
        ("intake-state", "non-numeric"),
        ("front-target-pose", "non-numeric"),
    }
    written = {r["signal_id"] for r in _signal_rows(lake)}
    assert written == {
        "shooter-velocity-setpoint", "shooter-shot-count", "intake-beam", "front-target-pitch",
    }  # fmt: skip
    warnings = [r for r in caplog.records if "signal" in r.getMessage()]
    assert len(warnings) == 1
    assert "intake-renamed" in warnings[0].getMessage()
    con = connect(lake)
    assert con.sql("SELECT count(*) FROM meta_missing_signals").fetchone() == (3,)


def test_truncated_wpilog_keeps_signals_before_truncation(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    path = tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog"
    b = WpilogBuilder().start(1, ROBOT_ROOT + "Shooter/Velocity Setpoint", "double")
    for ts in range(1_000, 11_000, 1_000):
        b.double(1, ts, float(ts))
    b.write(path, truncate_tail=3)  # cut the last record mid-payload
    _ingest(lake, registry, path)
    _derive(lake, _config(tmp_path / "config", {"r": (2026, SIGNALS)}))

    ledger = Ledger(lake.ledger)
    try:
        stages = {r["stage"] for r in ledger.query("SELECT stage FROM files")}
    finally:
        ledger.close()
    assert stages == {"success"}
    times = sorted(
        r["t_us"] for r in _signal_rows(lake) if r["signal_id"] == "shooter-velocity-setpoint"
    )
    assert times == list(range(1_000, 10_000, 1_000))
    assert ("shooter-velocity-setpoint", "missing") not in _missing(lake)
