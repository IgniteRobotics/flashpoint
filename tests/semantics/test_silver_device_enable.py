"""Silver keeps Phoenix 6 DeviceEnable, which hoots log as text (issue #5)."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from flashpoint import config
from flashpoint.ingest import Ingestor
from flashpoint.lake import bronze
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.query import connect
from flashpoint.readers.hoot import OwletRegistry
from flashpoint.semantics.derive import Deriver
from flashpoint.semantics.framing import frame
from flashpoint.semantics.identity import Observation
from flashpoint.semantics.silver import HootInSession, silver_dir, write_session
from tests.wpilog_builder import WpilogBuilder

Row = tuple[str, str, int, float | None, str | None]  # signal, type, ts_us, v_f64, v_str


def _bronze(lake: LakePaths, rows: list[Row]) -> None:
    n = len(rows)
    table = pa.table(
        {
            "signal": pa.array([r[0] for r in rows]).dictionary_encode(),
            "type": pa.array([r[1] for r in rows]).dictionary_encode(),
            "ts_us": pa.array([r[2] for r in rows], pa.int64()),
            "v_f64": pa.array([r[3] for r in rows], pa.float64()),
            "v_i64": pa.nulls(n, pa.int64()),
            "v_bool": pa.nulls(n, pa.bool_()),
            "v_str": pa.array([r[4] for r in rows], pa.large_string()),
            "v_bytes": pa.nulls(n, pa.large_binary()),
        }
    )
    bronze.write(lake, table, season="2026", log_id="h1", run_id="t")


def _silver(tmp_path: Path, rows: list[Row]) -> list[tuple[str, int, int, str, float]]:
    lake = LakePaths(tmp_path / "lake")
    lake.ensure()
    _bronze(lake, rows)
    framing = frame([(0, "Disabled"), (10_500, "Teleop"), (20_000, "Disabled")])
    observations = [Observation("s1", "hood", "ctre:A", "inventory", None, None)]
    write_session(
        connect(lake), lake, "s1", "2026", [HootInSession("h1", 10_000, {("TalonFX", 8): "hood"})],
        observations, framing, run_id="r",
    )  # fmt: skip
    out = pq.read_table(silver_dir(lake) / "season=2026" / "session_id=s1").to_pylist()
    return sorted((r["metric"], r["t_us"], r["match_time_us"], r["phase"], r["value"]) for r in out)


def test_text_enable_state_is_kept(tmp_path: Path) -> None:
    got = _silver(
        tmp_path,
        [
            ("Phoenix6/TalonFX-8/DeviceEnable", "string", 1_000, None, "Enabled"),
            ("Phoenix6/TalonFX-8/DeviceEnable", "string", 3_000, None, "Disabled"),
        ],
    )
    assert got == [
        ("device_enable", 11_000, 500, "teleop", 1.0),
        ("device_enable", 13_000, 2_500, "teleop", 0.0),
    ]


def test_unknown_text_is_dropped_and_other_metrics_kept(tmp_path: Path) -> None:
    got = _silver(
        tmp_path,
        [
            ("Phoenix6/TalonFX-8/DeviceEnable", "string", 1_000, None, "Foo"),
            ("Phoenix6/TalonFX-8/DeviceEnable", "string", 2_000, None, "enabled"),  # exact match
            ("Phoenix6/TalonFX-8/SupplyCurrent", "double", 1_000, 5.5, None),
        ],
    )
    assert got == [("supply_current", 11_000, 500, "teleop", 5.5)]


def test_text_on_other_signals_is_not_decoded(tmp_path: Path) -> None:
    got = _silver(
        tmp_path,
        [
            ("Phoenix6/TalonFX-8/MotorKT", "string", 1_000, None, "Enabled"),  # only DeviceEnable
            ("Phoenix6/TalonFX-8/SupplyVoltage", "double", 1_000, 12.25, "Enabled"),
        ],
    )
    assert got == [("supply_voltage", 11_000, 500, "teleop", 12.25)]


def test_new_derive_version_rederives_every_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lake = LakePaths(tmp_path / "lake")
    wpilog = (
        WpilogBuilder()
        .start(1, "DS:enabled", "boolean")
        .boolean(1, 1_000, True)
        .boolean(1, 5_000, False)
        .write(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog")
    )
    manifest = tmp_path / "empty-manifest.toml"
    manifest.write_text("")
    ingestor = Ingestor(lake, OwletRegistry(manifest, tmp_path / "owlet-cache"))
    try:
        ingestor.ingest([wpilog])
    finally:
        ingestor.close()
    config_root = tmp_path / "config"
    (config_root / "robots").mkdir(parents=True)
    (config_root / "seasons").mkdir()

    def derive() -> dict[str, int]:
        deriver = Deriver(lake, config_root)
        try:
            return deriver.run()
        finally:
            deriver.close()

    assert derive() == {"sessions": 1, "rebuilt": 1}
    assert derive() == {"sessions": 1, "rebuilt": 0}
    monkeypatch.setattr(config, "DERIVE_VERSION", config.DERIVE_VERSION + 1)
    assert derive() == {"sessions": 1, "rebuilt": 1}
