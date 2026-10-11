import json
import shutil
from pathlib import Path

import polars as pl
import pytest

from flashpoint import config
from flashpoint.anomaly import detect
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from tests.anomaly.gold_builder import GoldLake, steady


def _config(tmp_path: Path, anomaly_toml: str = "") -> Path:
    """A copy of the repo config with `anomaly.toml` replaced."""
    target = tmp_path / "config"
    shutil.copytree(config.config_root(), target)
    (target / "anomaly.toml").write_text(anomaly_toml)
    return target


def _run(lake: LakePaths, config_dir: Path, **kwargs: object) -> detect.DetectResult:
    return detect.detect(lake, config_dir, **kwargs)  # type: ignore[arg-type]


def _rows(lake: LakePaths) -> list[dict[str, object]]:
    return detect.read_anomalies(lake).to_dicts()


@pytest.fixture
def step_lake(tmp_path: Path) -> GoldLake:
    gold = GoldLake(tmp_path / "lake", matches=25, slots=["drive-fl", "drive-fr", "hood"])
    gold.set("hood", "temp_max_c", steady(45.0, 20) + steady(55.0, 5, start=3))
    return gold


def test_writes_table_and_summary(step_lake: GoldLake, tmp_path: Path) -> None:
    lake = step_lake.write()
    result = _run(lake, _config(tmp_path))
    assert result.ran
    out = lake.root / "gold" / "anomalies"
    assert (out / "anomalies.parquet").is_file() and (out / "summary.json").is_file()
    assert not list(out.glob("*.part"))
    step = [r for r in _rows(lake) if r["method"] == "STEP"]
    assert [(r["subject"], r["metric"], r["onset_match_key"]) for r in step] == [
        ("ctre:HOOD", "temp_max_c", "2026syn_qm21")
    ]
    summary = json.loads((out / "summary.json").read_text())
    assert summary["scanned_through"] == "2026syn_qm25"
    assert summary["counts"]["HIGH"] >= 1
    assert summary["building_baseline"]["count"] == 0


def test_rerun_is_row_identical_and_skipped_when_unchanged(
    step_lake: GoldLake, tmp_path: Path
) -> None:
    lake, cfg = step_lake.write(), _config(tmp_path)
    _run(lake, cfg)
    first = (lake.root / "gold" / "anomalies" / "anomalies.parquet").read_bytes()
    assert not _run(lake, cfg).ran  # fingerprint unchanged
    forced = _run(lake, cfg, force=True)
    assert forced.ran
    assert pl.read_parquet(lake.root / "gold/anomalies/anomalies.parquet").equals(
        pl.read_parquet(__import__("io").BytesIO(first))
    )


def test_config_error_keeps_the_previous_table(step_lake: GoldLake, tmp_path: Path) -> None:
    lake, cfg = step_lake.write(), _config(tmp_path)
    _run(lake, cfg)
    before = _rows(lake)
    (cfg / "anomaly.toml").write_text("[baseline]\ndrift_z_score = 2.5\n")
    with pytest.raises(detect.DetectConfigError, match=r"(?s)anomaly\.toml.*drift_z_score"):
        _run(lake, cfg)
    assert _rows(lake) == before


def test_raise_a_limit_needs_no_derive(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path / "lake", matches=6, slots=["intake-roller"])
    gold.set("intake-roller", "stall_s", [0, 0, 4.0, 0, 0, 0])
    lake = gold.write()
    _run(lake, _config(tmp_path))
    assert [r["metric"] for r in _rows(lake) if r["method"] == "SPEC"] == ["stall_s"]
    _run(lake, _config(tmp_path / "b", "[spec]\nstall_med_s = 10\n"))
    assert [r for r in _rows(lake) if r["metric"] == "stall_s"] == []


def test_restart_from_triage(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path / "lake", matches=30, slots=["hood"])
    gold.set("hood", "stator_current_p95", steady(40.0, 20) + steady(60.0, 10, start=2))
    gold.set("hood", "temp_max_c", steady(45.0, 20) + steady(55.0, 10, start=2))
    lake, cfg = gold.write(), _config(tmp_path)
    _run(lake, cfg)
    step = next(r for r in _rows(lake) if r["method"] == "STEP"
                and r["metric"] == "stator_current_p95")  # fmt: skip
    ledger = Ledger(lake.ledger)
    try:
        ledger.add_triage({
            "anomaly_id": step["id"], "from_state": "new", "to_state": "dismissed",
            "outcome": "gearbox ratio change", "restart_baseline": True,
            "subject": step["subject_key"], "metric": step["metric"],
            "onset_match_key": step["onset_match_key"],
        })  # fmt: skip
    finally:
        ledger.close()
    assert detect.restart_pending(lake)
    assert _run(lake, cfg).ran  # the restart revision is part of the fingerprint
    assert not detect.restart_pending(lake)
    current = [r for r in _rows(lake) if r["metric"] == "stator_current_p95"
               and r["method"] in ("STEP", "DRIFT")]  # fmt: skip
    assert current == []
    temps = [r for r in _rows(lake) if r["metric"] == "temp_max_c" and r["method"] == "STEP"]
    assert len(temps) == 1  # other metrics unchanged
    assert json.loads((lake.root / "gold/anomalies/summary.json").read_text())["restart_seq"] == 1


def test_restarts_ignored_when_disabled(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path / "lake", matches=30, slots=["hood"])
    gold.set("hood", "stator_current_p95", steady(40.0, 20) + steady(60.0, 10, start=2))
    lake, cfg = gold.write(), _config(tmp_path)
    ledger = Ledger(lake.ledger)
    try:
        ledger.add_triage({
            "anomaly_id": "x" * 16, "from_state": "new", "to_state": "dismissed",
            "outcome": "r", "restart_baseline": True, "subject": "ctre:HOOD",
            "metric": "stator_current_p95", "onset_match_key": "2026syn_qm21",
        })  # fmt: skip
    finally:
        ledger.close()
    rows = detect.compute(lake, detect.load_settings(cfg), restarts=False).rows
    assert any(r["method"] == "STEP" for r in rows)


def test_empty_lake(tmp_path: Path) -> None:
    lake = LakePaths(tmp_path / "lake")
    result = _run(lake, _config(tmp_path))
    assert result.ran and _rows(lake) == []
    assert result.summary["scanned_through"] is None
