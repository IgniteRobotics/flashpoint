from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.anomaly.settings import (
    BASELINE_METRICS,
    AnomalyConfig,
    AnomalyConfigError,
    load_anomaly_config,
)


def _write(tmp_path: Path, text: str) -> Path:
    (tmp_path / "anomaly.toml").write_text(text)
    return tmp_path


def test_repo_file_matches_defaults() -> None:
    assert load_anomaly_config(config.config_root()) == AnomalyConfig()


def test_missing_file_is_defaults(tmp_path: Path) -> None:
    assert load_anomaly_config(tmp_path) == AnomalyConfig()


def test_defaults() -> None:
    cfg = AnomalyConfig()
    assert (cfg.spec.temp_max_med_c, cfg.spec.temp_max_high_c, cfg.spec.temp_mean_med_c) == (
        65.0,
        75.0,
        55.0,
    )
    assert cfg.spec.stall_med_s == 5.0
    assert (cfg.spec.gap_med_ms, cfg.spec.gap_high_ms) == (250.0, 1000.0)
    assert (cfg.baseline.window, cfg.baseline.min_baseline) == (20, 5)
    assert (cfg.baseline.step_z, cfg.baseline.step_persist, cfg.baseline.drift_z) == (3.5, 3, 2.5)
    assert tuple(cfg.baseline.metrics) == BASELINE_METRICS
    assert cfg.baseline.floors["residual_p95_a"] == 5.0
    assert cfg.sibling.z == 3.0
    assert (cfg.strict.step_z, cfg.strict.drift_z, cfg.strict.sibling_z) == (2.5, 1.5, 2.0)
    assert cfg.strict.persist == 2
    assert cfg.model.enabled is False


def test_override(tmp_path: Path) -> None:
    cfg = load_anomaly_config(_write(tmp_path, "[spec]\nstall_med_s = 10\n"))
    assert cfg.spec.stall_med_s == 10.0
    assert cfg.baseline == AnomalyConfig().baseline


def test_unknown_key_names_file_and_key(tmp_path: Path) -> None:
    with pytest.raises(AnomalyConfigError, match=r"(?s)anomaly\.toml.*drift_z_score"):
        load_anomaly_config(_write(tmp_path, "[baseline]\ndrift_z_score = 2.5\n"))


def test_unknown_section_rejected(tmp_path: Path) -> None:
    with pytest.raises(AnomalyConfigError, match=r"(?s)anomaly\.toml.*alerts"):
        load_anomaly_config(_write(tmp_path, "[alerts]\nslack = true\n"))


def test_negative_threshold_rejected(tmp_path: Path) -> None:
    with pytest.raises(AnomalyConfigError, match=r"(?s)anomaly\.toml.*step_z"):
        load_anomaly_config(_write(tmp_path, "[baseline]\nstep_z = -1\n"))


def test_negative_floor_rejected(tmp_path: Path) -> None:
    with pytest.raises(AnomalyConfigError, match=r"(?s)anomaly\.toml.*temp_max_c"):
        load_anomaly_config(_write(tmp_path, "[baseline.floors]\ntemp_max_c = -1\n"))


def test_strict_above_normal_rejected(tmp_path: Path) -> None:
    with pytest.raises(AnomalyConfigError, match=r"(?s)anomaly\.toml.*strict.*drift_z"):
        load_anomaly_config(_write(tmp_path, "[strict]\ndrift_z = 3.0\n"))


def test_strict_persist_above_normal_rejected(tmp_path: Path) -> None:
    with pytest.raises(AnomalyConfigError, match=r"(?s)anomaly\.toml.*persist"):
        load_anomaly_config(_write(tmp_path, "[strict]\npersist = 4\n"))


def test_unknown_metric_rejected(tmp_path: Path) -> None:
    with pytest.raises(AnomalyConfigError, match=r"(?s)anomaly\.toml.*voltage_p95"):
        load_anomaly_config(_write(tmp_path, '[baseline]\nmetrics = ["voltage_p95"]\n'))


def test_metric_subset(tmp_path: Path) -> None:
    cfg = load_anomaly_config(_write(tmp_path, '[baseline]\nmetrics = ["temp_max_c"]\n'))
    assert cfg.baseline.metrics == ["temp_max_c"]


def test_bad_toml(tmp_path: Path) -> None:
    with pytest.raises(AnomalyConfigError, match="anomaly.toml"):
        load_anomaly_config(_write(tmp_path, "["))


def test_hash_changes_with_values(tmp_path: Path) -> None:
    a = load_anomaly_config(tmp_path)
    b = load_anomaly_config(_write(tmp_path, "[spec]\nstall_med_s = 10\n"))
    assert a.digest() != b.digest()
    assert a.digest() == AnomalyConfig().digest()
