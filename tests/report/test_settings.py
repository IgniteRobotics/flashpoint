from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.report.settings import ReportConfig, ReportConfigError, load_report_config


def test_repo_config_matches_defaults() -> None:
    assert load_report_config(config.config_root()) == ReportConfig()


def test_missing_file_is_defaults(tmp_path: Path) -> None:
    assert load_report_config(tmp_path) == ReportConfig()


def test_override(tmp_path: Path) -> None:
    (tmp_path / "report.toml").write_text("[thresholds]\ntemp_warn_c = 60\n")
    assert load_report_config(tmp_path).temp_warn_c == 60.0


@pytest.mark.parametrize(
    "text",
    ["[thresholds]\ntemp_wran_c = 60\n", "[other]\n", "[thresholds]\ntemp_warn_c = 'x'\n", "["],
)
def test_bad_config(tmp_path: Path, text: str) -> None:
    (tmp_path / "report.toml").write_text(text)
    with pytest.raises(ReportConfigError):
        load_report_config(tmp_path)


def test_dropout_threshold_default_and_override(tmp_path: Path) -> None:
    assert ReportConfig().dropout_warn_s == 0.25
    (tmp_path / "report.toml").write_text("[thresholds]\ndropout_warn_s = 0.5\n")
    assert load_report_config(tmp_path).dropout_warn_s == 0.5


def test_dropout_threshold_below_event_floor_rejected(tmp_path: Path) -> None:
    (tmp_path / "report.toml").write_text("[thresholds]\ndropout_warn_s = 0.05\n")
    with pytest.raises(ReportConfigError, match=r"dropout_warn_s.*0\.1"):
        load_report_config(tmp_path)


def test_existing_sample_gap_key_still_loads(tmp_path: Path) -> None:
    (tmp_path / "report.toml").write_text("[thresholds]\nsample_gap_s = 2.0\n")
    assert load_report_config(tmp_path).sample_gap_s == 2.0
