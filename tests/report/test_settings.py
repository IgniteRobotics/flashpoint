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
