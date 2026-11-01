"""The theme in the setup checks (team-branding "Broken theme file")."""

import shutil
from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.cli import main
from flashpoint.setup.checks import CheckResult, run_checks
from tests.setup.test_checks import _by_id, _context, _ignite


def test_default_theme_is_ok(tmp_path: Path) -> None:
    checks = _by_id(run(_ignite(tmp_path), tmp_path))
    found = checks["file:theme.toml"]
    assert found.status == "ok" and found.default and found.source == "theme.toml"


def test_team_theme_is_ok(tmp_path: Path) -> None:
    root = _ignite(tmp_path)
    (root / "theme.toml").write_text('team_name = "Ignite Robotics"\npalette = "green"\n')
    found = _by_id(run(root, tmp_path))["file:theme.toml"]
    assert found.status == "ok" and not found.default


def test_unknown_key_is_a_problem_naming_the_file_and_key(tmp_path: Path) -> None:
    root = _ignite(tmp_path)
    (root / "theme.toml").write_text('palette = "amber"\ncolour = "red"\n')
    found = _by_id(run(root, tmp_path))["file:theme.toml"]
    assert found.status == "problem"
    assert "theme.toml" in found.message and "colour" in found.message
    assert found.command is not None and "theme.toml" in found.command


def test_refused_palette_is_a_problem_with_the_reason(tmp_path: Path) -> None:
    root = _ignite(tmp_path)
    (root / "theme.toml").write_text('palette = "custom"\n[custom]\nprimary = "#202020"\n')
    found = _by_id(run(root, tmp_path))["file:theme.toml"]
    assert found.status == "problem"
    assert "text contrast" in found.message and "#202020" in found.message


@pytest.mark.parametrize(
    "text",
    ['palette = "custom"\n[custom]\nprimary = "#202020"\n', 'palette = "amber"\ncolour = "red"\n'],
)
def test_problem_names_the_file_once(tmp_path: Path, text: str) -> None:
    root = _ignite(tmp_path)
    (root / "theme.toml").write_text(text)
    found = _by_id(run(root, tmp_path))["file:theme.toml"]
    assert found.message.count("theme.toml") == 1, found.message


def test_doctor_reports_a_broken_theme(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "cfg"
    shutil.copytree(config.config_root(), root)
    (root / "theme.toml").write_text('palette = "amber"\ncolour = "red"\n')
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    assert main(["doctor", "--lake", str(tmp_path / "lake")]) == 1
    out = capsys.readouterr().out
    line = next(x for x in out.splitlines() if "PROBLEM" in x and "theme.toml" in x)
    assert "colour" in line


def run(root: Path, tmp_path: Path) -> list[CheckResult]:
    return run_checks(_context(root, tmp_path / "lake"))
