import os
import tomllib
from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.acquire.config import AcquireConfig
from flashpoint.cli import main
from flashpoint.setup.init import InitError, init_config

DEFAULT_FILES = [
    "report.toml",
    "anomaly.toml",
    "anomaly-causes.toml",
    "seasons/2025.toml",
    "seasons/2026.toml",
    "units/units-seed.csv",
]


@pytest.fixture
def root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "cfg"
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(path))
    monkeypatch.delenv("FLASHPOINT_LAKE", raising=False)
    return path


def test_first_run(root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    lake = tmp_path / "frc-lake"
    assert main(["init", "--team", "254", "--lake", str(lake)]) == 0
    acquire = AcquireConfig.load(root)
    assert acquire.team == 254 and acquire.lake == lake
    for name in DEFAULT_FILES:
        assert (root / name).read_bytes() == (config.DEFAULTS_ROOT / name).read_bytes(), name
    assert (root / "robots").is_dir() and lake.is_dir()
    assert config.lake_root(None) == lake  # the lake comes from acquire.toml from now on
    out = capsys.readouterr().out
    assert str(root) in out and "acquire.toml" in out


def test_second_run_changes_nothing(
    root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["init", "--team", "254", "--lake", str(tmp_path / "lake")]) == 0
    (root / "report.toml").write_text("# hand edit\n")
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    capsys.readouterr()

    assert main(["init", "--team", "1678"]) == 0
    after = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    assert after == before
    out = capsys.readouterr().out
    assert "already exists" in out and "acquire.toml" in out and "report.toml" in out
    assert "team" in out  # how to change the team number


@pytest.mark.parametrize("team", ["0", "100000", "-1", "abc"])
def test_bad_team_number(
    root: Path, tmp_path: Path, team: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["init", "--team", team, "--lake", str(tmp_path / "lake")]) != 0
    assert "team number" in capsys.readouterr().err
    assert not root.exists() and not (tmp_path / "lake").exists()


def test_prompts_on_a_terminal(root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    answers = iter(["6829", str(tmp_path / "asked-lake")])
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt="": next(answers))
    assert main(["init"]) == 0
    acquire = AcquireConfig.load(root)
    assert (acquire.team, acquire.lake) == (6829, tmp_path / "asked-lake")


def test_blank_lake_answer_takes_the_default(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    answers = iter(["254", ""])
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt="": next(answers))
    monkeypatch.setattr(Path, "home", lambda: root.parent / "home")
    assert main(["init"]) == 0
    assert AcquireConfig.load(root).lake == root.parent / "home" / "flashpoint-lake"


def test_no_prompt_without_a_terminal(
    root: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    def no_input(prompt: str = "") -> str:
        raise AssertionError("init prompted without a terminal")

    monkeypatch.setattr("builtins.input", no_input)
    assert main(["init"]) != 0
    assert "--team" in capsys.readouterr().err
    assert not root.exists()


def test_lake_path_with_quotes_and_backslashes(root: Path, tmp_path: Path) -> None:
    # Windows paths carry backslashes already, and can't hold one in a folder name.
    lake = tmp_path / ("it's a lake" if os.name == "nt" else "it's a \\ lake")
    init_config(root, team=254, lake=lake)
    with (root / "acquire.toml").open("rb") as f:
        assert Path(tomllib.load(f)["lake"]) == lake


def test_init_config_refuses_a_bad_team(root: Path, tmp_path: Path) -> None:
    with pytest.raises(InitError, match="team number"):
        init_config(root, team=0, lake=tmp_path / "lake")
    assert not root.exists()
