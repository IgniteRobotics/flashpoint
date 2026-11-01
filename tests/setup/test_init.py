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
    answers = iter(["6829", str(tmp_path / "asked-lake"), "", ""])
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt="": next(answers))
    assert main(["init"]) == 0
    acquire = AcquireConfig.load(root)
    assert (acquire.team, acquire.lake) == (6829, tmp_path / "asked-lake")


def test_blank_lake_answer_takes_the_default(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    answers = iter(["254", "", "", ""])
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


# --- team-branding: init --team-name --palette ----------------------------------------------


def _theme(root: Path) -> dict[str, object]:
    with (root / "theme.toml").open("rb") as f:
        return tomllib.load(f)


def test_init_with_a_preset(root: Path, tmp_path: Path) -> None:
    """Init with a preset: theme.toml names the team and preset, and `serve` shows them."""
    from flashpoint.theme import load_theme, preset_tokens
    from flashpoint.web.branding import clear_cache, load_branding

    args = ["init", "--team", "254", "--team-name", "The Cheesy Poofs", "--palette", "blue"]
    assert main([*args, "--lake", str(tmp_path / "lake")]) == 0
    assert _theme(root) == {"team_name": "The Cheesy Poofs", "palette": "blue"}
    assert load_theme(root).config.team_name == "The Cheesy Poofs"
    clear_cache()
    branding = load_branding(root)  # what `serve` sends: theme.css and mode.js's team
    assert branding.team == {"name": "The Cheesy Poofs", "number": 254}
    assert f"--accent: {preset_tokens('blue')['accent']};" in branding.css
    text = (root / "theme.toml").read_text()
    assert "# team_number" in text and "# [custom]" in text  # the bundled comments are kept


def test_init_without_a_theme_copies_the_default(root: Path, tmp_path: Path) -> None:
    assert main(["init", "--team", "254", "--lake", str(tmp_path / "lake")]) == 0
    assert (root / "theme.toml").read_bytes() == (config.DEFAULTS_ROOT / "theme.toml").read_bytes()


@pytest.mark.parametrize("name", ['Team "Quotes" \\ Back', "Équipe Ünïcode 🤖", "x" * 40])
def test_team_name_round_trips(root: Path, tmp_path: Path, name: str) -> None:
    init_config(root, team=254, lake=tmp_path / "lake", team_name=name)
    assert _theme(root)["team_name"] == name


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        (["--team-name", "x" * 41], "40 characters"),
        (["--palette", "purple"], "palette"),
    ],
)
def test_bad_theme_writes_nothing(
    root: Path, tmp_path: Path, extra: list[str], message: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["init", "--team", "254", "--lake", str(tmp_path / "lake"), *extra]) != 0
    assert message in capsys.readouterr().err
    assert not root.exists() and not (tmp_path / "lake").exists()


def test_theme_prompts_on_a_terminal(
    root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prompts: list[str] = []
    answers = iter(["6829", str(tmp_path / "lake"), "  Ignite Robotics ", "green"])

    def ask(prompt: str = "") -> str:
        prompts.append(prompt)
        return next(answers)

    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", ask)
    assert main(["init"]) == 0
    assert _theme(root) == {"team_name": "Ignite Robotics", "palette": "green"}
    assert all(name in prompts[3] for name in ("amber", "green", "blue", "red", "white"))


def test_blank_theme_answers_keep_the_default(
    root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    answers = iter(["254", str(tmp_path / "lake"), "", ""])
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt="": next(answers))
    assert main(["init"]) == 0
    assert (root / "theme.toml").read_bytes() == (config.DEFAULTS_ROOT / "theme.toml").read_bytes()


def test_bad_palette_answer_is_refused(
    root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    answers = iter(["254", str(tmp_path / "lake"), "", "purple"])
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt="": next(answers))
    assert main(["init"]) != 0
    assert "purple" in capsys.readouterr().err and not root.exists()


def test_given_theme_options_are_not_asked(
    root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    answers = iter(["254"])
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt="": next(answers))
    lake = str(tmp_path / "lake")
    assert main(["init", "--lake", lake, "--team-name", "", "--palette", "red"]) == 0
    assert _theme(root) == {"palette": "red"}  # an empty name means no name


def test_no_theme_prompt_without_a_terminal(
    root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    def no_input(prompt: str = "") -> str:
        raise AssertionError("init prompted without a terminal")

    monkeypatch.setattr("builtins.input", no_input)
    assert main(["init", "--team", "254", "--lake", str(tmp_path / "lake")]) == 0
    assert _theme(root) == {"palette": "amber"}
