"""The lake root is absolute from every source, taken from the current folder (#107)."""

import tomllib
from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.cli import main


@pytest.fixture
def work(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An empty configuration folder, no lake variable, and the current folder `work/`."""
    folder = tmp_path / "work"
    folder.mkdir()
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(tmp_path / "cfg"))
    monkeypatch.delenv("FLASHPOINT_LAKE", raising=False)
    monkeypatch.chdir(folder)
    return folder


def _acquire_toml(tmp_path: Path, lake: str) -> None:
    root = tmp_path / "cfg"
    root.mkdir(exist_ok=True)
    (root / "acquire.toml").write_text(f"lake = '{lake}'\n")


def test_relative_argument_is_taken_from_the_current_folder(work: Path) -> None:
    assert config.lake_root(Path("lake")) == work / "lake"


def test_relative_env_is_taken_from_the_current_folder(
    work: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLASHPOINT_LAKE", "lake")
    assert config.lake_root(None) == work / "lake"


def test_relative_acquire_toml_is_taken_from_the_current_folder(work: Path, tmp_path: Path) -> None:
    _acquire_toml(tmp_path, "lake")
    assert config.lake_root(None) == work / "lake"  # not cfg/lake


@pytest.mark.parametrize("source", ["argument", "env", "acquire.toml"])
def test_home_is_expanded_from_every_source(
    work: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    explicit = Path("~/frc-lake") if source == "argument" else None
    if source == "env":
        monkeypatch.setenv("FLASHPOINT_LAKE", "~/frc-lake")
    if source == "acquire.toml":
        _acquire_toml(tmp_path, "~/frc-lake")
    assert config.lake_root(explicit) == Path.home() / "frc-lake"


def test_parent_folder_is_normalised(work: Path, tmp_path: Path) -> None:
    root = config.lake_root(Path("../lake"))
    assert root == tmp_path / "lake"
    assert ".." not in root.parts


def test_default_is_in_home(work: Path) -> None:
    assert config.lake_root(None) == Path.home() / "flashpoint-lake"


def test_init_stores_an_absolute_lake(work: Path, tmp_path: Path) -> None:
    assert main(["init", "--team", "254", "--lake", "lake"]) == 0
    with (tmp_path / "cfg" / "acquire.toml").open("rb") as f:
        assert Path(tomllib.load(f)["lake"]) == work / "lake"
