from pathlib import Path

import pytest

from flashpoint import config


def test_lake_root_prefers_explicit_argument(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLASHPOINT_LAKE", str(tmp_path / "env"))
    assert config.lake_root(tmp_path / "arg") == tmp_path / "arg"


def test_lake_root_falls_back_to_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASHPOINT_LAKE", str(tmp_path / "env"))
    assert config.lake_root(None) == tmp_path / "env"


def test_lake_root_default_is_in_home(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLASHPOINT_LAKE", raising=False)
    assert config.lake_root(None) == Path.home() / "flashpoint-lake"


def test_cache_root_honours_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASHPOINT_CACHE", str(tmp_path))
    assert config.cache_root() == tmp_path


def test_pipeline_version_is_positive_int() -> None:
    assert isinstance(config.PIPELINE_VERSION, int)
    assert config.PIPELINE_VERSION >= 1
