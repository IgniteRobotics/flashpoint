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


def test_lake_root_default_is_in_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLASHPOINT_LAKE", raising=False)
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(tmp_path / "empty"))
    assert config.lake_root(None) == Path.home() / "flashpoint-lake"


def test_lake_root_reads_acquire_toml_after_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "cfg"
    root.mkdir()
    (root / "acquire.toml").write_text(f"lake = '{tmp_path / 'team-lake'}'\n")
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    monkeypatch.setenv("FLASHPOINT_LAKE", str(tmp_path / "env"))
    assert config.lake_root(None) == tmp_path / "env"
    monkeypatch.delenv("FLASHPOINT_LAKE")
    assert config.lake_root(None) == tmp_path / "team-lake"
    (root / "acquire.toml").write_text("lake = '~/frc-lake'\n")
    assert config.lake_root(None) == Path.home() / "frc-lake"


def test_cache_root_honours_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASHPOINT_CACHE", str(tmp_path))
    assert config.cache_root() == tmp_path


def test_pipeline_version_is_positive_int() -> None:
    assert isinstance(config.PIPELINE_VERSION, int)
    assert config.PIPELINE_VERSION >= 1


def test_config_root_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(tmp_path / "env"))
    assert config.config_root(tmp_path / "arg") == tmp_path / "arg"
    assert config.config_root() == tmp_path / "env"


@pytest.mark.parametrize(
    ("system", "env", "expected"),
    [
        ("Linux", {}, Path.home() / ".config" / "flashpoint"),
        ("Darwin", {}, Path.home() / ".config" / "flashpoint"),
        ("Linux", {"XDG_CONFIG_HOME": "/x/cfg"}, Path("/x/cfg/flashpoint")),
        ("Windows", {"APPDATA": "/x/Roaming"}, Path("/x/Roaming/flashpoint")),
    ],
)
def test_config_root_per_user_folder(
    system: str,
    env: dict[str, str],
    expected: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in ("FLASHPOINT_CONFIG", "XDG_CONFIG_HOME", "APPDATA"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr("platform.system", lambda: system)
    assert config.config_root() == expected


def test_no_checkout_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """A source checkout's config/ is never used unless FLASHPOINT_CONFIG names it."""
    monkeypatch.delenv("FLASHPOINT_CONFIG")
    repo_config = Path(__file__).resolve().parents[1] / "config"
    assert config.config_root() != repo_config


def test_conftest_points_at_the_repo_config() -> None:
    assert (config.config_root() / "robots" / "2026-comp.toml").is_file()


def test_config_file_falls_back_to_the_bundled_default(tmp_path: Path) -> None:
    """Default thresholds: a root with no report.toml uses the bundled one."""
    assert config.config_file(tmp_path, "report.toml") == config.DEFAULTS_ROOT / "report.toml"
    assert config.is_default(tmp_path, "report.toml")
    (tmp_path / "report.toml").write_text("")
    assert config.config_file(tmp_path, "report.toml") == tmp_path / "report.toml"
    assert not config.is_default(tmp_path, "report.toml")


def test_loaders_use_the_bundled_defaults(tmp_path: Path) -> None:
    from flashpoint.anomaly.catalogue import load_causes
    from flashpoint.anomaly.settings import load_anomaly_config
    from flashpoint.report.settings import load_report_config
    from flashpoint.semantics.robot_config import load_config_seasons
    from flashpoint.theme import load_theme

    bundled = config.DEFAULTS_ROOT
    assert load_report_config(tmp_path) == load_report_config(bundled)
    assert load_anomaly_config(tmp_path) == load_anomaly_config(bundled)
    assert load_causes(tmp_path) == load_causes(bundled)
    assert load_causes(tmp_path)  # the bundled catalogue is not empty
    assert set(load_config_seasons(tmp_path)) == {2025, 2026}
    assert load_theme(tmp_path) == load_theme(bundled)


def test_team_seasons_replace_the_bundled_file(tmp_path: Path) -> None:
    from flashpoint.semantics.robot_config import load_config_seasons

    seasons = load_config_seasons(config.config_root())
    assert seasons[2026].root_names()["robot"] == "NT:Robot/m_robotContainer/"
    (tmp_path / "seasons").mkdir()
    assert "robot" not in load_config_seasons(tmp_path)[2026].root_names()
