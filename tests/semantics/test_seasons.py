from pathlib import Path

import pytest

from flashpoint.semantics.robot_config import ConfigError, load_robot, load_robots, load_seasons

SEASON = """
season = 2026
[roots]
robot = "NT:Robot/m_robotContainer/"
photonvision = "NT:/photonvision/"
camera_publisher = "NT:/CameraPublisher/"
preferences = "NT:/Preferences/"
fms = "NT:/FMSInfo/"
"""

ROBOT = """
robot = "r"
season = 2026
project = "Robot-2026"
[[slot]]
id = "a"
bus = "rio"
model = "TalonFX"
can_id = 1
subsystem = "s"
role = "x"
"""

SIGNAL = """
[[signal]]
id = "{id}"
root = "{root}"
entry = "{entry}"
subsystem = "intake"
metric = "{metric}"
"""


def _signal(
    id: str = "beam", root: str = "robot", entry: str = "Intake/Beam", metric: str = "is_detected"
) -> str:  # noqa: A002
    return SIGNAL.format(id=id, root=root, entry=entry, metric=metric)


def _config(tmp_path: Path, robot: str, season: str | None = SEASON) -> Path:
    (tmp_path / "robots").mkdir()
    (tmp_path / "robots" / "r.toml").write_text(robot)
    if season is not None:
        (tmp_path / "seasons").mkdir()
        (tmp_path / "seasons" / "2026.toml").write_text(season)
    return tmp_path


def test_season_loads_five_roots(tmp_path: Path) -> None:
    seasons = load_seasons(_config(tmp_path, ROBOT) / "seasons")
    roots = seasons[2026].root_names()
    assert roots == {
        "robot": "NT:Robot/m_robotContainer/",
        "photonvision": "NT:/photonvision/",
        "camera_publisher": "NT:/CameraPublisher/",
        "preferences": "NT:/Preferences/",
        "fms": "NT:/FMSInfo/",
    }


def test_missing_seasons_dir_is_empty(tmp_path: Path) -> None:
    assert load_seasons(tmp_path / "seasons") == {}


@pytest.mark.parametrize(
    ("text", "key"),
    [
        (SEASON + 'colour = "red"\n', "colour"),
        (SEASON.replace("season = 2026\n", ""), "season"),
        (SEASON.replace('fms = "NT:/FMSInfo/"', 'fms = ""'), "fms"),
        (
            SEASON.replace(
                'fms = "NT:/FMSInfo/"', 'fms = "NT:/FMSInfo/"\nshuffleboard = "NT:/Shuffleboard/"'
            ),
            "shuffleboard",
        ),
    ],
)
def test_bad_season_rejected_naming_file_and_key(tmp_path: Path, text: str, key: str) -> None:
    with pytest.raises(ConfigError, match=f"(?s)2026.toml.*{key}"):
        load_seasons(_config(tmp_path, ROBOT, text) / "seasons")


def test_season_file_name_must_match_season(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="(?s)2026.toml.*season"):
        load_seasons(_config(tmp_path, ROBOT, SEASON.replace("2026", "2025", 1)) / "seasons")


def test_robot_with_signals_loads(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        ROBOT + _signal() + _signal("pitch", "photonvision", "FRONT/targetPitch", "target_pitch"),
    )
    (robot,) = load_robots(config / "robots")
    assert [s.id for s in robot.signals] == ["beam", "pitch"]
    assert robot.signals[0].component is None
    seasons = load_seasons(config / "seasons")
    assert robot.signal_names(seasons[2026]) == {
        "NT:Robot/m_robotContainer/Intake/Beam": robot.signals[0],
        "NT:/photonvision/FRONT/targetPitch": robot.signals[1],
    }


def test_load_robot_finds_sibling_seasons(tmp_path: Path) -> None:
    config = _config(tmp_path, ROBOT + _signal())
    assert len(load_robot(config / "robots" / "r.toml").signals) == 1


def test_duplicate_signal_id_names_both(tmp_path: Path) -> None:
    config = _config(tmp_path, ROBOT + _signal() + _signal(entry="Intake/Other"))
    with pytest.raises(ConfigError, match="(?s)r.toml.*'beam'.*Intake/Beam.*Intake/Other"):
        load_robots(config / "robots")


def test_duplicate_root_entry_names_both(tmp_path: Path) -> None:
    config = _config(tmp_path, ROBOT + _signal() + _signal(id="beam2"))
    with pytest.raises(ConfigError, match="(?s)r.toml.*'beam'.*'beam2'"):
        load_robots(config / "robots")


def test_undefined_root_names_signal_and_root(tmp_path: Path) -> None:
    config = _config(tmp_path, ROBOT + _signal(root="smartdashboard"))
    with pytest.raises(ConfigError, match="(?s)r.toml.*'beam'.*smartdashboard"):
        load_robots(config / "robots")


def test_signals_without_season_file_rejected(tmp_path: Path) -> None:
    config = _config(tmp_path, ROBOT + _signal(), season=None)
    with pytest.raises(ConfigError, match="(?s)r.toml.*season 2026"):
        load_robots(config / "robots")


def test_non_snake_case_metric_rejected(tmp_path: Path) -> None:
    config = _config(tmp_path, ROBOT + _signal(metric="IsDetected"))
    with pytest.raises(ConfigError, match="(?s)r.toml.*metric"):
        load_robots(config / "robots")


def test_robot_without_signals_needs_no_season(tmp_path: Path) -> None:
    (robot,) = load_robots(_config(tmp_path, ROBOT, season=None) / "robots")
    assert robot.signals == []
