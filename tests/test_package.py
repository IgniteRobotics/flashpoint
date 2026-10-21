import shutil
import subprocess
import zipfile
from importlib import metadata
from pathlib import Path

import pytest

import flashpoint

WHEEL_BUDGET_BYTES = 5 * 1024 * 1024
DEFAULT_FILES = {
    "flashpoint/defaults/report.toml",
    "flashpoint/defaults/anomaly.toml",
    "flashpoint/defaults/anomaly-causes.toml",
    "flashpoint/defaults/seasons/2025.toml",
    "flashpoint/defaults/seasons/2026.toml",
    "flashpoint/defaults/units/units-seed.csv",
}


def test_package_exposes_version() -> None:
    assert flashpoint.__version__ == metadata.version("flashpoint")


def test_cli_without_command_prints_help() -> None:
    from flashpoint.cli import main

    assert main([]) == 0


@pytest.fixture(scope="module")
def wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    poetry = shutil.which("poetry")
    if poetry is None:
        pytest.skip("poetry is not on PATH")
    out = tmp_path_factory.mktemp("dist")
    repo = Path(__file__).resolve().parents[1]
    subprocess.run([poetry, "build", "-f", "wheel", "-o", str(out)], cwd=repo, check=True,
                   capture_output=True)  # fmt: skip
    (path,) = out.glob("*.whl")
    return path


def test_wheel_holds_the_defaults_and_no_team_data(wheel: Path) -> None:
    """No team data in the package."""
    with zipfile.ZipFile(wheel) as zf:
        names = set(zf.namelist())
        units = zf.read("flashpoint/defaults/units/units-seed.csv").decode()
    assert names >= DEFAULT_FILES
    assert not [n for n in names if "/robots/" in n]
    assert not [n for n in names if n.endswith((".wpilog", ".hoot"))]
    assert not [n for n in names if n.endswith("failure-labels.toml")]
    assert len(units.strip().splitlines()) == 1  # the header only


def test_wheel_is_small(wheel: Path) -> None:
    assert wheel.stat().st_size < WHEEL_BUDGET_BYTES


def test_bundled_seasons_leave_the_robot_root_to_the_team() -> None:
    from flashpoint.config import DEFAULTS_ROOT
    from flashpoint.semantics.robot_config import load_seasons

    seasons = load_seasons(DEFAULTS_ROOT / "seasons")
    assert set(seasons) == {2025, 2026}
    for season in seasons.values():
        assert "robot" not in season.root_names()
        assert season.root_names()["fms"] == "NT:/FMSInfo/"
