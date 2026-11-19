import re
import shutil
import subprocess
import tomllib
import zipfile
from importlib import metadata
from pathlib import Path

import pytest
import yaml

import flashpoint

REPO = Path(__file__).resolve().parents[1]
DECLARED_PYTHONS = ["3.11", "3.12", "3.13", "3.14"]
WHEEL_BUDGET_BYTES = 5 * 1024 * 1024
DEFAULT_FILES = {
    "flashpoint/defaults/report.toml",
    "flashpoint/defaults/anomaly.toml",
    "flashpoint/defaults/anomaly-causes.toml",
    "flashpoint/defaults/theme.toml",
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


# --- install and Python range (spec: team-setup "Installable public package") ---------------


def _ci_jobs() -> dict[str, dict[str, object]]:
    workflow = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text("utf-8"))
    jobs: dict[str, dict[str, object]] = workflow["jobs"]
    return jobs


def _matrix(job: str) -> dict[str, list[str]]:
    strategy = _ci_jobs()[job]["strategy"]
    assert isinstance(strategy, dict)
    matrix: dict[str, list[str]] = strategy["matrix"]
    return matrix


def test_declared_pythons_are_tested() -> None:
    project = tomllib.loads((REPO / "pyproject.toml").read_text("utf-8"))["project"]
    assert project["requires-python"] == ">=3.11,<3.15"
    classified = [
        c.rsplit(" :: ", 1)[1]
        for c in project["classifiers"]
        if re.fullmatch(r"Programming Language :: Python :: 3\.\d+", c)
    ]
    assert classified == DECLARED_PYTHONS
    assert [str(v) for v in _matrix("lint-test")["python"]] == DECLARED_PYTHONS


def test_clean_install_runs_lowest_default_and_highest_python_on_each_os() -> None:
    matrix = _matrix("clean-install")
    assert set(matrix["os"]) == {"ubuntu-latest", "macos-latest", "windows-latest"}
    assert [str(v) for v in matrix["python"]] == ["3.11", "3.13", "3.14"]
    steps = _ci_jobs()["clean-install"]["steps"]
    assert isinstance(steps, list)
    pythons = [s["with"]["python-version"] for s in steps if "setup-python" in s.get("uses", "")]
    assert "${{ matrix.python }}" in pythons


SHIPPED_TEXT = (".py", ".js", ".html", ".css", ".xml", ".service", ".toml", ".md")


def test_no_shipped_text_installs_with_pipx_alone() -> None:
    """Every install, upgrade or reinstall hint names uv first (team-setup "Setup checks")."""
    offenders = []
    for path in sorted((REPO / "src" / "flashpoint").rglob("*")):
        if path.suffix not in SHIPPED_TEXT or not path.is_file():
            continue
        text = path.read_text("utf-8", errors="replace")
        for match in re.finditer(r"pipx", text):
            nearby = text[max(0, match.start() - 200) : match.end() + 200]
            if "uv tool" not in nearby:
                line = text.count("\n", 0, match.start()) + 1
                offenders.append(f"{path.relative_to(REPO)}:{line}")
    assert not offenders, "name `uv tool ...` first, pipx as the alternative:\n" + "\n".join(
        offenders
    )
