"""tools/check-release-links.py (spec public-release, moving-branch documentation links)."""

import importlib.util
import subprocess
import sys
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "check-release-links.py"
RELEASE = REPO / ".github" / "workflows" / "release.yml"
SITE = "https://igniterobotics.github.io/flashpoint"
GITHUB = "https://github.com/IgniteRobotics/flashpoint"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_release_links", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


check_release_links = _load()


def _problems(text: str, version: str = "0.3.0") -> list[str]:
    found: list[str] = check_release_links.problems(text, version, "README.md")
    return found


GOOD_README = f"""\
# Flashpoint

![Replay]({SITE}/0.3/assets/media/replay.png)

Read the [user guide]({SITE}/0.3/guide/), the [changelog]({GITHUB}/blob/v0.3.0/CHANGELOG.md)
and [AdvantageScope's docs](https://github.com/Mechanical-Advantage/AdvantageScope/blob/main/docs/).

<img src="{SITE}/0.3/assets/media/history.png" alt="History">
"""


def test_good_readme_passes() -> None:
    assert _problems(GOOD_README) == []


def test_moving_branch_link_refused() -> None:
    text = f"# Flashpoint\n\nSee the [guide]({GITHUB}/blob/main/docs/guide/user-guide.md).\n"
    found = _problems(text)
    assert len(found) == 1
    assert found[0].startswith("README.md:3:")
    assert "blob/main" in found[0]


def test_every_moving_branch_form_is_refused() -> None:
    lines = [
        f"[a]({GITHUB}/blob/main/README.md)",
        f"[b]({GITHUB}/blob/development/README.md)",
        f"[c]({GITHUB}/tree/main/docs)",
        f"[d]({GITHUB}/tree/development/docs)",
        "![e](https://raw.githubusercontent.com/IgniteRobotics/flashpoint/main/media/e.png)",
    ]
    found = _problems("\n".join(lines))
    assert [problem.split(":")[1] for problem in found] == ["1", "2", "3", "4", "5"]


def test_relative_image_link_refused() -> None:
    text = "# Flashpoint\n\n![Replay](media/replay.png)\n\n<img src='docs/a.png' alt='a'>\n"
    found = _problems(text)
    assert [problem.split(":")[1] for problem in found] == ["3", "5"]
    assert all("relative" in problem for problem in found)


def test_relative_text_link_is_not_an_image_problem() -> None:
    assert _problems("[Contributing](CONTRIBUTING.md)\n") == []


def test_image_from_another_version() -> None:
    text = f"![Replay]({SITE}/0.2/assets/media/replay.png)\n"
    found = _problems(text, "0.3.0")
    assert len(found) == 1
    assert found[0].startswith("README.md:1:")
    assert "/0.3/" in found[0]


def test_image_from_latest_is_refused() -> None:
    assert len(_problems(f"![Replay]({SITE}/latest/assets/media/replay.png)\n")) == 1


def test_pre_release_and_dev_versions_use_their_minor() -> None:
    text = f"![Replay]({SITE}/0.3/assets/media/replay.png)\n"
    assert _problems(text, "0.3.0rc1") == []
    assert _problems(text, "0.3.0.dev1") == []
    assert len(_problems(text, "0.4.0.dev1")) == 1


def _wheel(path: Path, metadata: str) -> Path:
    wheel = path / "flashpoint-0.3.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("flashpoint/__init__.py", "")
        archive.writestr("flashpoint-0.3.0.dist-info/METADATA", metadata)
    return wheel


def _metadata(project_url: str, readme: str) -> str:
    return (
        "Metadata-Version: 2.4\nName: flashpoint\nVersion: 0.3.0\n"
        f"Project-URL: Documentation, {project_url}\n"
        "Description-Content-Type: text/markdown\n\n"
        f"{readme}"
    )


def test_built_package_metadata_passes(tmp_path: Path) -> None:
    wheel = _wheel(tmp_path, _metadata(f"{SITE}/", GOOD_README))
    assert check_release_links.check_file(wheel, "0.3.0") == []


def test_built_package_metadata_with_a_moving_branch_url(tmp_path: Path) -> None:
    wheel = _wheel(tmp_path, _metadata(f"{GITHUB}/blob/main/docs/guide/user-guide.md", "# Hi\n"))
    found = check_release_links.check_file(wheel, "0.3.0")
    assert len(found) == 1
    assert "METADATA:4:" in found[0]


def test_built_package_readme_names_the_readme_line(tmp_path: Path) -> None:
    readme = "# Flashpoint\n\n![Replay](media/replay.png)\n"
    wheel = _wheel(tmp_path, _metadata(f"{SITE}/", readme))
    found = check_release_links.check_file(wheel, "0.3.0")
    assert len(found) == 1
    assert "README:3:" in found[0]


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args], capture_output=True, text=True, check=False
    )


def test_cli_fails_and_names_the_line(tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text(
        f"# Flashpoint\n\n[guide]({GITHUB}/blob/main/docs/guide/)\n", encoding="utf-8"
    )
    done = _run("--version", "0.3.0", str(readme))
    assert done.returncode == 1
    assert f"{readme}:3:" in done.stderr


def test_cli_passes_a_clean_package(tmp_path: Path) -> None:
    wheel = _wheel(tmp_path, _metadata(f"{SITE}/", GOOD_README))
    done = _run("--version", "0.3.0", str(wheel))
    assert done.returncode == 0, done.stderr


def _build_steps() -> list[dict[str, Any]]:
    workflow: dict[str, Any] = yaml.safe_load(RELEASE.read_text(encoding="utf-8"))
    steps: list[dict[str, Any]] = workflow["jobs"]["build"]["steps"]
    return steps


def test_release_build_runs_the_check_after_building_and_before_uploading() -> None:
    steps = _build_steps()

    def index(predicate: Any) -> int:
        return next(number for number, step in enumerate(steps) if predicate(step))

    build = index(lambda step: step.get("run", "").strip() == "poetry build")
    check = index(lambda step: "tools/check-release-links.py" in step.get("run", ""))
    upload = index(lambda step: str(step.get("uses", "")).startswith("actions/upload-artifact@"))
    assert build < check < upload
    assert "dist/*.whl" in steps[check]["run"]
    assert "if" not in steps[check]
