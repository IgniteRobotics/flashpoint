"""The release workflow publishes a GitHub Release after PyPI (spec public-release).

Commit pinning of every action is checked for all workflows in tests/test_quality.py.
"""

from pathlib import Path
from typing import Any

import yaml

REPO = Path(__file__).resolve().parents[1]
RELEASE = REPO / ".github" / "workflows" / "release.yml"


def _workflow() -> dict[str, Any]:
    loaded: dict[str, Any] = yaml.safe_load(RELEASE.read_text(encoding="utf-8"))
    return loaded


def _jobs() -> dict[str, dict[str, Any]]:
    jobs: dict[str, dict[str, Any]] = _workflow()["jobs"]
    return jobs


def _needs(job: dict[str, Any]) -> list[str]:
    needs = job.get("needs", [])
    return [needs] if isinstance(needs, str) else list(needs)


def _step_index(steps: list[dict[str, Any]], predicate: Any) -> int:
    matches = [index for index, step in enumerate(steps) if predicate(step)]
    assert matches, "no matching step"
    return matches[0]


def _runs_extraction(step: dict[str, Any]) -> bool:
    return "tools/changelog-section.py" in step.get("run", "")


def test_build_extracts_the_notes_before_uploading_the_package() -> None:
    steps = _jobs()["build"]["steps"]
    extract = _step_index(steps, _runs_extraction)
    upload = _step_index(
        steps,
        lambda step: (
            str(step.get("uses", "")).startswith("actions/upload-artifact@")
            and step.get("with", {}).get("name") == "dist"
        ),
    )
    assert extract < upload


def test_extraction_runs_only_for_a_tag() -> None:
    step = next(step for step in _jobs()["build"]["steps"] if _runs_extraction(step))
    assert step.get("if") == "github.event_name == 'push'"


def test_notes_stay_out_of_the_package_upload() -> None:
    steps = _jobs()["build"]["steps"]
    extract = next(step for step in steps if _runs_extraction(step))
    assert "dist/" not in extract["run"]
    uploads = [
        step for step in steps if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    notes = [step for step in uploads if step.get("with", {}).get("name") == "release-notes"]
    assert len(notes) == 1
    assert not str(notes[0]["with"]["path"]).startswith("dist")


def test_every_publish_job_needs_build() -> None:
    jobs = _jobs()
    for name in ("pypi", "testpypi"):
        assert "build" in _needs(jobs[name]), name


def test_github_release_follows_the_pypi_publish() -> None:
    job = _jobs()["github-release"]
    assert "pypi" in _needs(job)
    assert job.get("if") == "github.event_name == 'push'"


def test_only_the_github_release_job_can_write_contents() -> None:
    workflow = _workflow()
    assert workflow["permissions"] == {"contents": "read"}
    writers = [
        name
        for name, job in workflow["jobs"].items()
        if job.get("permissions", {}).get("contents") == "write"
    ]
    assert writers == ["github-release"]
    assert workflow["jobs"]["github-release"]["permissions"] == {"contents": "write"}


def test_github_release_marks_latest_or_pre_release() -> None:
    job = _jobs()["github-release"]
    script = "\n".join(step.get("run", "") for step in job["steps"])
    assert "gh release create" in script
    assert "--notes-file" in script
    assert "--verify-tag" in script
    assert "--latest" in script
    assert "--prerelease" in script
    assert "needs.build.outputs.prerelease" in str(job)


def test_no_github_release_on_a_dry_run() -> None:
    jobs = _jobs()
    dry_run = {"testpypi", "build"}
    for name, job in jobs.items():
        if name in dry_run:
            continue
        assert job.get("if") == "github.event_name == 'push'", name
    assert "github-release" not in str(jobs["testpypi"])
