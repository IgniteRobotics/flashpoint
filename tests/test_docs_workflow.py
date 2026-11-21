"""The docs deploy workflow (mvp-documentation design D-c; spec project-documentation,
"Published, versioned docs site")."""

from pathlib import Path
from typing import Any

import yaml

REPO = Path(__file__).resolve().parents[1]
DOCS = REPO / ".github" / "workflows" / "docs.yml"


def _workflow() -> dict[Any, Any]:
    loaded: dict[Any, Any] = yaml.safe_load(DOCS.read_text(encoding="utf-8"))
    return loaded


def _jobs() -> dict[str, dict[str, Any]]:
    jobs: dict[str, dict[str, Any]] = _workflow()["jobs"]
    return jobs


def _runs(job: str) -> list[str]:
    return [str(step.get("run", "")) for step in _jobs()[job]["steps"]]


def _first(runs: list[str], needle: str) -> int:
    return next(i for i, run in enumerate(runs) if needle in run)


def test_triggers_tags_nightly_and_manual_with_dry_run() -> None:
    on = _workflow()[True]  # YAML 1.1 reads the `on` key as True
    assert on["push"]["tags"] == ["v*"]
    assert on["schedule"]
    inputs = on["workflow_dispatch"]["inputs"]
    assert inputs["dry_run"]["type"] == "boolean"
    assert "tag" in inputs  # a docs-only fix redeploys a released minor
    assert ".github/workflows/docs.yml" in on["pull_request"]["paths"]  # a dry run on PRs


def test_only_the_push_job_can_write() -> None:
    assert _workflow()["permissions"] == {"contents": "read"}
    writers = [name for name, job in _jobs().items() if "permissions" in job]
    assert writers == ["deploy"]
    assert _jobs()["deploy"]["permissions"] == {"contents": "write"}


def test_one_deploy_at_a_time_never_cancelled() -> None:
    assert "concurrency" not in _workflow(), "a dry run must not queue against a deploy"
    concurrency = _jobs()["deploy"]["concurrency"]
    assert concurrency == {"group": "docs-deploy", "cancel-in-progress": False}


def test_build_runs_strict_then_media_then_demo_then_archive() -> None:
    runs = _runs("build")
    steps = [
        _first(runs, "zensical build --strict"),
        _first(runs, "tools/docs-media.py"),
        _first(runs, "demo"),
        _first(runs, "mkdocs.offline.yml"),
    ]
    assert steps == sorted(steps)
    assert _first(runs, "zensical build --strict") < _first(runs, "check-site")


def test_deploy_needs_a_passing_build_and_is_skipped_on_a_dry_run() -> None:
    deploy = _jobs()["deploy"]
    assert set(deploy["needs"]) == {"plan", "build"}
    assert "dry_run" in deploy["if"]


def test_deploy_starts_from_the_published_branch() -> None:
    """mike works from the local gh-pages branch; without a fetch, the first push would replace
    every published version with this one."""
    runs = _runs("deploy")
    assert _first(runs, "fetch origin gh-pages") < _first(runs, "mike deploy")
    assert _first(runs, "mike deploy") < _first(runs, "mike set-default")
    assert "latest" in runs[_first(runs, "mike set-default")]


def test_plan_uses_the_version_tool_and_forces_a_dry_run_off_development() -> None:
    runs = "\n".join(_runs("plan"))
    assert "tools/docs-version.py" in runs
    assert "versions.json" in runs
    assert "refs/heads/development" in runs
