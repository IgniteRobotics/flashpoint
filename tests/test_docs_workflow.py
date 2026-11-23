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


def test_only_the_push_jobs_can_write() -> None:
    assert _workflow()["permissions"] == {"contents": "read"}
    writers = [name for name, job in _jobs().items() if "permissions" in job]
    assert writers == ["deploy", "attach-archive"]
    for name in writers:
        assert _jobs()[name]["permissions"] == {"contents": "write"}


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


def test_a_tag_attaches_the_offline_archive_to_its_release() -> None:
    """Spec "Offline copy of the docs": the archive built here (with the real media) goes on the
    tag's GitHub Release, which release.yml creates after the smoke and PyPI, so it waits."""
    job = _jobs()["attach-archive"]
    assert set(job["needs"]) == {"plan", "build", "deploy"}
    assert "github.event_name == 'push'" in job["if"] and "dry_run" in job["if"]
    script = "\n".join(str(step.get("run", "")) for step in job["steps"])
    assert "gh release view" in script
    assert "gh release upload" in script and "--clobber" in script
    assert "flashpoint-docs-$VERSION.zip" in script


# --- docs-squash.yml (design D-c: gh-pages history is disposable) ---------------------------

SQUASH = REPO / ".github" / "workflows" / "docs-squash.yml"


def test_squash_is_manual_confirmed_and_never_overlaps_a_deploy() -> None:
    workflow: dict[Any, Any] = yaml.safe_load(SQUASH.read_text(encoding="utf-8"))
    assert set(workflow[True]) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read"}
    (job,) = workflow["jobs"].values()
    assert job["permissions"] == {"contents": "write"}
    assert "inputs.confirm" in job["if"]
    assert job["concurrency"] == {"group": "docs-deploy", "cancel-in-progress": False}
    script = "\n".join(str(step.get("run", "")) for step in job["steps"])
    assert "git commit-tree" in script
    assert "--force-with-lease" in script  # a deploy that landed meanwhile is never lost


# --- media capture (task 8.7) ---------------------------------------------------------------

MEDIA_WORKFLOW = REPO / ".github" / "workflows" / "docs-media.yml"


def test_build_captures_media_from_the_corpus_into_the_site() -> None:
    runs = _runs("build")
    assert _first(runs, "fetch-corpus.py") < _first(runs, "docs-media.py capture")
    assert _first(runs, "docs-media.py capture") < _first(runs, "check-site")
    assert "placeholders" not in "\n".join(runs), "the deploy publishes real media, not stand-ins"


def test_media_pr_job_is_non_blocking_and_uploads_the_media() -> None:
    flow = yaml.safe_load(MEDIA_WORKFLOW.read_text(encoding="utf-8"))
    paths = flow[True]["pull_request"]["paths"]  # YAML reads the bare key `on` as True
    assert set(paths) == {
        "src/flashpoint/web/**",
        "docs/media-manifest.toml",
        "tools/docs-media.py",
    }
    steps = flow["jobs"]["docs-media"]["steps"]
    uploads = [s for s in steps if s.get("uses", "").startswith("actions/upload-artifact@")]
    assert uploads and uploads[0]["with"]["path"] == "build/docs-media/"
    ci = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    assert "docs-media" not in ci["jobs"]["ci-ok"]["needs"], "the media job must not gate ci-ok"
