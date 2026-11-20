"""The hand-started evidence workflows (issue #104): fresh installs and the smoke report."""

from pathlib import Path
from typing import Any

import yaml

WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"


def _load(name: str) -> dict[Any, Any]:
    loaded: dict[Any, Any] = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    return loaded


def _on(workflow: dict[Any, Any]) -> dict[str, Any]:
    on: dict[str, Any] = workflow["on"] if "on" in workflow else workflow[True]  # YAML 1.1
    return on


def test_both_start_by_hand_and_never_on_a_schedule() -> None:
    for name in ("fresh-install.yml", "smoke-report.yml"):
        on = _on(_load(name))
        assert "workflow_dispatch" in on and "schedule" not in on, name
        assert all(
            p.startswith((".github/workflows/", "tools/")) for p in on["pull_request"]["paths"]
        )


def test_neither_is_part_of_the_required_check() -> None:
    needs = _load("ci.yml")["jobs"]["ci-ok"]["needs"]
    assert {"fresh-install", "smoke-report", "ubuntu", "others", "report"}.isdisjoint(needs)


def test_fresh_install_runs_the_documented_command_with_no_suitable_python() -> None:
    jobs = _load("fresh-install.yml")["jobs"]
    assert jobs["ubuntu"]["container"] == "ubuntu:24.04"
    others = jobs["others"]
    assert set(others["strategy"]["matrix"]["os"]) == {"macos-latest", "windows-latest"}
    assert others["env"]["UV_PYTHON_PREFERENCE"] == "only-managed"
    for job in jobs.values():
        runs = " ".join(s.get("run", "") for s in job["steps"])
        assert 'uv tool install --python 3.13 "$PACKAGE"' in runs
        assert "flashpoint --version" in runs and "flashpoint init" in runs


def test_smoke_report_posts_only_when_asked() -> None:
    (job,) = _load("smoke-report.yml")["jobs"].values()
    post = [s for s in job["steps"] if "gh issue comment" in s.get("run", "")]
    assert len(post) == 1 and post[0]["if"] == "inputs.post"
    assert job["permissions"]["issues"] == "write"
