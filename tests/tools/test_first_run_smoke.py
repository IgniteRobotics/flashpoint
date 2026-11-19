"""The cross-OS first-run smoke (spec: ci-quality-gates, "First-run smoke on every platform")."""

import fnmatch
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "first-run-smoke.py"
WORKFLOWS = REPO / ".github" / "workflows"
SMOKE = WORKFLOWS / "first-run-smoke.yml"
STEPS = ("install", "init", "ingest", "replay", "serve")


def _load(path: Path) -> dict[Any, Any]:
    loaded: dict[Any, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return loaded


def _triggers(workflow: dict[Any, Any]) -> dict[str, Any]:
    on: dict[str, Any] = workflow["on"] if "on" in workflow else workflow[True]  # YAML 1.1: on=True
    return on


def _needs(job: dict[str, Any]) -> list[str]:
    needs = job.get("needs", [])
    return [needs] if isinstance(needs, str) else list(needs)


# --- the workflows ------------------------------------------------------------------------


def test_smoke_runs_nightly_on_source_prs_and_when_called() -> None:
    on = _triggers(_load(SMOKE))
    assert "schedule" in on and "workflow_call" in on
    assert "wheel-artifact" in (on["workflow_call"].get("inputs") or {})
    paths = on["pull_request"]["paths"]
    changes = ["src/flashpoint/cli.py", "pyproject.toml", "poetry.lock",
               "tools/first-run-smoke.py", ".github/workflows/first-run-smoke.yml"]  # fmt: skip
    for changed in changes:
        assert any(fnmatch.fnmatch(changed, p) for p in paths), changed


def test_docs_only_change_does_not_run_the_smoke() -> None:
    paths = _triggers(_load(SMOKE))["pull_request"]["paths"]
    for changed in ("docs/guide/user-guide.md", "README.md", "openspec/changes/x/tasks.md"):
        assert not any(fnmatch.fnmatch(changed, p) for p in paths), changed


def test_smoke_runs_on_every_platform_with_uv() -> None:
    jobs = _load(SMOKE)["jobs"]
    (job,) = jobs.values()
    assert set(job["strategy"]["matrix"]["os"]) == {
        "ubuntu-latest", "macos-latest", "windows-latest"
    }  # fmt: skip
    assert job["strategy"].get("fail-fast") is False
    uses = [s.get("uses", "") for s in job["steps"]]
    assert any(u.startswith("astral-sh/setup-uv@") for u in uses)
    assert any("tools/first-run-smoke.py" in s.get("run", "") for s in job["steps"])


def test_smoke_is_not_part_of_the_required_check() -> None:
    ci = _load(WORKFLOWS / "ci.yml")["jobs"]
    assert not [n for n in _needs(ci["ci-ok"]) if "smoke" in n]
    assert "ci-ok" not in _load(SMOKE)["jobs"]


def test_smoke_failure_holds_a_release() -> None:
    """The release calls the smoke with the wheel its build made; PyPI waits for it."""
    jobs = _load(WORKFLOWS / "release.yml")["jobs"]
    smoke = [n for n, j in jobs.items() if str(j.get("uses", "")).endswith("first-run-smoke.yml")]
    assert len(smoke) == 1
    job = jobs[smoke[0]]
    assert "build" in _needs(job)
    assert job["with"]["wheel-artifact"] == "dist"  # the artifact the build job uploads
    assert smoke[0] in _needs(jobs["pypi"])
    assert smoke[0] not in _needs(jobs["testpypi"])  # a dry run doesn't wait for it


# --- the script ---------------------------------------------------------------------------


def _alive(pid: int) -> bool:
    if sys.platform == "win32":
        done = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True)
        return str(pid) in done.stdout
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.fixture(scope="module")
def wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    poetry = shutil.which("poetry")
    if poetry is None:
        pytest.skip("poetry is not on PATH")
    out = tmp_path_factory.mktemp("dist")
    subprocess.run([poetry, "build", "-f", "wheel", "-o", str(out)], cwd=REPO, check=True,
                   capture_output=True)  # fmt: skip
    (path,) = out.glob("*.whl")
    return path


@pytest.mark.corpus
def test_server_that_does_not_answer_fails_its_step_and_is_stopped(
    wheel: Path, tmp_path: Path
) -> None:
    if shutil.which("uv") is None:
        pytest.skip("uv is not on PATH")
    done = subprocess.run(
        [sys.executable, str(SCRIPT), "--wheel", str(wheel), "--work", str(tmp_path / "work"),
         "--inject-serve-failure", "--serve-timeout", "5"],
        capture_output=True, text=True, timeout=1800, cwd=tmp_path,
    )  # fmt: skip
    output = done.stdout + done.stderr
    assert done.returncode != 0, output
    for step in STEPS:
        assert re.search(rf"^== step \d/5: {step}\b", output, re.MULTILINE), (step, output)
    for step in STEPS[:-1]:
        assert re.search(rf"^ok +{step}\b", output, re.MULTILINE), (step, output)
    assert re.search(r"^FAILED serve\b", output, re.MULTILINE), output
    pid = int(re.search(r"server pid (\d+)", output)[1])  # type: ignore[index]  # asserted present
    assert not _alive(pid)
