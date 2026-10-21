import os
import shutil
import subprocess
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.readers import hoot
from flashpoint.setup.checks import CheckContext, CheckResult, run_checks

CHECKS_BUDGET_S = 2.0
IGNITE = config.config_root()


def _ok_run(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(list(args), 0, "active\n", "")


def _context(root: Path, lake: Path, **kwargs: Any) -> CheckContext:
    defaults: dict[str, Any] = {
        "config_root": root,
        "lake": LakePaths(lake),
        "cache_root": lake.parent / "cache",
        "system": "Darwin",
        "season": 2026,
        "python": (3, 12),
        "run": _ok_run,
        "find_advantagescope": lambda root: (None, "not found"),
    }
    return CheckContext(**(defaults | kwargs))


def _by_id(results: list[CheckResult]) -> dict[str, CheckResult]:
    return {r.id: r for r in results}


def _ignite(tmp_path: Path) -> Path:
    root = tmp_path / "cfg"
    shutil.copytree(IGNITE, root)
    return root


# --- configuration root and files ---------------------------------------------------------


def test_missing_configuration(tmp_path: Path) -> None:
    checks = _by_id(run_checks(_context(tmp_path / "none", tmp_path / "lake")))
    root = checks["config-root"]
    assert root.status == "problem" and root.fix_action == "init"
    assert root.fix_label == "Create configuration" and "flashpoint init" in (root.command or "")
    assert checks["team"].status == "problem"


def test_ignite_configuration_is_ok(tmp_path: Path) -> None:
    (tmp_path / "lake").mkdir()
    checks = _by_id(run_checks(_context(_ignite(tmp_path), tmp_path / "lake")))
    assert checks["config-root"].status == "ok"
    assert checks["team"].status == "ok" and "6829" in checks["team"].message
    assert checks["robot-season"].status == "ok" and "2026-comp" in checks["robot-season"].message
    files = {r.source: r for r in checks.values() if r.id.startswith("file:")}
    assert files["robots/2026-comp.toml"].status == "ok"
    assert files["report.toml"].default and files["report.toml"].status == "ok"
    assert not files["seasons/2026.toml"].default
    assert all(r.status == "ok" for r in files.values())


def test_invalid_robot_file(tmp_path: Path) -> None:
    root = _ignite(tmp_path)
    robot = root / "robots" / "2026-comp.toml"
    text = robot.read_text()
    # steer-fl takes drive-fl's CAN id: two slots, one device.
    start = text.index('id = "steer-fl"')
    end = text.index("can_id", start)
    line_end = text.index("\n", end)
    robot.write_text(text[:end] + "can_id = 11" + text[line_end:])
    checks = _by_id(run_checks(_context(root, tmp_path / "lake")))
    bad = checks["file:robots/2026-comp.toml"]
    assert bad.status == "problem"
    assert "2026-comp.toml" in bad.message and "drive-fl" in bad.message
    assert "steer-fl" in bad.message


def test_broken_threshold_file(tmp_path: Path) -> None:
    root = _ignite(tmp_path)
    (root / "report.toml").write_text("[thresholds]\nnope = 1\n")
    (root / "anomaly.toml").write_text("not toml [")
    checks = _by_id(run_checks(_context(root, tmp_path / "lake")))
    assert checks["file:report.toml"].status == "problem"
    assert "nope" in checks["file:report.toml"].message
    assert checks["file:anomaly.toml"].status == "problem"
    assert not checks["file:report.toml"].default


def test_bad_acquire_file(tmp_path: Path) -> None:
    root = _ignite(tmp_path)
    (root / "acquire.toml").write_text("team = 0\n")
    checks = _by_id(run_checks(_context(root, tmp_path / "lake")))
    assert checks["file:acquire.toml"].status == "problem"
    assert checks["team"].status == "problem"


def test_after_init_only_the_robot_is_missing(tmp_path: Path) -> None:
    from flashpoint.setup.init import init_config

    root = tmp_path / "cfg"
    init_config(root, team=254, lake=tmp_path / "lake")
    results = run_checks(_context(root, tmp_path / "lake"))
    problems = [r.id for r in results if r.status == "problem"]
    assert problems == ["robot-season"]
    checks = _by_id(results)
    assert "254" in checks["team"].message and "10.2.54.2" in checks["team"].message


# --- team, robot, lake --------------------------------------------------------------------


def test_team_from_hosts_only_is_ok(tmp_path: Path) -> None:
    root = tmp_path / "cfg"
    root.mkdir()
    (root / "acquire.toml").write_text('hosts = ["192.0.2.1"]\n')
    assert _by_id(run_checks(_context(root, tmp_path / "lake")))["team"].status == "ok"


def test_robot_for_another_season_only(tmp_path: Path) -> None:
    root = _ignite(tmp_path)
    robot = _by_id(run_checks(_context(root, tmp_path / "lake", season=2027)))["robot-season"]
    assert robot.status == "problem" and "2027" in robot.message


def test_lake_states(tmp_path: Path) -> None:
    root = _ignite(tmp_path)
    missing = _by_id(run_checks(_context(root, tmp_path / "lake")))["lake"]
    assert missing.status == "warning" and "not created" in missing.message
    (tmp_path / "lake").mkdir()
    assert _by_id(run_checks(_context(root, tmp_path / "lake")))["lake"].status == "ok"


@pytest.mark.skipif(os.name == "nt" or os.geteuid() == 0, reason="needs POSIX permissions")
def test_lake_not_writable(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    lake.chmod(0o500)
    try:
        result = _by_id(run_checks(_context(_ignite(tmp_path), lake)))["lake"]
    finally:
        lake.chmod(0o700)
    assert result.status == "problem" and "write" in result.message


# --- owlet --------------------------------------------------------------------------------


def _lake_with_hoot(lake: Path, compliancy: int) -> None:
    paths = LakePaths(lake)
    paths.meta.mkdir(parents=True)
    ledger = Ledger(paths.ledger)
    ledger.execute(  # a hoot of this compliancy was ingested
        "INSERT INTO hoot_logs (log_id, compliancy) VALUES (?, ?)", (f"h{compliancy}", compliancy)
    )
    ledger.close()


def _fake_owlet(cache: Path, compliancy: int, content: bytes) -> None:
    registry = hoot.default_registry(cache)
    target = registry.cached_path(compliancy)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)


def test_owlet_per_compliancy_in_the_lake(tmp_path: Path) -> None:
    compliancy = hoot.default_registry(tmp_path).compliancies()[-1]
    lake = tmp_path / "lake"
    _lake_with_hoot(lake, compliancy)
    ctx = _context(_ignite(tmp_path), lake)
    owlet = _by_id(run_checks(ctx))[f"owlet:C{compliancy}"]
    assert owlet.status == "problem" and owlet.fix_action == "owlet"
    _fake_owlet(ctx.cache_root, compliancy, b"not owlet")
    owlet = _by_id(run_checks(ctx))[f"owlet:C{compliancy}"]
    assert owlet.status == "problem" and "checksum" in owlet.message


def test_owlet_with_no_hoots_checks_the_newest(tmp_path: Path) -> None:
    newest = hoot.default_registry(tmp_path).compliancies()[-1]
    owlet = _by_id(run_checks(_context(_ignite(tmp_path), tmp_path / "lake")))
    result = owlet[f"owlet:C{newest}"]
    assert result.status == "warning" and result.fix_action == "owlet"


def test_owlet_cached_is_ok(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    compliancy = hoot.default_registry(tmp_path).compliancies()[-1]
    ctx = _context(_ignite(tmp_path), tmp_path / "lake")
    _fake_owlet(ctx.cache_root, compliancy, b"owlet")
    monkeypatch.setattr(
        hoot.OwletRegistry, "binary_for", lambda self, c, download=True: self.cached_path(c)
    )
    assert _by_id(run_checks(ctx))[f"owlet:C{compliancy}"].status == "ok"


# --- AdvantageScope, service, Python ------------------------------------------------------


def test_advantagescope(tmp_path: Path) -> None:
    root = _ignite(tmp_path)

    class Install:
        path = Path("/Applications/AdvantageScope.app")
        found_by = "standalone"

    found = _context(root, tmp_path / "lake", find_advantagescope=lambda r: (Install(), None))
    assert _by_id(run_checks(found))["advantagescope"].status == "ok"
    missing = _by_id(run_checks(_context(root, tmp_path / "lake")))["advantagescope"]
    assert missing.status == "warning" and missing.fix_action == "advantagescope"
    broken = _context(root, tmp_path / "lake", find_advantagescope=lambda r: (None, "no such"))
    result = _by_id(run_checks(broken))["advantagescope"]
    assert result.status == "problem" and "no such" in result.message


def _runner(outputs: dict[str, tuple[int, str]]) -> Callable[..., Any]:
    def run(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
        for key, (code, out) in outputs.items():
            if key in " ".join(args):
                return subprocess.CompletedProcess(list(args), code, out, "")
        raise FileNotFoundError(args[0])

    return run


@pytest.mark.parametrize(
    ("outputs", "status", "fix"),
    [
        ({"is-enabled": (0, "enabled\n"), "is-active": (0, "active\n")}, "ok", None),
        (
            {"is-enabled": (0, "enabled\n"), "is-active": (3, "inactive\n")},
            "warning",
            "service-start",
        ),
        (
            {"is-enabled": (1, "not-found\n"), "is-active": (3, "inactive\n")},
            "warning",
            "service-install",
        ),
        ({}, "warning", None),  # no systemctl
    ],
)
def test_acquire_service_linux(
    tmp_path: Path, outputs: dict[str, tuple[int, str]], status: str, fix: str | None
) -> None:
    ctx = _context(_ignite(tmp_path), tmp_path / "lake", system="Linux", run=_runner(outputs))
    result = _by_id(run_checks(ctx))["acquire-service"]
    assert (result.status, result.fix_action) == (status, fix)


@pytest.mark.parametrize(
    ("outputs", "status", "fix"),
    [
        ({"schtasks": (0, '"\\flashpoint-acquire","N/A","Running"\n')}, "ok", None),
        ({"schtasks": (0, '"\\flashpoint-acquire","N/A","Ready"\n')}, "warning", "service-start"),
        ({"schtasks": (1, "")}, "warning", "service-install"),
    ],
)
def test_acquire_service_windows(
    tmp_path: Path, outputs: dict[str, tuple[int, str]], status: str, fix: str | None
) -> None:
    ctx = _context(_ignite(tmp_path), tmp_path / "lake", system="Windows", run=_runner(outputs))
    result = _by_id(run_checks(ctx))["acquire-service"]
    assert (result.status, result.fix_action) == (status, fix)


def test_acquire_service_not_applicable_on_macos(tmp_path: Path) -> None:
    def never(args: Sequence[str]) -> Any:
        raise AssertionError("no service query on macOS")

    ctx = _context(_ignite(tmp_path), tmp_path / "lake", system="Darwin", run=never)
    result = _by_id(run_checks(ctx))["acquire-service"]
    assert result.status == "n/a" and "macOS" in result.message


@pytest.mark.parametrize(
    ("version", "status"),
    [((3, 11), "ok"), ((3, 13), "ok"), ((3, 10), "problem"), ((3, 14), "problem")],
)
def test_python_version(tmp_path: Path, version: tuple[int, int], status: str) -> None:
    ctx = _context(_ignite(tmp_path), tmp_path / "lake", python=version)
    assert _by_id(run_checks(ctx))["python"].status == status


def test_results_are_json_ready(tmp_path: Path) -> None:
    import json

    results = run_checks(_context(tmp_path / "none", tmp_path / "lake"))
    data = json.loads(json.dumps([r.to_dict() for r in results]))
    assert {
        "id",
        "title",
        "status",
        "message",
        "fix_action",
        "fix_label",
        "command",
        "source",
        "default",
    } <= set(data[0])


@pytest.mark.corpus
def test_checks_are_fast_on_the_corpus_lake(corpus_lake: LakePaths) -> None:
    ctx = CheckContext.default(config.config_root(), corpus_lake)
    started = time.perf_counter()
    results = run_checks(ctx)
    elapsed = time.perf_counter() - started
    assert any(r.id.startswith("owlet:C") for r in results)
    assert elapsed < CHECKS_BUDGET_S, f"checks took {elapsed:.2f} s"
