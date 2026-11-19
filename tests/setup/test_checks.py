import datetime as dt
import json
import os
import shutil
import subprocess
import sys
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


def test_robot_for_a_season_with_no_season_file(tmp_path: Path) -> None:
    root = _ignite(tmp_path)
    robot = root / "robots" / "2026-comp.toml"
    text = robot.read_text().replace("season = 2026", "season = 2027", 1)
    (root / "robots" / "2027-comp.toml").write_text(text.replace('"2026-comp"', '"2027-comp"', 1))
    checks = _by_id(run_checks(_context(root, tmp_path / "lake", season=2027)))
    bad = checks["file:robots/2027-comp.toml"]
    assert bad.status == "problem" and "seasons/2027.toml" in bad.message
    # The fix is the season file, not the robot file (native separators: a path to type).
    assert bad.command is not None and str(root / "seasons" / "2027.toml") in bad.command
    assert str(root / "seasons" / "2026.toml") in bad.command


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


def _drafted_robot(root: Path, subsystems: list[str], comment: bool = True) -> None:
    slots = "".join(
        f'[[slot]]\nid = "talonfx-{i}"\nbus = "rio"\nmodel = "TalonFX"\ncan_id = {i}\n'
        f'subsystem = "{name}"\nrole = ""\n\n'
        for i, name in enumerate(subsystems, start=1)
    )
    header = (
        "# Drafted automatically by flashpoint from 2026gacmp_qm7 on 2026-11-19."
        " Name each slot's subsystem and role in Setup > Robots.\n"
        if comment
        else ""
    )
    (root / "robots").mkdir(exist_ok=True)
    (root / "robots" / "2026-comp.toml").write_text(
        header + 'robot = "2026-comp"\nseason = 2026\nproject = "Robot-2026"\n\n' + slots,
        encoding="utf-8",
    )


def test_drafted_robot_not_named_yet_is_a_warning(tmp_path: Path) -> None:
    from flashpoint.setup.init import init_config

    root = tmp_path / "cfg"
    init_config(root, team=254, lake=tmp_path / "lake")
    _drafted_robot(root, ["", "drive", ""])
    check = _by_id(run_checks(_context(root, tmp_path / "lake")))["robot-season"]
    assert check.status == "warning"
    assert "2 of 3 slots" in check.message and "unnamed" in check.message
    assert "Setup > Robots" in (check.command or "")


def test_named_drafted_robot_and_hand_written_robot_are_ok(tmp_path: Path) -> None:
    from flashpoint.setup.init import init_config

    root = tmp_path / "cfg"
    init_config(root, team=254, lake=tmp_path / "lake")
    _drafted_robot(root, ["drive", "intake"])
    assert _by_id(run_checks(_context(root, tmp_path / "lake")))["robot-season"].status == "ok"
    _drafted_robot(root, ["", ""], comment=False)  # the team's own file: not ours to nag about
    assert _by_id(run_checks(_context(root, tmp_path / "lake")))["robot-season"].status == "ok"


def test_lake_states(tmp_path: Path) -> None:
    root = _ignite(tmp_path)
    missing = _by_id(run_checks(_context(root, tmp_path / "lake")))["lake"]
    assert missing.status == "warning" and "not created" in missing.message
    (tmp_path / "lake").mkdir()
    assert _by_id(run_checks(_context(root, tmp_path / "lake")))["lake"].status == "ok"


def _posix_non_root() -> bool:
    if sys.platform == "win32":
        return False
    return os.geteuid() != 0


@pytest.mark.skipif(not _posix_non_root(), reason="needs POSIX permissions, not as root")
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


def test_hoot_format_newer_than_this_release_names_uv_then_pipx(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _lake_with_hoot(lake, 999)  # no owlet in the manifest reads C999
    check = _by_id(run_checks(_context(_ignite(tmp_path), lake)))["owlet:C999"]
    command = check.command or ""
    assert check.status == "problem"
    assert "uv tool upgrade flashpoint" in command and "pipx upgrade flashpoint" in command
    assert command.index("uv tool upgrade") < command.index("pipx upgrade")


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
    [
        ((3, 11), "ok"),
        ((3, 13), "ok"),
        ((3, 14), "ok"),
        ((3, 10), "problem"),
        ((3, 15), "problem"),
    ],
)
def test_python_version(tmp_path: Path, version: tuple[int, int], status: str) -> None:
    ctx = _context(_ignite(tmp_path), tmp_path / "lake", python=version)
    assert _by_id(run_checks(ctx))["python"].status == status


def test_unsupported_python_names_the_range_and_the_uv_install(tmp_path: Path) -> None:
    ctx = _context(_ignite(tmp_path), tmp_path / "lake", python=(3, 10))
    check = _by_id(run_checks(ctx))["python"]
    assert check.status == "problem" and "3.11 to 3.14" in check.message
    assert (check.command or "").startswith("uv tool install --python 3.13 flashpoint")


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


# --- backup (spec: backup-status) -----------------------------------------------------------

BACKUP_NOW = dt.datetime(2026, 10, 24, 12, 0, tzinfo=dt.UTC)


def _backup_lake(tmp_path: Path, log_age_h: float, remote: str | None) -> tuple[Path, Path]:
    root = tmp_path / "cfg"
    root.mkdir()
    (root / "acquire.toml").write_text(
        "team = 6829\n" + (f'[backup]\nremote = "{remote}"\n' if remote else ""), encoding="utf-8"
    )
    lake = LakePaths(tmp_path / "lake")
    lake.ensure()
    ledger = Ledger(lake.ledger)
    try:
        if log_age_h >= 0:
            ledger.register("a" * 64, "wpilog", 1, lake.raw / "a")
            seen = (BACKUP_NOW - dt.timedelta(hours=log_age_h)).isoformat(timespec="seconds")
            ledger.execute("UPDATE files SET first_seen = ?", (seen,))
    finally:
        ledger.close()
    return root, lake.root


def _status(lake: Path, last_ok_h: float, error: str | None = None) -> None:
    ok = (BACKUP_NOW - dt.timedelta(hours=last_ok_h)).isoformat()
    slot = {"last_time": ok, "result": "ok: x", "pending": False, "last_ok": ok, "config_sha": None}
    if error:
        slot |= {"result": f"error: {error}", "last_time": BACKUP_NOW.isoformat()}
    status = LakePaths(lake).status
    status.write_text(json.dumps({"version": 1, "backup": slot}), encoding="utf-8")


def _with_rclone(name: str) -> str | None:
    return f"/usr/bin/{name}"


def _backup_check(
    root: Path,
    lake: Path,
    calls: list[Sequence[str]] | None = None,
    which: Callable[[str], str | None] = _with_rclone,
) -> CheckResult:
    def run(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
        if calls is not None:
            calls.append(args)
        return _ok_run(args)

    ctx = _context(root, lake, now=lambda: BACKUP_NOW, run=run, which=which)
    return _by_id(run_checks(ctx))["backup"]


def test_backup_offline_for_two_days_is_a_problem(tmp_path: Path) -> None:
    root, lake = _backup_lake(tmp_path, 48, "gdrive:flashpoint")
    _status(lake, 72, error="rclone copy exited 1: no route to host")

    check = _backup_check(root, lake)

    assert check.status == "problem"
    assert "1 log" in check.message and "2 days" in check.message
    assert "no route to host" in check.message
    assert check.command == "flashpoint backup"


def test_backup_failing_but_not_overdue_is_a_warning(tmp_path: Path) -> None:
    root, lake = _backup_lake(tmp_path, 3, "gdrive:flashpoint")
    _status(lake, 5, error="connection refused")

    check = _backup_check(root, lake)

    assert check.status == "warning" and "connection refused" in check.message
    assert check.command == "flashpoint backup"


PUBLISHED = "https://igniterobotics.github.io/flashpoint/"
RCLONE_INSTALL = "https://rclone.org/install/"


def _without_rclone(name: str) -> str | None:
    return None


def test_backup_not_configured_is_ok_and_optional(tmp_path: Path) -> None:
    root, lake = _backup_lake(tmp_path, 1, None)

    check = _backup_check(root, lake, which=_without_rclone)

    assert check.status == "ok" and check.optional
    assert "only on this machine" in check.message
    command = check.command or ""
    assert command.startswith(f"see {PUBLISHED}") and command.endswith("#backup-and-restore")
    assert check.to_dict()["optional"] is True


def test_backup_configured_without_rclone_is_a_warning(tmp_path: Path) -> None:
    root, lake = _backup_lake(tmp_path, 3, "gdrive:flashpoint")

    check = _backup_check(root, lake, which=_without_rclone)

    assert check.status == "warning" and "rclone isn't installed" in check.message
    command = check.command or ""
    assert RCLONE_INSTALL in command and "flashpoint backup" in command
    assert command.index(RCLONE_INSTALL) < command.index("flashpoint backup")
    assert not check.optional


def test_backup_overdue_without_rclone_names_installing_it_first(tmp_path: Path) -> None:
    root, lake = _backup_lake(tmp_path, 48, "gdrive:flashpoint")

    check = _backup_check(root, lake, which=_without_rclone)

    assert check.status == "problem" and "2 days" in check.message
    command = check.command or ""
    assert command.index("rclone") < command.index("flashpoint backup")


def test_backup_unreadable_records_is_a_warning_and_the_rest_still_run(tmp_path: Path) -> None:
    root, lake = _backup_lake(tmp_path, 3, "gdrive:flashpoint")
    LakePaths(lake).ledger.write_bytes(b"this is not a sqlite database" * 100)

    ctx = _context(root, lake, now=lambda: BACKUP_NOW, which=_with_rclone)
    results = _by_id(run_checks(ctx))

    assert results["backup"].status == "warning"
    assert "can't read" in results["backup"].message
    assert {"python", "acquire-service", "advantagescope", "lake"} <= set(results)


@pytest.mark.corpus
def test_backup_not_configured_adds_no_doctor_warning(
    corpus_lake: LakePaths,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from flashpoint.cli import main
    from flashpoint.setup.init import init_config

    root = tmp_path / "cfg"
    init_config(root, team=254, lake=corpus_lake.root)
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))

    main(["doctor", "--lake", str(corpus_lake.root)])

    out = capsys.readouterr().out
    (line,) = [ln for ln in out.splitlines() if " Backup " in ln]
    assert line.split()[0] == "ok" and "optional" in line
    assert PUBLISHED in out and "#backup-and-restore" in out


def test_backup_with_an_empty_lake_is_ok(tmp_path: Path) -> None:
    root, lake = _backup_lake(tmp_path, -1, "gdrive:flashpoint")

    check = _backup_check(root, lake)

    assert check.status == "ok" and "nothing to back up" in check.message
    assert check.command is None


def test_backup_ok_names_the_last_success_and_never_runs_rclone(tmp_path: Path) -> None:
    root, lake = _backup_lake(tmp_path, 30, "gdrive:flashpoint")
    _status(lake, 2)
    calls: list[Sequence[str]] = []

    check = _backup_check(root, lake, calls)

    assert check.status == "ok" and "last backup 2 h ago" in check.message
    assert not any("rclone" in " ".join(c) for c in calls)


def test_doctor_exits_non_zero_when_the_backup_is_overdue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from flashpoint.cli import main

    root = _ignite(tmp_path)
    (root / "acquire.toml").write_text(
        'team = 6829\n[backup]\nremote = "gdrive:flashpoint"\n', encoding="utf-8"
    )
    lake = LakePaths(tmp_path / "lake")
    lake.ensure()
    ledger = Ledger(lake.ledger)
    try:
        ledger.register("b" * 64, "wpilog", 1, lake.raw / "b")
        ledger.execute("UPDATE files SET first_seen = '2026-01-01T00:00:00+00:00'")
    finally:
        ledger.close()
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))

    assert main(["doctor", "--lake", str(lake.root)]) == 1

    out = capsys.readouterr().out
    assert "Backup" in out and "not backed up for" in out
