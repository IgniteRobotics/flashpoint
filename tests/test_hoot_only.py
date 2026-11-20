"""Hoot-only sessions are reported (spec: session-grouping, "Hoot-only sessions are reported")."""

import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest

from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.setup.checks import CheckContext, CheckResult, run_checks
from flashpoint.setup.init import init_config

WARNING = "hoots but no wpilog"


def _ok_run(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(list(args), 0, "active\n", "")


def _check(root: Path, lake: LakePaths, season: int = 2026) -> CheckResult:
    ctx = CheckContext(
        root, lake, lake.root.parent / "cache", system="Darwin", season=season, run=_ok_run,
        find_advantagescope=lambda r: (None, "not found"),
    )  # fmt: skip
    return {r.id: r for r in run_checks(ctx)}["hoot-only"]


def _sessions(lake: LakePaths, rows: list[tuple[str, str, bool]]) -> None:
    """(session id, season, has a wpilog); every session here has hoots."""
    lake.meta.mkdir(parents=True, exist_ok=True)
    ledger = Ledger(lake.ledger)
    try:
        for sid, season, wpilog in rows:
            ledger.execute(
                "INSERT INTO sessions (session_id, wpilog_id, season, robot) VALUES (?, ?, ?, ?)",
                (sid, f"w-{sid}" if wpilog else None, season, "unknown"),
            )
            ledger.execute(
                "INSERT INTO session_hoots (log_id, session_id, hoot_group, bus)"
                " VALUES (?, ?, 'g', 'rio')",
                (f"h-{sid}", sid),
            )
    finally:
        ledger.close()


@pytest.fixture
def root(tmp_path: Path) -> Path:
    path = tmp_path / "cfg"
    init_config(path, team=254, lake=tmp_path / "lake")
    return path


# --- the setup check ------------------------------------------------------------------------


def test_no_hoot_only_session_is_ok(tmp_path: Path, root: Path) -> None:
    lake = LakePaths(tmp_path / "lake")
    _sessions(lake, [("a", "2026", True)])
    check = _check(root, lake)
    assert check.status == "ok"


def test_some_hoot_only_sessions_is_ok_with_their_count(tmp_path: Path, root: Path) -> None:
    lake = LakePaths(tmp_path / "lake")
    _sessions(lake, [("a", "2026", True), ("b", "2026", False), ("c", "2026", True)])
    check = _check(root, lake)
    assert check.status == "ok" and "1 " in check.message


def test_every_hoot_session_of_the_latest_season_hoot_only_is_a_warning(
    tmp_path: Path, root: Path
) -> None:
    lake = LakePaths(tmp_path / "lake")
    _sessions(lake, [("old", "2025", True), ("b", "2026", False), ("c", "2026", False)])
    check = _check(root, lake)
    assert check.status == "warning"
    assert "2026" in check.message and "WPILib data logging" in (check.command or "")


def test_an_older_season_all_hoot_only_is_not_the_latest(tmp_path: Path, root: Path) -> None:
    lake = LakePaths(tmp_path / "lake")
    _sessions(lake, [("old", "2025", False), ("new", "2026", True)])
    assert _check(root, lake).status == "ok"


def test_empty_lake_has_nothing_to_report(tmp_path: Path, root: Path) -> None:
    assert _check(root, LakePaths(tmp_path / "lake")).status == "ok"


# --- corpus: the derive warning and the check after a real import -------------------------


@pytest.fixture
def init_env(root: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    return root


def _ingest(lake: Path, files: list[Path], capfd: pytest.CaptureFixture[str]) -> tuple[int, str]:
    from flashpoint.cli import main

    capfd.readouterr()
    code = main(["ingest", "--no-report", "--lake", str(lake), *map(str, files)])
    captured = capfd.readouterr()
    return code, captured.out + captured.err


def _warnings(output: str) -> list[str]:
    return [line for line in output.splitlines() if WARNING in line]


@pytest.mark.corpus
def test_hoot_logger_restart(
    tmp_path: Path,
    init_env: Path,
    corpus_group: Callable[[str], list[Path]],
    capfd: pytest.CaptureFixture[str],
) -> None:
    lake = tmp_path / "lake"
    code, output = _ingest(lake, corpus_group("2026-gacmp-e10"), capfd)
    assert code == 0
    (line,) = _warnings(output)
    assert "1 session" in line and "2026gacmp_e10" in line
    check = _check(init_env, LakePaths(lake))
    assert check.status == "ok" and "1 " in check.message


@pytest.mark.corpus
def test_hoots_only_then_a_rerun_with_nothing_new(
    tmp_path: Path,
    init_env: Path,
    corpus_group: Callable[[str], list[Path]],
    capfd: pytest.CaptureFixture[str],
) -> None:
    from flashpoint.cli import main

    lake = tmp_path / "lake"
    _code, output = _ingest(lake, corpus_group("2025-gacmp-q19"), capfd)
    (line,) = _warnings(output)
    assert "1 session" in line and "wpilog" in line
    check = _check(init_env, LakePaths(lake), season=2025)
    assert check.status == "warning" and "2025" in check.message
    assert "WPILib data logging" in (check.command or "")

    # Re-run with nothing new: no warning again.
    assert main(["derive", "--lake", str(lake)]) == 0
    captured: Any = capfd.readouterr()
    assert not _warnings(captured.out + captured.err)


@pytest.mark.corpus
def test_quarantined_hoot(
    tmp_path: Path,
    init_env: Path,
    corpus_group: Callable[[str], list[Path]],
    capfd: pytest.CaptureFixture[str],
) -> None:
    lake = tmp_path / "lake"
    _code, output = _ingest(lake, corpus_group("2026-gadal-q11-empty"), capfd)
    assert "QUARANTINED" in output
    assert not _warnings(output)
    check = _check(init_env, LakePaths(lake))
    assert check.status == "ok"
