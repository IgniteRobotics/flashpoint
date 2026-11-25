"""A failed derive after `ingest` or `rebuild` is reported and fails the command (#107).

Spec: match-reports / Failed derive after a manual ingest.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.cli import main
from tests.conftest import REPO_CONFIG
from tests.wpilog_builder import WpilogBuilder

FAILED_LINE = "derive failed: Replay data was not built"
RETRY = "flashpoint derive --report"


class FakeRuns:
    """Stands in for `subprocess.run` in the CLI: records commands, returns chosen exit codes."""

    def __init__(self) -> None:
        self.codes = {"derive": 0, "report": 0}
        self.commands: list[list[str]] = []

    def __call__(self, args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        self.commands.append(list(args))
        return subprocess.CompletedProcess(args, self.codes[args[3]], "", "")

    def steps(self) -> list[str]:
        return [c[3] for c in self.commands]


@pytest.fixture
def runs(monkeypatch: pytest.MonkeyPatch) -> FakeRuns:
    fake = FakeRuns()
    monkeypatch.setattr(subprocess, "run", fake)
    return fake


def _wpilog(path: Path) -> Path:
    return WpilogBuilder().start(1, "x", "double").double(1, 1, 1.0).write(path)


def _bad(tmp_path: Path) -> Path:
    bad = tmp_path / "bad.wpilog"
    bad.write_bytes(b"x" * 20)
    return bad


def _assert_reported(err: str) -> None:
    last = err.rstrip().splitlines()[-1]
    assert last.startswith(FAILED_LINE) and RETRY in last, err


@pytest.mark.parametrize("derive_code", [1, 2])
def test_failed_derive_fails_the_ingest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], runs: FakeRuns, derive_code: int
) -> None:
    runs.codes["derive"] = derive_code
    log = _wpilog(tmp_path / "a.wpilog")
    assert main(["ingest", "--lake", str(tmp_path / "lake"), str(log)]) == 1
    out = capsys.readouterr()
    assert "1 ingested, 0 skipped, 0 failed" in out.out
    _assert_reported(out.err)
    assert runs.steps() == ["derive"]


def test_quarantine_and_failed_derive_are_both_shown(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], runs: FakeRuns
) -> None:
    runs.codes["derive"] = 1
    log = _wpilog(tmp_path / "a.wpilog")
    lake = str(tmp_path / "lake")
    assert main(["ingest", "--lake", lake, str(log), str(_bad(tmp_path))]) == 1
    out = capsys.readouterr()
    assert "QUARANTINED bad.wpilog: invalid-header" in out.out
    assert "1 ingested, 0 skipped, 0 failed, 1 quarantined" in out.out
    _assert_reported(out.err)
    assert "report" not in runs.steps()


def test_rebuild_reports_a_failed_derive(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], runs: FakeRuns
) -> None:
    lake = str(tmp_path / "lake")
    assert main(["ingest", "--no-derive", "--lake", lake, str(_wpilog(tmp_path / "a.wpilog"))]) == 0
    runs.codes["derive"] = 1
    capsys.readouterr()
    with pytest.MonkeyPatch.context() as m:
        m.setattr(config, "PIPELINE_VERSION", config.PIPELINE_VERSION + 1)
        assert main(["rebuild", "--lake", lake]) == 1
    _assert_reported(capsys.readouterr().err)
    assert runs.steps() == ["derive"]


@pytest.mark.parametrize("with_bad", [False, True])
def test_no_derive_keeps_the_ingest_code(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], runs: FakeRuns, with_bad: bool
) -> None:
    runs.codes["derive"] = 1
    paths = [str(_wpilog(tmp_path / "a.wpilog"))] + ([str(_bad(tmp_path))] if with_bad else [])
    code = main(["ingest", "--no-derive", "--lake", str(tmp_path / "lake"), *paths])
    assert code == (1 if with_bad else 0)
    assert FAILED_LINE not in capsys.readouterr().err
    assert runs.steps() == []


def test_failed_replay_build_after_good_derive_keeps_exit_0(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], runs: FakeRuns
) -> None:
    runs.codes["report"] = 1
    log = _wpilog(tmp_path / "a.wpilog")
    assert main(["ingest", "--lake", str(tmp_path / "lake"), str(log)]) == 0
    err = capsys.readouterr().err
    assert "Replay build failed" in err and FAILED_LINE not in err
    assert runs.steps() == ["derive", "report"]


def test_real_derive_config_error_then_retry(
    tmp_path: Path, capfd: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "cfg"
    shutil.copytree(REPO_CONFIG, root)
    robot = root / "robots" / "2026-comp.toml"
    good = robot.read_text(encoding="utf-8")
    robot.write_text("season = [not toml\n", encoding="utf-8")
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    lake = tmp_path / "lake"

    assert main(["ingest", "--lake", str(lake), str(_wpilog(tmp_path / "a.wpilog"))]) == 1
    err = capfd.readouterr().err
    assert "robot configuration error" in err
    assert err.index("robot configuration error") < err.index(FAILED_LINE)
    _assert_reported(err)
    assert not (lake / "report" / "data" / "matches.js").exists()

    robot.write_text(good, encoding="utf-8")
    assert main(["derive", "--report", "--lake", str(lake)]) == 0
    assert (lake / "report" / "data" / "matches.js").exists()
