"""The sample lakes `tools/docs-media.py` builds ride out a file that fails once (#123).

`flashpoint ingest` is stubbed: the tool's `subprocess.run` returns scripted results, and the
summary text is printed by the real `flashpoint.cli._summarize`, so these tests read the same
wording the deploy does.
"""

import contextlib
import importlib.util
import io
import re
import subprocess
from collections.abc import Callable
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from flashpoint.cli import _summarize
from flashpoint.ingest import FileResult, IngestReport

REPO = Path(__file__).resolve().parents[2]
ENTRIES = [{"name": "X.hoot"}, {"name": "Y.hoot"}, {"name": "a.wpilog"}]


def _tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("docs_media", REPO / "tools" / "docs-media.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _report(
    failed: tuple[str, ...] = (),
    quarantined: tuple[str, ...] = (),
    earlier: tuple[str, ...] = (),
    ingested: tuple[str, ...] = (),
) -> IngestReport:
    def result(name: str, status: str, reason: str | None) -> FileResult:
        return FileResult(Path("/logs") / name, "0" * 64, Path(name).suffix[1:], status, reason)

    return IngestReport(
        results=[result(n, "failed", "owlet: exit -11: no output") for n in failed]
        + [result(n, "quarantined", "empty file") for n in quarantined]
        + [result(n, "quarantined-earlier", "empty file") for n in earlier]
        + [result(n, "success", None) for n in ingested],
        elapsed_s=1.5,
    )


def summary(
    failed: tuple[str, ...] = (),
    quarantined: tuple[str, ...] = (),
    earlier: tuple[str, ...] = (),
    ingested: tuple[str, ...] = (),
) -> str:
    """The stdout `flashpoint ingest` prints for a run with these results."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        _summarize(_report(failed, quarantined, earlier, ingested))
    return out.getvalue()


class FakeRun:
    """Stands in for the tool's `subprocess.run`: one scripted (exit, stdout) per call."""

    def __init__(self, ingest: list[tuple[int, str]], report: int = 0) -> None:
        self.ingest = list(ingest)
        self.report = report
        self.calls: list[list[str]] = []

    def __call__(self, cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(cmd))
        if cmd[3] == "ingest":
            code, stdout = self.ingest.pop(0)
        else:
            code, stdout = self.report, "Replay built\n"
        return subprocess.CompletedProcess(cmd, code, stdout, "")

    def runs(self, command: str) -> list[list[str]]:
        return [c for c in self.calls if c[3] == command]


@pytest.fixture
def build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[..., tuple[ModuleType, FakeRun, Callable[..., Path]]]:
    def make(
        ingest: list[tuple[int, str]], report: int = 0
    ) -> tuple[ModuleType, FakeRun, Callable[..., Path]]:
        tool = _tool()
        fake = FakeRun(ingest, report)
        monkeypatch.setattr(tool, "subprocess", SimpleNamespace(run=fake))

        def run(ok: tuple[int, ...] = (0,)) -> Path:
            lake: Path = tool.build_sample_lake(
                tmp_path / "work", tmp_path / "corpus", ENTRIES, "", ok=ok
            )
            return lake

        return tool, fake, run

    return make


def test_the_fake_ingest_prints_the_real_summary() -> None:
    text = summary(failed=("X.hoot",), quarantined=("Y.hoot",))
    assert "FAILED      X.hoot" in text
    assert "QUARANTINED Y.hoot" in text
    assert "0 ingested, 0 skipped, 1 failed, 1 quarantined, 0 quarantined earlier" in text


def test_a_file_that_fails_once_is_retried_and_imports(
    build: Callable[..., Any], capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _, fake, run = build(
        [(1, summary(failed=("X.hoot",), ingested=("Y.hoot",))), (0, summary(ingested=("X.hoot",)))]
    )
    assert run() == tmp_path / "work" / "lake"
    ingests = fake.runs("ingest")
    assert len(ingests) == 2
    assert ingests[0] == ingests[1]
    assert len(fake.runs("report")) == 1
    out = capsys.readouterr().out
    assert "ingest: retrying X.hoot (run 2 of 3)" in out
    assert re.search(r"retried\b.*X\.hoot", out)


def test_a_file_that_keeps_failing_stops_after_three_runs(
    build: Callable[..., Any],
) -> None:
    failed = (1, summary(failed=("X.hoot",)))
    _, fake, run = build([failed, failed, failed])
    with pytest.raises(SystemExit) as exc:
        run()
    message = str(exc.value.code)
    assert "X.hoot" in message
    assert "3 ingest runs" in message
    assert len(fake.runs("ingest")) == 3
    assert fake.runs("report") == []


def test_a_quarantined_file_is_not_retried(build: Callable[..., Any]) -> None:
    _, fake, run = build([(1, summary(quarantined=("Y.hoot",)))])
    with pytest.raises(SystemExit) as exc:
        run()
    message = str(exc.value.code)
    assert "Y.hoot" in message
    assert "quarantined" in message
    assert len(fake.runs("ingest")) == 1
    assert fake.runs("report") == []


def test_the_quarantine_lake_accepts_its_quarantine(
    build: Callable[..., Any], tmp_path: Path
) -> None:
    _, fake, run = build([(1, summary(quarantined=("Y.hoot",)))])
    assert run(ok=(0, 1)) == tmp_path / "work" / "lake"
    assert len(fake.runs("ingest")) == 1
    assert len(fake.runs("report")) == 1


def test_a_retry_that_quarantines_the_file_still_fails(build: Callable[..., Any]) -> None:
    _, fake, run = build([(1, summary(failed=("X.hoot",))), (1, summary(quarantined=("X.hoot",)))])
    with pytest.raises(SystemExit) as exc:
        run()
    message = str(exc.value.code)
    assert "X.hoot" in message
    assert "quarantined" in message
    assert len(fake.runs("ingest")) == 2
    assert fake.runs("report") == []


@pytest.mark.parametrize(
    "stdout",
    [
        summary(ingested=("X.hoot",)) + "derive failed: Replay data was not built.\n",
        "Traceback (most recent call last):\nRuntimeError: lake is locked\n",
    ],
    ids=["no-file-failed", "no-summary"],
)
def test_a_failure_that_isnt_a_file_is_not_retried(
    build: Callable[..., Any], capsys: pytest.CaptureFixture[str], stdout: str
) -> None:
    _, fake, run = build([(1, stdout)])
    with pytest.raises(SystemExit):
        run()
    assert len(fake.runs("ingest")) == 1
    assert fake.runs("report") == []
    assert stdout in capsys.readouterr().err


def test_the_parser_reads_the_real_ingest_summary() -> None:
    tool = _tool()
    stdout = (
        summary(failed=("X.hoot",), quarantined=("Y.hoot",), earlier=("Z.hoot",))
        + "derive: 3 matches, 0 failed in 2.0s\n"
    )
    parsed = tool.parse_ingest_summary(stdout)
    assert parsed is not None
    assert parsed.failed == ["X.hoot"]
    assert parsed.quarantined == ["Y.hoot"]
    assert parsed.counts == {
        "ingested": 0,
        "skipped": 0,
        "failed": 1,
        "quarantined": 1,
        "quarantined_earlier": 1,
    }


def test_the_parser_finds_no_summary_in_other_output() -> None:
    assert _tool().parse_ingest_summary("Traceback (most recent call last):\nboom\n") is None
