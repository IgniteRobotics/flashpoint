"""`flashpoint ingest --lake lake .` on a corpus match builds Replay data (#107).

Spec: telemetry-lake / Lake location and portability (Relative --lake on a corpus match).
"""

import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

pytestmark = pytest.mark.corpus

CHILD_TIMEOUT_S = 280  # under the 300 s per-test timeout


def test_relative_lake_ingest_builds_replay(
    corpus_group: Callable[[str], list[Path]], tmp_path: Path
) -> None:
    work = tmp_path / "work"
    work.mkdir()
    for path in corpus_group("2026-gacmp-q7"):
        (work / path.name).symlink_to(path)

    done = subprocess.run(
        [sys.executable, "-m", "flashpoint", "ingest", "--lake", "lake", "."],
        cwd=work, capture_output=True, text=True, timeout=CHILD_TIMEOUT_S, check=False,
    )  # fmt: skip

    assert "Traceback" not in done.stderr, done.stderr
    assert done.returncode == 0, done.stdout + done.stderr
    assert (work / "lake" / "report" / "data" / "matches.js").is_file()
