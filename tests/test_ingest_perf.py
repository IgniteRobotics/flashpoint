"""Ingest budget (spec: telemetry-lake / Ingest budget): one qual match < 30 s and < 1 GB."""

import json
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

import pytest

pytestmark = [pytest.mark.corpus, pytest.mark.perf]

BUDGET_S = 30.0
BUDGET_RSS_BYTES = 1 << 30


_MEASURE = """
import json, resource, sys
from flashpoint.cli import main
code = main(sys.argv[1:])
scale = 1 if sys.platform == "darwin" else 1024  # Linux reports KiB
print(json.dumps({
    "flashpoint_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * scale,
    "owlet_peak_bytes": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * scale,
}))
sys.exit(code)
"""


def test_q7_match_ingest_budget(corpus_group: Callable[[str], list[Path]], tmp_path: Path) -> None:
    match_dir = tmp_path / "q7"
    match_dir.mkdir()
    for path in corpus_group("2026-gacmp-q7"):
        (match_dir / path.name).symlink_to(path)

    # Warm-up (excluded by the spec): owlet download and compiled-kernel cache.
    warm = [sys.executable, "-m", "flashpoint", "doctor"]
    subprocess.run(warm, check=True, capture_output=True)

    started = time.perf_counter()
    done = subprocess.run(
        [
            sys.executable,
            "-c",
            _MEASURE,
            "ingest",
            "--lake",
            str(tmp_path / "lake"),
            str(match_dir),
        ],
        capture_output=True,
        text=True,
    )
    elapsed = time.perf_counter() - started
    peaks = json.loads(done.stdout.strip().splitlines()[-1])
    flashpoint_mb = peaks["flashpoint_peak_bytes"] / 2**20
    owlet_mb = peaks["owlet_peak_bytes"] / 2**20

    print(json.dumps({"elapsed_s": round(elapsed, 2), "flashpoint_peak_mb": round(flashpoint_mb),
                      "owlet_peak_mb": round(owlet_mb)}))  # fmt: skip
    assert done.returncode == 0, done.stdout + done.stderr
    assert "3 ingested" in done.stdout
    assert elapsed < BUDGET_S
    assert peaks["flashpoint_peak_bytes"] < BUDGET_RSS_BYTES
    assert peaks["owlet_peak_bytes"] < BUDGET_RSS_BYTES
