"""Ingest budget (spec: telemetry-lake / Ingest budget): one qual match < 30 s and < 1 GB."""

import json
import resource
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

import pytest

pytestmark = [pytest.mark.corpus, pytest.mark.perf]

BUDGET_S = 30.0
BUDGET_RSS_BYTES = 1 << 30


def _max_rss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024  # Linux reports KiB


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
            "-m",
            "flashpoint",
            "ingest",
            "--lake",
            str(tmp_path / "lake"),
            str(match_dir),
        ],
        capture_output=True,
        text=True,
    )
    elapsed = time.perf_counter() - started
    peak = _max_rss_bytes()

    print(json.dumps({"elapsed_s": round(elapsed, 2), "peak_rss_mb": round(peak / 2**20)}))
    assert done.returncode == 0, done.stdout + done.stderr
    assert "3 ingested" in done.stdout
    assert elapsed < BUDGET_S
    assert peak < BUDGET_RSS_BYTES
