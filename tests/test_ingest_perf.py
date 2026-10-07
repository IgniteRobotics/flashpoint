"""Ingest budget (spec: telemetry-lake / Ingest budget): one qual match < 30 s and < 1 GB."""

import json
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

import pytest

pytestmark = [pytest.mark.corpus, pytest.mark.perf]

BUDGET_S = 30.0
BUDGET_RSS_BYTES = 1 << 30
SIGNALS_BUDGET_S = 2.0  # nt-signal-mapping design: signals add at most 2 s and 150 MB
SIGNALS_BUDGET_RSS_BYTES = 150 << 20


# Runs the CLI in a fresh process and reports its own peak RSS.
# Linux keeps ru_maxrss across exec (a child inherits the parent's high-water mark), so there
# the process's own peak comes from VmHWM, which exec resets. For the same reason, children's
# ru_maxrss on Linux is inflated by the forking parent, so the children's peak (owlet conversions
# and the derive subprocess) is asserted on macOS only.
_MEASURE = """
import json, resource, sys
from flashpoint.cli import main

def own_peak():
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

code = main(sys.argv[1:])
scale = 1 if sys.platform == "darwin" else 1024  # Linux reports KiB
print(json.dumps({
    "flashpoint_peak_bytes": own_peak(),
    "children_peak_bytes": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * scale,
}))
sys.exit(code)
"""
_WARM_UP = """
from flashpoint import config
from flashpoint.readers import hoot
from flashpoint.cli import main
hoot.default_registry(config.cache_root()).binary_for(19)  # converter download
main(["doctor"])  # imports and compiled-kernel cache
"""


def test_q7_match_ingest_budget(corpus_group: Callable[[str], list[Path]], tmp_path: Path) -> None:
    match_dir = tmp_path / "q7"
    match_dir.mkdir()
    for path in corpus_group("2026-gacmp-q7"):
        (match_dir / path.name).symlink_to(path)

    # Warm-up (excluded by the spec): converter download and compiled-kernel cache.
    subprocess.run([sys.executable, "-c", _WARM_UP], check=True, capture_output=True)

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
    children_mb = peaks["children_peak_bytes"] / 2**20

    print(json.dumps({"elapsed_s": round(elapsed, 2), "flashpoint_peak_mb": round(flashpoint_mb),
                      "children_peak_mb": round(children_mb)}))  # fmt: skip
    assert done.returncode == 0, done.stdout + done.stderr
    assert "3 ingested" in done.stdout
    assert "derived 1 of 1 session(s)" in done.stdout  # ingest + derive (P2)
    assert elapsed < BUDGET_S
    assert peaks["flashpoint_peak_bytes"] < BUDGET_RSS_BYTES
    if sys.platform == "darwin":
        assert peaks["children_peak_bytes"] < BUDGET_RSS_BYTES


def _measure(args: list[str], env: dict[str, str] | None = None) -> tuple[float, dict[str, int]]:
    started = time.perf_counter()
    done = subprocess.run(
        [sys.executable, "-c", _MEASURE, *args], capture_output=True, text=True, env=env
    )
    elapsed = time.perf_counter() - started
    assert done.returncode == 0, done.stdout + done.stderr
    return elapsed, json.loads(done.stdout.strip().splitlines()[-1])


def test_q7_signal_step_budget(corpus_group: Callable[[str], list[Path]], tmp_path: Path) -> None:
    """Derive Q7 with and without the declared signals; the difference is the signal step."""
    import os

    from flashpoint import config

    match_dir = tmp_path / "q7"
    match_dir.mkdir()
    for path in corpus_group("2026-gacmp-q7"):
        (match_dir / path.name).symlink_to(path)
    subprocess.run([sys.executable, "-c", _WARM_UP], check=True, capture_output=True)
    lake = tmp_path / "lake"
    _measure(["ingest", "--no-derive", "--lake", str(lake), str(match_dir)])

    bare = tmp_path / "config-no-signals"
    shutil.copytree(config.config_root(), bare)
    for robot in (bare / "robots").glob("*.toml"):
        robot.write_text(re.split(r"^\[\[signal\]\]", robot.read_text(), maxsplit=1, flags=re.M)[0])
    shutil.rmtree(bare / "seasons")

    runs = {}
    for name, root in (("without", bare), ("with", config.config_root())):
        env = {**os.environ, "FLASHPOINT_CONFIG": str(root)}
        runs[name] = _measure(["derive", "--all", "--lake", str(lake)], env)
    partition = sum(f.stat().st_size for f in (lake / "silver" / "signals").rglob("*.parquet"))
    added_s = runs["with"][0] - runs["without"][0]
    added_mb = (
        runs["with"][1]["flashpoint_peak_bytes"] - runs["without"][1]["flashpoint_peak_bytes"]
    ) / 2**20
    print(json.dumps({"signal_step_s": round(added_s, 2), "signal_step_peak_mb": round(added_mb),
                      "derive_s": round(runs["with"][0], 2),
                      "q7_signals_partition_kb": round(partition / 1024)}))  # fmt: skip
    assert partition > 0
    assert added_s < SIGNALS_BUDGET_S
    assert added_mb < SIGNALS_BUDGET_RSS_BYTES / 2**20
