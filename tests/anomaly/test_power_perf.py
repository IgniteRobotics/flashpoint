"""Brownout count budget (spec: anomaly-detection / Detection budget): counting a match not yet
counted adds at most 0.5 s."""

import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

pytestmark = [pytest.mark.corpus, pytest.mark.perf]

BUDGET_S = 0.5

# A fresh process counts Q7 with an empty cache (or only loads the modules, for the baseline).
# Peak RSS is VmHWM on Linux (ru_maxrss survives exec there) and ru_maxrss on macOS.
_COUNT = """
import json, resource, sys, time
from pathlib import Path
from flashpoint import config
from flashpoint.anomaly import power
from flashpoint.lake.paths import LakePaths
from flashpoint.report.settings import load_report_config
from flashpoint.semantics.robot_config import load_robots

def own_peak():
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

lake, count = LakePaths(Path(sys.argv[1])), sys.argv[2] == "count"
report = load_report_config(config.config_root())
robots = load_robots(config.config_root() / "robots")
(power.anomalies_dir(lake) / power.POWER_FILE).unlink(missing_ok=True)
started = time.perf_counter()
if count:
    assert power.brownout_counts(lake, report, robots).recounted == ["2026gacmp_qm7"]
elapsed = time.perf_counter() - started
print(json.dumps({"count_s": elapsed, "peak_bytes": own_peak()}))
"""


def _run(lake: Path, mode: str) -> dict[str, float]:
    done = subprocess.run(
        [sys.executable, "-c", _COUNT, str(lake), mode], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stdout + done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])  # type: ignore[no-any-return]


def test_q7_uncached_brownout_count(
    corpus_group: Callable[[str], list[Path]], tmp_path: Path
) -> None:
    match_dir = tmp_path / "q7"
    match_dir.mkdir()
    for path in corpus_group("2026-gacmp-q7"):
        (match_dir / path.name).symlink_to(path)
    lake = tmp_path / "lake"
    subprocess.run(
        [sys.executable, "-m", "flashpoint", "ingest", "--lake", str(lake), str(match_dir)],
        check=True,
        capture_output=True,
    )
    _run(lake, "count")  # warm the OS file cache for both measured runs
    without, counted = _run(lake, "baseline"), _run(lake, "count")
    added_mb = (counted["peak_bytes"] - without["peak_bytes"]) / 2**20
    print(json.dumps({"brownout_count_s": round(counted["count_s"], 3),
                      "brownout_peak_added_mb": round(added_mb)}))  # fmt: skip
    assert counted["count_s"] <= BUDGET_S
