"""Detection budget (spec: anomaly-detection / Season budget): a second run over one season of
gold (60 matches x 23 slots, model off) takes under 5 s and 400 MB."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tests.anomaly.gold_builder import GoldLake, steady

pytestmark = pytest.mark.perf

BUDGET_S = 5.0
BUDGET_RSS_BYTES = 400 << 20

# Peak RSS is VmHWM on Linux (ru_maxrss survives exec there) and ru_maxrss on macOS.
_DETECT = """
import json, resource, sys, time
from pathlib import Path
from flashpoint import config
from flashpoint.anomaly import detect
from flashpoint.lake.paths import LakePaths

def own_peak():
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

lake = LakePaths(Path(sys.argv[1]))
started = time.perf_counter()
result = detect.detect(lake, config.config_root(), force=True)
elapsed = time.perf_counter() - started
assert result.ran
print(json.dumps({"detect_s": elapsed, "peak_bytes": own_peak(),
                  "anomalies": result.summary["anomalies"]}))
"""


def _run(lake: Path) -> dict[str, float]:
    done = subprocess.run([sys.executable, "-c", _DETECT, str(lake)], capture_output=True,
                          text=True)  # fmt: skip
    assert done.returncode == 0, done.stdout + done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])  # type: ignore[no-any-return]


def test_season_budget(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path / "lake", matches=60)
    for k, slot in enumerate(gold.slot_ids()):  # every slot of 2026-comp, varied histories
        gold.set(slot, "temp_max_c", steady(40.0 + k % 5, 60, start=k))
        gold.set(slot, "stator_current_p95", steady(30.0 + k % 7, 60, spread=2.0, start=k))
    gold.set("hood", "temp_max_c", steady(45.0, 40) + steady(56.0, 20))
    gold.write()
    _run(gold.root)  # the first run (and the OS file cache)
    second = _run(gold.root)
    print(json.dumps({"detect_s": round(second["detect_s"], 2),
                      "detect_peak_mb": round(second["peak_bytes"] / 2**20),
                      "anomalies": second["anomalies"]}))  # fmt: skip
    assert second["anomalies"] > 0
    assert second["detect_s"] < BUDGET_S
    assert second["peak_bytes"] < BUDGET_RSS_BYTES
