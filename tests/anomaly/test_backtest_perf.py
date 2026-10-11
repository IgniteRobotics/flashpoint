"""Backtest budget (spec: detection-backtest / Two-season budget): 120 matches x 23 slots, model
off, under 60 s and 1 GB."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tests.anomaly.gold_builder import GoldLake, steady

pytestmark = pytest.mark.perf

BUDGET_S = 60.0
BUDGET_RSS_BYTES = 1 << 30

_BACKTEST = """
import json, resource, sys, time
from pathlib import Path
from flashpoint import config
from flashpoint.anomaly import backtest
from flashpoint.lake.paths import LakePaths

def own_peak():
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

started = time.perf_counter()
result = backtest.backtest(LakePaths(Path(sys.argv[1])), config.config_root())
elapsed = time.perf_counter() - started
print(json.dumps({"backtest_s": elapsed, "peak_bytes": own_peak(),
                  "appearances": len(result.report["appearances"])}))
"""


def test_two_season_budget(tmp_path: Path) -> None:
    gold = GoldLake(tmp_path / "lake", matches=120)
    gold.keys = [f"{2025 + i // 60}syn_qm{i % 60 + 1}" for i in range(120)]
    for k, slot in enumerate(gold.slot_ids()):
        gold.set(slot, "temp_max_c", steady(40.0 + k % 5, 120, start=k))
        gold.set(slot, "stator_current_p95", steady(30.0 + k % 7, 120, spread=2.0, start=k))
    gold.set("hood", "temp_max_c", steady(45.0, 70) + steady(56.0, 50))
    gold.write()
    done = subprocess.run([sys.executable, "-c", _BACKTEST, str(gold.root)],
                          capture_output=True, text=True)  # fmt: skip
    assert done.returncode == 0, done.stdout + done.stderr
    measured = json.loads(done.stdout.strip().splitlines()[-1])
    print(json.dumps({"backtest_s": round(measured["backtest_s"], 2),
                      "backtest_peak_mb": round(measured["peak_bytes"] / 2**20),
                      "appearances": measured["appearances"]}))  # fmt: skip
    assert measured["appearances"] > 0
    assert measured["backtest_s"] < BUDGET_S
    assert measured["peak_bytes"] < BUDGET_RSS_BYTES
