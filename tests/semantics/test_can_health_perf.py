"""CAN-health budget (spec: can-health / CAN health is rebuildable and bounded): the step adds
under 3 s and 150 MB to deriving one match."""

import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

pytestmark = [pytest.mark.corpus, pytest.mark.perf]

BUDGET_S = 3.0
BUDGET_RSS_BYTES = 150 << 20

# Loads a derived lake's Q7 session as derive does, then runs the CAN-health step (or not), in
# a fresh process. Peak RSS is VmHWM on Linux (ru_maxrss survives exec there) and ru_maxrss
# on macOS.
_STEP = """
import json, resource, sys, time
from pathlib import Path
from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.derive import Deriver

def own_peak():
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

lake, run_step = LakePaths(Path(sys.argv[1])), sys.argv[2] == "step"
deriver = Deriver(lake, config.config_root())
(session,) = deriver.ledger.query("SELECT * FROM sessions WHERE match_key = '2026gacmp_qm7'")
framing = deriver.framing_for(session["session_id"])
robot = next(r for r in deriver.robots if r.robot == session["robot"])
deriver.con.execute("SELECT 1")
started = time.perf_counter()
if run_step:
    deriver._write_can_health(session, framing, robot, "high", "", "perf")
elapsed = time.perf_counter() - started
print(json.dumps({"step_s": elapsed, "peak_bytes": own_peak()}))
"""


def _run(lake: Path, mode: str) -> dict[str, float]:
    done = subprocess.run(
        [sys.executable, "-c", _STEP, str(lake), mode], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stdout + done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])  # type: ignore[no-any-return]


def test_q7_can_health_budget(corpus_group: Callable[[str], list[Path]], tmp_path: Path) -> None:
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
    _run(lake, "step")  # warm the OS file cache for both measured runs
    without, with_step = _run(lake, "baseline"), _run(lake, "step")
    added_mb = (with_step["peak_bytes"] - without["peak_bytes"]) / 2**20
    print(json.dumps({"can_health_step_s": round(with_step["step_s"], 2),
                      "can_health_peak_mb": round(added_mb)}))  # fmt: skip
    assert with_step["step_s"] < BUDGET_S
    assert added_mb < BUDGET_RSS_BYTES / 2**20
