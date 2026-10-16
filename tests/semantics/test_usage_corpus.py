"""Unit usage on the golden corpus (spec: unit-odometry / Per-session unit usage)."""

import json
import subprocess
import sys
from typing import Any

import duckdb
import pytest

from flashpoint import config
from flashpoint.semantics.derive import Deriver

pytestmark = pytest.mark.corpus

# A coasting drive regenerates a few joules after the match ends, so a whole session's net
# supply energy can sit just below its match window's (Q7 drive-br: 0.002 Wh).
REGEN_ALLOWANCE_WH = 0.01


@pytest.fixture(scope="module")
def usage_rows(corpus_lake: Any) -> list[dict[str, Any]]:
    deriver = Deriver(corpus_lake, config.config_root())
    try:
        deriver.run()
    finally:
        deriver.close()
    usage = corpus_lake.root / "gold" / "unit_usage" / "*" / "*" / "*.parquet"
    features = corpus_lake.root / "gold" / "match_features" / "*" / "*" / "*.parquet"
    con = duckdb.connect()
    cursor = con.execute(
        f"SELECT u.*, f.supply_energy_wh AS match_energy_wh FROM read_parquet('{usage}') u"
        f" LEFT JOIN (SELECT * FROM read_parquet('{features}') WHERE phase = 'match') f"
        " USING (session_id, slot_id, unit_id)"
    )
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _q7(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in rows if r["match_key"] == "2026gacmp_qm7"]


def test_q7_rows_for_every_unit(usage_rows: list[dict[str, Any]]) -> None:
    q7 = _q7(usage_rows)
    motors = [r for r in q7 if r["supply_energy_wh"] is not None]
    assert len(q7) == 22  # 17 motors + gyro + 4 steer encoders
    assert len(motors) == 17
    for row in q7:
        assert 335 < row["powered_s"] < 345  # the 341.8 s session
        assert 140 < row["enabled_s"] < 155  # auto + teleop
        assert row["alignment"] == "high" and row["session_start"]


def test_q7_usage_energy_covers_the_match(usage_rows: list[dict[str, Any]]) -> None:
    for row in _q7(usage_rows):
        if row["match_energy_wh"] is not None:
            assert row["supply_energy_wh"] >= row["match_energy_wh"] - REGEN_ALLOWANCE_WH, row


def test_q7_thermal_cycles(usage_rows: list[dict[str, Any]]) -> None:
    motors = {r["slot_id"]: r for r in _q7(usage_rows) if r["supply_energy_wh"] is not None}
    cycled = {slot for slot, r in motors.items() if r["thermal_cycles"]}
    assert len(cycled) == 12
    assert {"intake-extension", "drive-fl", "steer-fr"} <= cycled
    assert all(r["thermal_cycles"] is not None for r in motors.values())
    sensors = [r for r in _q7(usage_rows) if r["supply_energy_wh"] is None]
    assert all(r["thermal_cycles"] is None and "no-temperature" in r["flags"] for r in sensors)


def test_e10_never_enabled(usage_rows: list[dict[str, Any]]) -> None:
    e10 = [r for r in usage_rows if r["match_key"] == "2026gacmp_e10"]
    assert len(e10) == 22
    assert all(r["enabled_s"] == 0 and r["powered_s"] > 60 for r in e10)


_DERIVE = """
import json, resource, sys, time
from pathlib import Path
from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics import derive, usage

spent = [0.0]
original = usage.session_usage

def timed(*args, **kwargs):
    started = time.perf_counter()
    try:
        return original(*args, **kwargs)
    finally:
        spent[0] += time.perf_counter() - started

usage.session_usage = timed

def own_peak():
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

deriver = derive.Deriver(LakePaths(Path(sys.argv[1])), config.config_root())
deriver.run(force=True)
deriver.close()
print(json.dumps({"usage_s": spent[0], "peak_bytes": own_peak()}))
"""


@pytest.mark.perf
def test_usage_derive_budget(corpus_lake: Any) -> None:
    """Usage adds < 3 s to deriving the corpus, and derive stays < 1 GB peak."""
    done = subprocess.run(
        [sys.executable, "-c", _DERIVE, str(corpus_lake.root)],
        capture_output=True,
        text=True,
        check=True,
    )
    result = json.loads(done.stdout.strip().splitlines()[-1])
    print(json.dumps(result))
    assert result["usage_s"] < 3.0
    assert result["peak_bytes"] < 1 << 30
