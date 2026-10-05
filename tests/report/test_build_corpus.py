"""Replay data on the golden corpus (spec: match-reports)."""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

from flashpoint import config
from flashpoint.report.build import BUDGET_BYTES, ReportBuilder, data_dir
from flashpoint.report.envelope import round_sig

pytestmark = pytest.mark.corpus


@pytest.fixture(scope="module")
def built(derived_corpus_lake: Any) -> Any:
    builder = ReportBuilder(derived_corpus_lake, config.config_root())
    try:
        builder.build(force=True)
    finally:
        builder.close()
    return derived_corpus_lake


def _load(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    return json.loads(text[text.index("(") + 1 : -3])


def test_q7_built_and_indexed(built: Any) -> None:
    q7 = data_dir(built) / "2026gacmp_qm7.js"
    assert q7.stat().st_size <= BUDGET_BYTES
    index = _load(data_dir(built) / "matches.js")
    entries = {m["key"]: m for m in index["matches"]}
    assert entries["2026gacmp_qm7"]["status"] == "ok"
    assert entries["2026gacmp_qm7"]["alignment"] == "high"
    assert entries["2025gacmp_qm19"]["status"] == "no-aligned-samples"  # hoot-only
    assert entries["2026gacmp_e10"]["status"] == "ok"
    assert index["sessions_without_match_key"] == 1


def test_q7_envelope_matches_silver(built: Any) -> None:
    data = _load(data_dir(built) / "2026gacmp_qm7.js")
    window = data["window"]
    silver = next(
        (built.root / "silver/samples/season=2026").glob(f"session_id={data['session_id']}")
    )
    # match start on the wpilog clock, from the auto phase (seconds 0)
    start_us = duckdb.execute(
        "SELECT start_us FROM read_parquet(?) WHERE session_id = ? AND phase = 'auto'",
        [str(built.meta / "match_phases.parquet"), data["session_id"]],
    ).fetchone()[0]  # type: ignore[index]
    t0 = start_us + round(window["t0"] * 1_000_000)
    width = round(window["width"] * 1_000_000)
    rows = duckdb.execute(
        "SELECT slot_id, metric, (t_us - ?) // ? AS b, min(value), max(value)"
        " FROM read_parquet(?) WHERE t_us >= ? AND t_us < ? AND metric IN"
        " ('supply_current', 'stator_current', 'rotor_velocity_rps', 'motor_voltage')"
        " GROUP BY ALL",
        [t0, width, str(silver / "*.parquet"), t0, t0 + window["n"] * width],
    ).fetchall()
    assert len(rows) > 10_000
    for slot, metric, bucket, lo, hi in rows:
        env = data["series"][slot][metric]
        assert env["min"][bucket] == round_sig([lo])[0], (slot, metric, bucket)
        assert env["max"][bucket] == round_sig([hi])[0], (slot, metric, bucket)
    filled = sum(
        v is not None for s in data["series"].values() for e in s.values() for v in e["max"]
    )
    assert filled == len(rows)


def test_q7_readout_and_markers(built: Any) -> None:
    data = _load(data_dir(built) / "2026gacmp_qm7.js")
    hottest = max(data["maxima"].items(), key=lambda kv: kv[1].get("temp_max_c", 0))
    assert hottest[0] == "intake-extension" and hottest[1]["temp_max_c"] == 46.0
    kinds = {(m["kind"], m["source"]) for m in data["markers"]}
    assert ("stall", "intake-extension") in kinds
    assert data["temperature"]["available"] is True
    assert {s["part"] for s in data["sources"]} == {
        "wpilog", "rio", "6E9415C3394C485320202050101C18FF",
    }  # fmt: skip


_BUILD = """
import json, resource, sys, time
from pathlib import Path
from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.report.build import ReportBuilder

def own_peak():
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

builder = ReportBuilder(LakePaths(Path(sys.argv[1])), config.config_root())
started = time.perf_counter()
builder.build(match_keys=["2026gacmp_qm7"], force=True)
elapsed = time.perf_counter() - started
builder.close()
print(json.dumps({"seconds": elapsed, "peak_bytes": own_peak()}))
"""


@pytest.mark.perf
def test_q7_build_budget(built: Any) -> None:
    done = subprocess.run(
        [sys.executable, "-c", _BUILD, str(built.root)], capture_output=True, text=True, check=True
    )
    result = json.loads(done.stdout.strip().splitlines()[-1])
    print(json.dumps(result))
    assert result["seconds"] < 5.0
    assert result["peak_bytes"] < 500 * 2**20
