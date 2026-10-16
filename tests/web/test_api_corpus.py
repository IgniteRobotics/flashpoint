"""The served API and raw downloads on the golden corpus (spec: lifetime-trends, match-reports)."""

import hashlib
import json
import subprocess
import sys
from typing import Any

import duckdb
import pytest

from flashpoint import config
from flashpoint.lake.ledger import Ledger
from flashpoint.report.build import ReportBuilder
from flashpoint.report.envelope import round_sig
from flashpoint.report.paths import data_dir
from flashpoint.semantics.robot_config import load_robots

pytestmark = pytest.mark.corpus


@pytest.fixture(scope="module")
def built(derived_corpus_lake: Any) -> Any:
    builder = ReportBuilder(derived_corpus_lake, config.config_root())
    try:
        builder.build()
    finally:
        builder.close()
    return derived_corpus_lake


def test_history_answers_for_q7_and_e10(serve: Any, built: Any) -> None:
    app = serve(built, robots=load_robots(config.config_root() / "robots"))
    status, trend = app.json("/api/trend?metric=peak_temp&event=2026gacmp")
    assert status == 200
    assert {p["match_key"] for p in trend["points"]} == {"2026gacmp_qm7"}  # E10 has no gold
    assert len({p["series"] for p in trend["points"]}) == 17
    status, devices = app.json("/api/devices?metric=stall_s&subsystem=intake")
    assert status == 200 and {r["slot_id"] for r in devices["rows"]} >= {"intake-extension"}
    status, unit = app.json("/api/unit/legacy:2026-comp:drive-fl:0")
    assert status == 200 and unit["found"]
    assert unit["totals"]["sessions"] == 2  # Q7 and E10 both have usage rows
    assert unit["sessions_without_aligned_samples"] >= 1  # the E10 restart and P2 have none
    for key in ("2026gacmp_qm7", "2026gacmp_e10"):
        status, match = app.json(f"/api/match/{key}")
        assert status == 200 and match["built"] is True and match["sources"]


def test_raw_downloads_equal_ledger_hashes(serve: Any, built: Any) -> None:
    app = serve(built)
    ledger = Ledger(built.ledger)
    try:
        known = {r["sha256"] for r in ledger.query("SELECT sha256 FROM files")}
    finally:
        ledger.close()
    status, match = app.json("/api/match/2026gacmp_qm7")
    assert status == 200
    parts = {s["part"] for s in match["sources"]}
    assert parts == {"wpilog", "rio", "6E9415C3394C485320202050101C18FF"}
    for source in match["sources"]:
        reply = app.get(f"/raw/{source['sha256']}/{source['download']}")
        assert reply.status == 200
        assert "2026gacmp_qm7" in reply.headers["Content-Disposition"]
        digest = hashlib.sha256(reply.body).hexdigest()
        assert digest == source["sha256"] and digest in known


_WINDOW = """
import json, resource, sys, time
from pathlib import Path
from flashpoint.lake.paths import LakePaths
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.api import Api

def own_peak():
    if sys.platform.startswith("linux"):
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

api = Api(HistoryQueries(LakePaths(Path(sys.argv[1]))))
started = time.perf_counter()
status, body = api.handle("envelope/2026gacmp_qm7", {"from": ["96"], "to": ["98"]})
elapsed = time.perf_counter() - started
print(json.dumps({"status": status, "available": body.get("available"),
                  "width": body["window"]["width"], "seconds": elapsed, "peak_bytes": own_peak()}))
"""


def test_q7_window_budget(built: Any) -> None:
    done = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _WINDOW, str(built.root)], capture_output=True, text=True, check=True
    )
    result = json.loads(done.stdout.strip().splitlines()[-1])
    print(json.dumps(result))
    assert result["status"] == 200 and result["available"] is True and result["width"] == 0.01
    assert result["seconds"] < 1.0
    assert result["peak_bytes"] < 500 * 2**20


def test_q7_window_matches_silver(serve: Any, built: Any) -> None:
    status, body = serve(built).json("/api/envelope/2026gacmp_qm7?from=96&to=98")
    assert status == 200 and body["available"] is True
    text = (data_dir(built) / "2026gacmp_qm7.js").read_text(encoding="utf-8")
    stored = json.loads(text[len("FP.register(") : -3])
    match_start = stored["t0_us"] - round(stored["window"]["t0"] * 1_000_000)
    window = body["window"]
    t0, width = match_start + round(window["t0"] * 1_000_000), round(window["width"] * 1_000_000)
    silver = built.silver / "season=2026" / f"session_id={stored['session_id']}"
    rows = duckdb.execute(
        "SELECT slot_id, metric, (t_us - ?) // ? AS b, min(value), max(value)"
        " FROM read_parquet(?) WHERE t_us >= ? AND t_us < ? AND metric IN"
        " ('supply_current', 'stator_current', 'rotor_velocity_rps', 'motor_voltage')"
        " GROUP BY ALL",
        [t0, width, str(silver / "*.parquet"), t0, t0 + window["n"] * width],
    ).fetchall()
    assert len(rows) > 1_000
    for slot, metric, bucket, lo, hi in rows:
        env = body["series"][slot][metric]
        assert env["min"][bucket] == round_sig([lo])[0], (slot, metric, bucket)
        assert env["max"][bucket] == round_sig([hi])[0], (slot, metric, bucket)
    filled = sum(
        v is not None for s in body["series"].values() for e in s.values() for v in e["max"]
    )
    assert filled == len(rows)
