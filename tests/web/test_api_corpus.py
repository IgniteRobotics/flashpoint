"""The served API and raw downloads on the golden corpus (spec: lifetime-trends, match-reports)."""

import hashlib
from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.ledger import Ledger
from flashpoint.report.build import ReportBuilder
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
