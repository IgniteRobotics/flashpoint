from typing import Any

import pytest

from flashpoint import config
from flashpoint.semantics.derive import Deriver

pytestmark = pytest.mark.corpus


@pytest.fixture(scope="module")
def derived(corpus_lake: Any) -> dict[str, Any]:
    deriver = Deriver(corpus_lake, config.config_root())
    try:
        deriver.derive_sessions()
        sessions = {r["session_id"]: r for r in deriver.ledger.query("SELECT * FROM sessions")}
        hoots = deriver.ledger.query(
            "SELECT sh.*, l.filename FROM session_hoots sh JOIN logs l USING (log_id)"
        )
        logs = {r["log_id"]: r for r in deriver.ledger.query("SELECT log_id, filename FROM logs")}
    finally:
        deriver.close()
    return {"sessions": sessions, "hoots": hoots, "logs": logs}


def _session_for(derived: dict[str, Any], wpilog_name: str) -> dict[str, Any]:
    return next(
        s for s in derived["sessions"].values()
        if s["wpilog_id"] and derived["logs"][s["wpilog_id"]]["filename"] == wpilog_name
    )  # fmt: skip


def test_q7_session_identity_and_robot(derived: dict[str, Any]) -> None:
    q7 = _session_for(derived, "FRC_20260409_215113_GACMP_Q7.wpilog")
    assert (q7["match_key"], q7["match_source"], q7["robot"]) == (
        "2026gacmp_qm7",
        "fms",
        "2026-comp",
    )


def test_q7_aligned_by_payload_match(derived: dict[str, Any]) -> None:
    q7 = _session_for(derived, "FRC_20260409_215113_GACMP_Q7.wpilog")
    hoots = [h for h in derived["hoots"] if h["session_id"] == q7["session_id"]]
    assert len(hoots) == 2
    for h in hoots:
        assert h["method"] == "payload-match"
        assert h["confidence"] == "high"
        assert abs(h["offset_us"] - 19_563_500) < 10_000
        assert h["spread_us"] < 5_000
        assert h["bus_agreement_us"] is not None and h["bus_agreement_us"] < 1_000


def test_e10_restart_after_wpilog_end_is_separate(derived: dict[str, Any]) -> None:
    e10 = _session_for(derived, "FRC_20260411_192626_GACMP_E10.wpilog")
    assert e10["match_key"] == "2026gacmp_e10"
    in_session = [h for h in derived["hoots"] if h["session_id"] == e10["session_id"]]
    assert {h["hoot_group"] for h in in_session} == {"2026-04-11_19-26-19"}
    assert {h["method"] for h in in_session} == {"payload-match"}
    restart = [h for h in derived["hoots"] if h["hoot_group"] == "2026-04-11_19-29-15"]
    assert restart and all(h["session_id"].startswith("hoot-") for h in restart)
    restart_session = derived["sessions"][restart[0]["session_id"]]
    assert restart_session["match_key"] == "2026gacmp_e10"  # from the hoot filename


def test_practice_and_2025_sessions(derived: dict[str, Any]) -> None:
    keys = {s["match_key"] for s in derived["sessions"].values()}
    assert {"2026gacmp_pm2", "2025gadal_qm30"} <= keys
