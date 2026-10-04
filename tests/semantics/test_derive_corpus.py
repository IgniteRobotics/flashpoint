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


def test_q7_slots_are_legacy_units(corpus_lake: Any) -> None:
    deriver = Deriver(corpus_lake, config.config_root())
    try:
        deriver.derive_sessions()
        deriver.derive_identity()
        q7 = deriver.ledger.query(
            "SELECT o.slot_id, o.unit_id, o.source FROM slot_observations o"
            " JOIN sessions s USING (session_id) WHERE s.match_key = '2026gacmp_qm7'"
        )
        unmapped = deriver.ledger.query(
            "SELECT u.* FROM unmapped_devices u JOIN sessions s USING (session_id)"
            " WHERE s.match_key = '2026gacmp_qm7'"
        )
    finally:
        deriver.close()
    assert len(q7) == 22  # 17 motors + gyro + 4 steer encoders
    assert {o["source"] for o in q7} == {"legacy"}
    assert {
        "slot_id": "drive-fl",
        "unit_id": "legacy:2026-comp:drive-fl:0",
        "source": "legacy",
    } in q7
    assert unmapped == []


def test_q7_framing_from_hoot_robot_mode(corpus_lake: Any) -> None:
    deriver = Deriver(corpus_lake, config.config_root())
    try:
        deriver.derive_sessions()
        framings = deriver.derive_framing()
        q7_id = deriver.ledger.query(
            "SELECT session_id FROM sessions WHERE match_key = '2026gacmp_qm7'"
        )
    finally:
        deriver.close()
    framing = framings[q7_id[0]["session_id"]]
    assert framing.source == "hoot-robot-mode"
    names = [p.name for p in framing.phases]
    assert names == ["pre", "auto", "gap", "teleop", "post"]
    teleop = framing.phases[3]
    assert teleop.end_us is not None and abs(teleop.end_us - 291_580_000) < 50_000
    assert framing.match_start_us is not None and abs(framing.match_start_us - 127_500_000) < 50_000
