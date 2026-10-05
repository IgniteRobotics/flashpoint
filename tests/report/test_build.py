import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from flashpoint import config
from flashpoint.report.build import ReportBuilder, match_label, to_script
from flashpoint.report.names import download_name
from flashpoint.report.paths import data_dir, state_path
from tests.report.lake import COMP_SLOTS, MatchLake, quiet_rows
from tests.report.silver import S, series


def _load(path: Path, call: str = "FP.register") -> Any:
    text = path.read_text(encoding="utf-8")
    assert text.startswith(f"{call}(") and text.endswith(");\n")
    return json.loads(text[len(call) + 1 : -3])


def _build(lake: MatchLake, **kwargs: Any) -> Any:
    builder = ReportBuilder(lake.write_meta(), config.config_root(), **kwargs)
    try:
        return builder.build()
    finally:
        builder.close()


@pytest.fixture
def q7(tmp_path: Path) -> MatchLake:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows())
    gold = lake.root / "gold/match_features/season=2026/session_id=q7"
    gold.mkdir(parents=True)
    pl.DataFrame(
        [
            {"match_key": "2026gacmp_qm7", "session_id": "q7", "phase": phase,
             "slot_id": slot, "unit_id": f"ctre:{slot.upper()}", "temp_max_c": 41.0,
             "supply_energy_wh": 1.234567, "stall_s": 0.0, "supply_current_p95": 12.0}
            for phase in ("auto", "teleop", "match") for slot in COMP_SLOTS
        ]
    ).write_parquet(gold / "part-0.parquet")  # fmt: skip
    return lake


def test_labels_and_names() -> None:
    assert match_label("2026gacmp_qm7") == "Q7"
    assert match_label("2026gacmp_e10") == "E10"
    assert match_label("2026gacmp_pm2") == "P2"
    assert match_label("2026gacmp_qm7r2") == "Q7r2"
    assert download_name("2026gacmp_qm7", "rio", "x');alert(1);//.wpilog") == (
        "2026gacmp_qm7__rio__x_alert_1_.wpilog"
    )


def test_script_escaping() -> None:
    text = to_script("FP.register", {"name": "</script><script>alert(1)</script>  "})
    assert "</" not in text and " " not in text and " " not in text
    assert "<\\/script>" in text and "\\u2028" in text and "\\u2029" in text
    assert json.loads(text[len("FP.register(") : -3])["name"].startswith("</script>")


def test_payload_shape(q7: MatchLake) -> None:
    summary = _build(q7)
    assert summary.built == ["2026gacmp_qm7"]
    data = _load(data_dir(q7.lake) / "2026gacmp_qm7.js")
    assert data["match_key"] == "2026gacmp_qm7" and data["label"] == "Q7"
    assert (data["event"], data["robot"], data["status"]) == ("2026gacmp", "2026-comp", "ok")
    assert data["start_utc"] == "2026-04-09T21:50:34+00:00"
    assert data["duration_s"] == 165.0 and data["framed"] is True
    assert (data["alignment"], data["alignment_method"], data["low_alignment"]) == (
        "high", "payload-match", False,
    )  # fmt: skip
    assert data["window"]["t0"] == -2.0 and data["window"]["width"] == 0.17
    assert data["window"]["n"] * data["window"]["width"] >= 169
    assert [p["name"] for p in data["phases"]] == ["pre", "auto", "gap", "teleop", "post"]
    assert data["phases"][1] == {"name": "auto", "start": 0.0, "end": 15.0}
    slots = {s["id"]: s for s in data["slots"]}
    assert set(slots) == set(COMP_SLOTS)
    assert slots["hood"]["role"] == "hood" and slots["hood"]["units"] == ["ctre:HOOD"]
    assert slots["hood"]["features"]["match"]["supply_energy_wh"] == 1.23
    assert set(data["series"]["hood"]) == {
        "supply_current", "stator_current", "rotor_velocity_rps", "motor_voltage",
    }  # fmt: skip
    env = data["series"]["hood"]["supply_current"]
    assert set(env) == {"min", "max", "mean"} and len(env["max"]) == data["window"]["n"]
    assert set(data["battery"]) == {"min", "max", "mean"}
    assert data["temps"]["intake-roller"] == [[-2.0, 27.0], [90.0, 45.0]]
    assert data["maxima"]["intake-roller"] == {
        "temp_max_c": 45.0, "supply_current_max": 12.0, "stator_current_max": 22.0,
    }  # fmt: skip
    assert data["not_logged"] == {slot: [] for slot in COMP_SLOTS}
    assert data["markers"] == [] and data["temperature"] == {"available": True, "reason": None}
    assert {s["part"] for s in data["sources"]} == {
        "wpilog", "rio", "6E9415C3394C485320202050101C18FF",
    }  # fmt: skip
    assert all(s["download"].startswith("2026gacmp_qm7__") for s in data["sources"])
    size = (data_dir(q7.lake) / "2026gacmp_qm7.js").stat().st_size
    assert data["bytes"] <= size < data["bytes"] + 32  # measured before the field was added


def test_index_and_state(q7: MatchLake) -> None:
    q7.add_match(None, "practice", quiet_rows())
    _build(q7)
    index = _load(data_dir(q7.lake) / "matches.js", "FP.index")
    assert index["sessions_without_match_key"] == 1
    assert index["lake"] == str(q7.root)
    assert [m["key"] for m in index["matches"]] == ["2026gacmp_qm7"]
    entry = index["matches"][0]
    assert entry["markers"] == {"WARN": 0, "FAULT": 0}
    assert entry["file"] == "data/2026gacmp_qm7.js" and entry["status"] == "ok"
    state = json.loads(state_path(q7.lake).read_text())
    assert set(state["matches"]) == {"2026gacmp_qm7"}


def test_not_logged_and_no_temperature(tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    rows = [r for r in quiet_rows() if r["metric"] != "temp_c"]
    rows = [r for r in rows if not (r["slot_id"] == "hood" and r["metric"] == "motor_voltage")]
    lake.add_match("2026gacmp_qm8", "q8", rows, pro_licensed=0)
    _build(lake)
    data = _load(data_dir(lake.lake) / "2026gacmp_qm8.js")
    assert data["temperature"]["available"] is False
    assert "not Pro-licensed" in data["temperature"]["reason"]
    assert data["not_logged"]["hood"] == ["motor_voltage", "temp_c"]
    assert data["not_logged"]["drive-fl"] == ["temp_c"]


def test_markers_embedded_and_counted(tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    rows = quiet_rows()
    rows += [{"slot_id": "hood", "unit_id": "ctre:HOOD", "metric": "temp_c",
              "t_us": 118 * S, "value": 66.0}]  # fmt: skip
    lake.add_match("2026gacmp_qm9", "q9", rows)
    _build(lake)
    data = _load(data_dir(lake.lake) / "2026gacmp_qm9.js")
    assert [(m["level"], m["source"], m["t"]) for m in data["markers"]] == [("WARN", "hood", 108.0)]
    index = _load(data_dir(lake.lake) / "matches.js", "FP.index")
    assert index["matches"][0]["markers"] == {"WARN": 1, "FAULT": 0}


def test_budget_lowers_buckets_but_keeps_extremes(tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    rows = quiet_rows(hz=100)
    for row in rows:
        if row["slot_id"] == "drive-fl" and row["metric"] == "supply_current":
            if 90 * S <= row["t_us"] < 90.004 * S:
                row["value"] = 150.0
            elif 120 * S <= row["t_us"] < 120.01 * S:
                row["value"] = -3.0
    lake.add_match("2026gacmp_qm10", "q10", rows)
    full = _build(lake)
    assert full.reduced == {}
    big = (data_dir(lake.lake) / "2026gacmp_qm10.js").stat().st_size
    builder = ReportBuilder(lake.lake, config.config_root(), budget_bytes=big // 3)
    try:
        summary = builder.build(force=True)
    finally:
        builder.close()
    assert 100 <= summary.reduced["2026gacmp_qm10"] < 1000
    data = _load(data_dir(lake.lake) / "2026gacmp_qm10.js")
    assert data["bytes"] <= big // 3
    env = data["series"]["drive-fl"]["supply_current"]
    assert max(v for v in env["max"] if v is not None) == 150.0
    assert min(v for v in env["min"] if v is not None) == -3.0


def test_incremental_rebuild(q7: MatchLake) -> None:
    first = _build(q7)
    q7_file = data_dir(q7.lake) / "2026gacmp_qm7.js"
    before = q7_file.read_bytes()
    q7.add_match("2026gacmp_qm8", "q8", quiet_rows())
    second = _build(q7)
    assert first.built == ["2026gacmp_qm7"]
    assert second.built == ["2026gacmp_qm8"] and second.unchanged == ["2026gacmp_qm7"]
    assert q7_file.read_bytes() == before
    q7.set_fingerprint("q7", "fp-2")
    third = _build(q7)
    assert third.built == ["2026gacmp_qm7"] and third.unchanged == ["2026gacmp_qm8"]


def test_index_rewrite_does_not_read_data_files(
    q7: MatchLake, monkeypatch: pytest.MonkeyPatch
) -> None:
    _build(q7)
    original = Path.read_text

    def guarded(self: Path, *args: Any, **kwargs: Any) -> str:
        assert self.suffix != ".js", f"index rewrite read {self}"
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded)
    summary = _build(q7)
    assert summary.unchanged == ["2026gacmp_qm7"] and summary.built == []


def test_select_event_and_keys(tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows())
    lake.add_match("2026gadal_qm1", "d1", quiet_rows())
    lake.add_match("2026gadal_qm2", "d2", quiet_rows())
    builder = ReportBuilder(lake.write_meta(), config.config_root())
    try:
        assert builder.build(events=["2026gadal"]).built == ["2026gadal_qm1", "2026gadal_qm2"]
        assert builder.build(match_keys=["2026gacmp_qm7"]).built == ["2026gacmp_qm7"]
        lake.set_fingerprint("d1", "fp-2")
        lake.set_fingerprint("q7", "fp-2")
        builder2 = ReportBuilder(lake.write_meta(), config.config_root())
        assert builder2.build(match_keys=["2026gadal_qm1"]).built == ["2026gadal_qm1"]
        builder2.close()
    finally:
        builder.close()
    index = _load(data_dir(lake.lake) / "matches.js", "FP.index")
    assert [m["key"] for m in index["matches"]] == [
        "2026gacmp_qm7", "2026gadal_qm1", "2026gadal_qm2",
    ]  # fmt: skip


def test_hoot_only_session_listed_with_reason(tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_e10", "hoot-2026-04-11_19-29-15-c027", None, with_wpilog=False,
                   phases=[("pre", None, None)])  # fmt: skip
    _build(lake)
    index = _load(data_dir(lake.lake) / "matches.js", "FP.index")
    entry = index["matches"][0]
    assert entry["status"] == "no-aligned-samples" and "No aligned samples" in entry["reason"]
    data = _load(data_dir(lake.lake) / "2026gacmp_e10.js")
    assert len(data["sources"]) == 2 and "series" not in data


def test_match_key_with_unaligned_second_session(tmp_path: Path) -> None:
    """E10: a wpilog session with silver plus a hoot-only restart under the same key."""
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_e10", "e10", quiet_rows(), phases=[("pre", None, None)])
    lake.add_match("2026gacmp_e10", "hoot-e10", None, with_wpilog=False, phases=[])
    _build(lake)
    data = _load(data_dir(lake.lake) / "2026gacmp_e10.js")
    assert data["status"] == "ok" and data["framed"] is False
    assert data["window"]["t0"] == -2.0 and data["duration_s"] == pytest.approx(180, abs=0.1)
    assert any("never enabled" in note for note in data["notes"])
    assert any("1 more session(s)" in note for note in data["notes"])
    assert len(data["sources"]) == 5  # wpilog + 2 hoots, and the restart's 2 hoots


def test_low_alignment_marked(tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows(), confidence="low")
    _build(lake)
    entry = _load(data_dir(lake.lake) / "matches.js", "FP.index")["matches"][0]
    assert entry["low_alignment"] is True and entry["alignment"] == "low"


def test_slow_slots_noted(tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    rows = [
        r for r in quiet_rows() if not (r["slot_id"] == "hood" and r["metric"] == "stator_current")
    ]
    rows += series("hood", "stator_current", 0, 180, 4, 20.0)
    lake.add_match("2026gacmp_qm7", "q7", rows)
    _build(lake)
    data = _load(data_dir(lake.lake) / "2026gacmp_qm7.js")
    assert data["rates"]["hood"] == pytest.approx(4.0, abs=0.1)
    assert any("(hood)" in note and "slower than 20 Hz" in note for note in data["notes"])


def test_hostile_names_are_data(tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows(), wpilog_name="x');alert(1);//.wpilog")
    _build(lake)
    data = _load(data_dir(lake.lake) / "2026gacmp_qm7.js")
    wpilog = next(s for s in data["sources"] if s["part"] == "wpilog")
    assert wpilog["name"] == "x');alert(1);//.wpilog"
    assert wpilog["download"] == "2026gacmp_qm7__wpilog__x_alert_1_.wpilog"


def test_removed_match_dropped_on_full_build(q7: MatchLake) -> None:
    _build(q7)
    q7.tables["sessions"] = []
    summary = _build(q7)
    assert summary.removed == ["2026gacmp_qm7"]
    assert not (data_dir(q7.lake) / "2026gacmp_qm7.js").exists()
    assert _load(data_dir(q7.lake) / "matches.js", "FP.index")["matches"] == []
