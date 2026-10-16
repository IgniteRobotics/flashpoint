"""A synthetic lake with detected anomalies for the Anomalies API, views, and badges.

25 matches of 2026-comp (2026syn_qm1..25) on six slots:
- hood: maximum temperature steps from 45 to 55 °C at qm21 (HIGH STEP)
- drive-fl: 66 °C maximum temperature in qm10 (MED SPEC)
- intake-roller: mean temperature 2 °C up from qm21 (LOW DRIFT, strict only)
- drive-fr: its unit is swapped at qm23, so ctre:DRIVE-FR-NEW builds its baseline (3 of 5)
"""

import json
import shutil
from pathlib import Path
from typing import Any

import polars as pl

from flashpoint import config
from flashpoint.anomaly import detect
from flashpoint.anomaly.episodes import anomaly_id
from flashpoint.lake.paths import LakePaths
from tests.anomaly.gold_builder import _FEATURE_SCHEMA as FEATURE_SCHEMA
from tests.anomaly.gold_builder import DEFAULTS, FEATURE_METRICS, GoldLake, steady

SLOTS = ["hood", "drive-fl", "drive-fr", "drive-bl", "drive-br", "intake-roller"]


def anomaly_config(root: Path) -> Path:
    """A copy of the repo configuration the tests may edit (e.g. the causes catalogue)."""
    target = root / "config"
    if not target.exists():
        shutil.copytree(config.config_root(), target)
    return target


def build_anomaly_lake(root: Path, matches: int = 25, slots: list[str] | None = None
                       ) -> tuple[LakePaths, Path]:  # fmt: skip
    gold = GoldLake(root / "lake", matches=matches, slots=slots or SLOTS)
    gold.set("hood", "temp_max_c", steady(45.0, 20) + steady(55.0, matches - 20, start=3))
    temps = [40.0] * matches
    temps[9] = 66.0
    gold.set("drive-fl", "temp_max_c", temps)
    gold.set("intake-roller", "temp_mean_c", [30.0] * 20 + [32.0] * (matches - 20))
    gold.swap("drive-fr", matches - 3, "ctre:DRIVE-FR-NEW")
    lake = gold.write()
    config_dir = anomaly_config(root)
    detect.detect(lake, config_dir)
    return lake, config_dir


def build_event_boundary_lake(root: Path) -> tuple[LakePaths, Path]:
    """10 matches at 2026aaa, then 6 at 2026bbb; the hood's maximum temperature steps from 45 to
    55 °C at its first 2026bbb match (a HIGH STEP tagged with the event boundary)."""
    keys = [f"2026aaa_qm{i + 1}" for i in range(10)] + [f"2026bbb_qm{i + 1}" for i in range(6)]
    gold = GoldLake(root / "lake", matches=16, slots=["hood"], keys=keys)
    gold.set("hood", "temp_max_c", steady(45.0, 10) + steady(55.0, 6))
    lake = gold.write()
    config_dir = anomaly_config(root)
    detect.detect(lake, config_dir)
    return lake, config_dir


def ids(lake: LakePaths) -> dict[str, str]:
    """step, spec, drift -> anomaly id."""
    rows = detect.read_anomalies(lake).to_dicts()
    pick = {
        "step": ("ctre:HOOD", "STEP", "temp_max_c"),
        "spec": ("ctre:DRIVE-FL", "SPEC", "temp_max_c"),
        "drift": ("ctre:INTAKE-ROLLER", "DRIFT", "temp_mean_c"),
    }
    return {
        name: next(r["id"] for r in rows if (r["subject"], r["method"], r["metric"]) == key)
        for name, key in pick.items()
    }


# --- a lake for the shared filters: two events, two robots (and a 2025-only robot) -----------

UNITS = {  # robot -> slot -> unit
    "comp": {"hood": "ctre:12345", "drive-fl": "ctre:11111"},
    "practice": {"hood": "ctre:67890", "drive-fl": "ctre:22222"},
    "2025-bot": {"hood": "ctre:25000"},
}


EVIDENCE = {  # anomaly name -> its evidence matches (the others have none)
    "step": ["2026gadal_qm1", "2026gadal_qm2", "2026gadal_qm3", "2026gacmp_qm7", "2026gacmp_qm8"],
    "med": ["2026gadal_qm1", "2026gadal_qm2"],
}


UNBUILT = ("2026gacol_qm1", "comp", "2026", "2026-04-20T15:00:00+00:00")


def _anomaly(name: str, subject: str, robot: str, severity: str, method: str,
             flagged: list[str], starts: dict[str, str]) -> dict[str, Any]:  # fmt: skip
    points = [{"match_key": k, "value": 55.0 if k in flagged else 45.0, "lo": 40.0, "hi": 50.0,
               "flagged": k in flagged} for k in EVIDENCE.get(name, [])]  # fmt: skip
    power = subject.startswith("power:")
    slot = "power" if power else next(s for s, u in UNITS[robot].items() if u == subject)
    return {
        **dict.fromkeys(detect.SCHEMA),
        "id": anomaly_id(subject, method, "temp_max_c", flagged[0]),
        "subject": "power" if power else subject, "subject_key": subject,
        "subject_kind": "power" if power else "unit", "robot": robot,
        "season": flagged[0][:4], "slot_id": slot, "role": "battery proxy" if power else slot,
        "subsystem": "power" if power else "shooter", "method": method, "metric": "temp_max_c",
        "metric_name": "Maximum temperature", "metric_unit": "°C", "severity": severity,
        "sensitivity": "normal", "onset_match_key": flagged[0], "last_match_key": flagged[-1],
        "matches_affected": len(flagged), "detected_match_key": flagged[-1],
        "actual": "55 °C", "expected": "45 °C", "title": f"{name}: hood ran hot",
        "why": f"{method}: synthetic", "confidence": "high", "baseline_n": 5,
        "flagged_keys": json.dumps(flagged), "low_alignment": False,
        "evidence": json.dumps({"points": points}), "last_start": starts[flagged[-1]],
    }  # fmt: skip


def build_filter_lake(root: Path, replay: bool = False) -> tuple[LakePaths, dict[str, str]]:
    """REPLAY_MATCHES (2025gaalb, 2026gadal, 2026gacmp; robots comp, practice, 2025-bot) as gold
    features with meta, plus anomalies written as rows (name -> id):
    - step: HIGH STEP on comp's ctre:12345, flagged at 2026gadal_qm3 and 2026gacmp_qm7; its
      evidence spans 3 matches at 2026gadal then 2 at 2026gacmp
    - drift: LOW DRIFT on ctre:12345, flagged only at 2026gadal_qm1
    - med: MED SPEC on practice's ctre:67890, flagged at 2026gadal_qm2; evidence at 2026gadal only
    - old: MED SPEC on 2025-bot's ctre:25000, flagged at 2025gaalb_qm3
    - brown1, brown2: LOW SPEC brownouts on comp's battery, at 2026gacmp_qm8 and 2026gadal_qm1
    `replay` also builds the matches for Replay (silver and the static index). 2026gacol_qm1
    (comp) is in the lake's features and meta but never built for Replay."""
    from tests.report.lake import MatchLake
    from tests.views.season import REPLAY_MATCHES, build_replay_season

    if replay:
        lake = build_replay_season(root)
    else:
        built = MatchLake(root)
        for key, robot, season, start in REPLAY_MATCHES:
            built.add_match(key, key, None, robot=robot, season=season, start_utc=start,
                            hoot_buses=())  # fmt: skip
        lake = built.write_meta()
    sessions = lake.meta / "sessions.parquet"
    pl.concat([pl.read_parquet(sessions), pl.DataFrame([{
        "session_id": UNBUILT[0], "robot": UNBUILT[1], "season": UNBUILT[2],
        "match_key": UNBUILT[0], "match_source": "fms", "kind": "match"}])],
        how="diagonal_relaxed").write_parquet(sessions)  # fmt: skip
    starts = {key: start for key, _, _, start in (*REPLAY_MATCHES, UNBUILT)}
    for key, robot, season, _ in (*REPLAY_MATCHES, UNBUILT):
        rows = [
            {"match_key": key, "session_id": key, "season": season, "robot": robot,
             "phase": "match", "slot_id": slot, "unit_id": unit, "subsystem": "shooter",
             "role": slot, "alignment": "high", "source_logs": key, "flags": "",
             **{m: DEFAULTS[m] for m in FEATURE_METRICS}}
            for slot, unit in UNITS[robot].items()
        ]  # fmt: skip
        path = lake.root / "gold" / "match_features" / f"season={season}" / f"session_id={key}"
        path.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(rows, schema=FEATURE_SCHEMA).write_parquet(path / "part-0.parquet")
    found: list[tuple[str, str, str, str, str, list[str]]] = [
        ("step", "ctre:12345", "comp", "HIGH", "STEP", ["2026gadal_qm3", "2026gacmp_qm7"]),
        ("drift", "ctre:12345", "comp", "LOW", "DRIFT", ["2026gadal_qm1"]),
        ("med", "ctre:67890", "practice", "MED", "SPEC", ["2026gadal_qm2"]),
        ("old", "ctre:25000", "2025-bot", "MED", "SPEC", ["2025gaalb_qm3"]),
        ("brown1", "power:comp", "comp", "LOW", "SPEC", ["2026gacmp_qm8"]),
        ("brown2", "power:comp", "comp", "LOW", "SPEC", ["2026gadal_qm1"]),
    ]
    rows = [_anomaly(*spec, starts=starts) for spec in found]
    out = detect.anomalies_dir(lake)
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=detect.SCHEMA).write_parquet(out / detect.ANOMALIES_FILE)
    summary = {"scanned_through": "2026gacmp_qm8", "building_baseline": {"count": 0}}
    (out / detect.SUMMARY_FILE).write_text(json.dumps(summary), encoding="utf-8")
    return lake, {spec[0]: str(row["id"]) for spec, row in zip(found, rows, strict=True)}
