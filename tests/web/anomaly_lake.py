"""A synthetic lake with detected anomalies for the Anomalies API, views, and badges.

25 matches of 2026-comp (2026syn_qm1..25) on six slots:
- hood: maximum temperature steps from 45 to 55 °C at qm21 (HIGH STEP)
- drive-fl: 66 °C maximum temperature in qm10 (MED SPEC)
- intake-roller: mean temperature 2 °C up from qm21 (LOW DRIFT, strict only)
- drive-fr: its unit is swapped at qm23, so ctre:DRIVE-FR-NEW builds its baseline (3 of 5)
"""

import shutil
from pathlib import Path

from flashpoint import config
from flashpoint.anomaly import detect
from flashpoint.lake.paths import LakePaths
from tests.anomaly.gold_builder import GoldLake, steady

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
