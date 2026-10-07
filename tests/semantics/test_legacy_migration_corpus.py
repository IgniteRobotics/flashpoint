import csv
import json
import subprocess
import tomllib
from collections.abc import Callable
from pathlib import Path

import pytest

from flashpoint.semantics.legacy_migration import (
    migrate_device_maps,
    migrate_nt_maps,
    reference_entries,
)
from flashpoint.semantics.robot_config import RobotConfig

pytestmark = pytest.mark.corpus

REPO = Path(__file__).resolve().parents[2]
LEGACY_TAG = "legacy-2025"
LEGACY = {
    "rio": "datamaps/rio_devices_map.csv",
    "canivore": "datamaps/drivetrain_devices_map.csv",
    "metrics": "datamaps/2025/metrics_map.csv",
    "vision": "datamaps/2025/vision_map.csv",
    "log_config": "log_configs/config2025.json",
}


@pytest.fixture(scope="module")
def migrated(
    corpus_group: Callable[[str], list[Path]], tmp_path_factory: pytest.TempPathFactory
) -> dict[str, object]:
    tmp = tmp_path_factory.mktemp("legacy")
    paths = {}
    for key, path in LEGACY.items():
        content = subprocess.run(
            ["git", "show", f"{LEGACY_TAG}:{path}"], cwd=REPO, check=True, capture_output=True
        ).stdout
        paths[key] = tmp / Path(path).name
        paths[key].write_bytes(content)
    slots_text, _ = migrate_device_maps(
        "2025-comp", 2025, "Robot-2025", paths["rio"], paths["canivore"], []
    )
    slots = RobotConfig.model_validate(tomllib.loads(slots_text)).slots
    (q30,) = corpus_group("2025-gadal-q30")
    season, signals, report = migrate_nt_maps(
        2025, paths["metrics"], paths["vision"], paths["log_config"], reference_entries(q30), slots
    )
    return {"paths": paths, "season": season, "signals": signals, "report": report}


def _rows(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return [row["entry"] for row in csv.DictReader(f)]


def test_every_legacy_row_and_prefix_is_accounted_for(migrated: dict[str, object]) -> None:
    paths: dict[str, Path] = migrated["paths"]  # type: ignore[assignment]
    report: list[str] = migrated["report"]  # type: ignore[assignment]
    signals = tomllib.loads(migrated["signals"])["signal"]  # type: ignore[arg-type]
    roots = tomllib.loads(migrated["season"])["roots"]  # type: ignore[arg-type]
    mapped = {(s["root"], s["entry"]) for s in signals}
    for key, root in (("metrics", "robot"), ("vision", "photonvision")):
        for entry in _rows(paths[key]):
            reported = any(f": {entry} (" in line for line in report)
            assert ((root, entry) in mapped) != reported, entry
    for key, prefix in json.loads(paths["log_config"].read_text()).items():
        assert prefix in roots.values() or any(f"{key} = {prefix} (" in r for r in report), key

    by_root = {r: sum(1 for s in signals if s["root"] == r) for r in ("robot", "photonvision")}
    assert by_root == {"robot": 6, "photonvision": 33}
    assert sum("superseded by CAN slot" in line for line in report) == 18
    assert sum("non-numeric in reference log" in line for line in report) == 4  # 3 poses + MetaData


def test_committed_configuration_is_the_migration_output(migrated: dict[str, object]) -> None:
    assert (REPO / "config" / "seasons" / "2025.toml").read_text() == migrated["season"]
    robot = (REPO / "config" / "robots" / "2025-comp.toml").read_text()
    assert robot.endswith(migrated["signals"])  # type: ignore[arg-type]
    for line in migrated["report"]:  # type: ignore[attr-defined]
        assert f"#   {line}\n" in robot
