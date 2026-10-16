"""Convert the legacy 2025 datamaps and log configuration into Flashpoint configuration.

Usage: python tools/migrate-legacy-config.py --reference-log <2025 wpilog>
Reads the legacy CSVs and log_configs/config2025.json from the `legacy-2025` git tag (the
device maps were removed from the tree in retire-legacy-code stage 2b), so the migration stays
reproducible. Writes config/robots/2025-comp.toml (slots, signals, and the migration report as
its head comment) and config/seasons/2025.toml (NetworkTables roots). NetworkTables rows are
checked for presence and type against the reference wpilog (corpus `2025-gadal-q30`).
"""

import argparse
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

from flashpoint.semantics.legacy_migration import (
    migrate_device_maps,
    migrate_nt_maps,
    reference_entries,
)
from flashpoint.semantics.robot_config import RobotConfig

ROOT = Path(__file__).resolve().parent.parent
LEGACY_TAG = "legacy-2025"
LEGACY_FILES = {
    "rio": "datamaps/rio_devices_map.csv",
    "canivore": "datamaps/drivetrain_devices_map.csv",
    "metrics": "datamaps/2025/metrics_map.csv",
    "vision": "datamaps/2025/vision_map.csv",
    "log_config": "log_configs/config2025.json",
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reference-log", type=Path, required=True, help="a 2025 .wpilog")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        paths = {}
        for key, path in LEGACY_FILES.items():
            content = subprocess.run(
                ["git", "show", f"{LEGACY_TAG}:{path}"], cwd=ROOT, check=True, capture_output=True
            ).stdout
            paths[key] = Path(tmp) / Path(path).name
            paths[key].write_bytes(content)
        slots_text, report = migrate_device_maps(
            robot="2025-comp",
            season=2025,
            project="Robot-2025",
            rio_map=paths["rio"],
            canivore_map=paths["canivore"],
            nt_maps=[],
        )
        slots = RobotConfig.model_validate(tomllib.loads(slots_text)).slots
        season_text, signals_text, nt_report = migrate_nt_maps(
            2025,
            paths["metrics"],
            paths["vision"],
            paths["log_config"],
            reference_entries(args.reference_log),
            slots,
        )
    report += nt_report
    head = [
        f"# Reference log: {args.reference_log.name} ({signals_text.count('[[signal]]')} signals).",
        "# Migration report (legacy rows and prefixes not mapped, with the reason):",
        *(f"#   {line}" for line in report),
    ]
    robot = ROOT / "config" / "robots" / "2025-comp.toml"
    first, _, rest = slots_text.partition("\n")
    robot.write_text("\n".join([first, *head, rest]) + signals_text)
    season = ROOT / "config" / "seasons" / "2025.toml"
    season.parent.mkdir(exist_ok=True)
    season.write_text(season_text)
    for line in report:
        print(line)
    print(f"wrote {robot} and {season} ({len(report)} report lines)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
