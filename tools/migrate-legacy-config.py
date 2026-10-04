"""Convert the legacy 2025 device datamaps into config/robots/2025-comp.toml.

Usage: python tools/migrate-legacy-config.py
Prints the migration report (corrections and unmapped NetworkTables rows).
"""

import sys
from pathlib import Path

from flashpoint.semantics.legacy_migration import migrate_device_maps

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    toml_text, report = migrate_device_maps(
        robot="2025-comp",
        season=2025,
        project="Robot-2025",
        rio_map=ROOT / "datamaps" / "rio_devices_map.csv",
        canivore_map=ROOT / "datamaps" / "drivetrain_devices_map.csv",
        nt_maps=[
            ROOT / "datamaps" / "2025" / "metrics_map.csv",
            ROOT / "datamaps" / "2025" / "vision_map.csv",
        ],
    )
    target = ROOT / "config" / "robots" / "2025-comp.toml"
    target.write_text(toml_text)
    for line in report:
        print(line)
    print(f"wrote {target} ({len(report)} report lines)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
