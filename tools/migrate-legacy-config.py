"""Convert the legacy 2025 device datamaps into config/robots/2025-comp.toml.

Usage: python tools/migrate-legacy-config.py
Reads the legacy CSVs from the `legacy-2025` git tag (the device maps were removed from the
tree in retire-legacy-code stage 2b), so the migration stays reproducible.
Prints the migration report (corrections and unmapped NetworkTables rows).
"""

import subprocess
import sys
import tempfile
from pathlib import Path

from flashpoint.semantics.legacy_migration import migrate_device_maps

ROOT = Path(__file__).resolve().parent.parent
LEGACY_TAG = "legacy-2025"
LEGACY_FILES = {
    "rio": "datamaps/rio_devices_map.csv",
    "canivore": "datamaps/drivetrain_devices_map.csv",
    "metrics": "datamaps/2025/metrics_map.csv",
    "vision": "datamaps/2025/vision_map.csv",
}


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        paths = {}
        for key, path in LEGACY_FILES.items():
            content = subprocess.run(
                ["git", "show", f"{LEGACY_TAG}:{path}"], cwd=ROOT, check=True, capture_output=True
            ).stdout
            paths[key] = Path(tmp) / f"{key}.csv"
            paths[key].write_bytes(content)
        toml_text, report = migrate_device_maps(
            robot="2025-comp",
            season=2025,
            project="Robot-2025",
            rio_map=paths["rio"],
            canivore_map=paths["canivore"],
            nt_maps=[paths["metrics"], paths["vision"]],
        )
    target = ROOT / "config" / "robots" / "2025-comp.toml"
    target.write_text(toml_text)
    for line in report:
        print(line)
    print(f"wrote {target} ({len(report)} report lines)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
