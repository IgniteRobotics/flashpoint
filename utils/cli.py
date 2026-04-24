from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from . import trimmer, analyzer, reporter, hoot_loader, site_builder
from . import names as _names


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m utils",
        description="Generate FRC motor power analysis PDF from hoot telemetry logs.",
    )
    parser.add_argument(
        "files", nargs="+", type=Path, metavar="FILE",
        help="Telemetry files (.hoot; one or more; multiple matches are supported)",
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=Path("report.pdf"),
        help="Output PDF path (default: report.pdf)",
    )
    parser.add_argument(
        "--site", type=Path, default=None, metavar="DIR",
        help="Output site directory (accumulates matches; creates dir if needed). Suppresses PDF output.",
    )
    parser.add_argument(
        "--per-motor-graphs", action="store_true",
        help="Include a per-motor graph page for each motor",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.5,
        help="Voltage threshold (V) for match start/end detection (default: 0.5)",
    )
    parser.add_argument(
        "--motor-names", type=Path, default=None, metavar="PATH",
        help="TOML config mapping TalonFX IDs to display names (optional)",
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=Path("converted_data/power_cache"),
        metavar="DIR",
        help="Directory for cached hoot conversions (default: converted_data/power_cache/)",
    )
    args = parser.parse_args()

    missing = [f for f in args.files if not f.exists()]
    if missing:
        for f in missing:
            print(f"error: file not found: {f}", file=sys.stderr)
        sys.exit(1)

    non_hoot = [f for f in args.files if f.suffix != ".hoot"]
    if non_hoot:
        for f in non_hoot:
            print(f"error: expected .hoot file, got: {f.name}", file=sys.stderr)
        sys.exit(1)

    match_groups = hoot_loader.find_matches(args.files)
    matches = []

    for match_id, files in sorted(match_groups.items()):
        print(f"Loading {match_id} ({len(files)} file(s))...")

        dfs: list[pd.DataFrame] = []
        for f in files:
            cached = hoot_loader.convert_hoot(f, args.cache_dir)
            if cached:
                dfs.append(pd.read_csv(cached, na_values=["null"]))
            else:
                print(f"  warning: skipping {f.name} (conversion produced no usable data)", file=sys.stderr)

        if not dfs:
            print(f"  No usable files for {match_id}, skipping.", file=sys.stderr)
            continue

        df = hoot_loader.merge_dataframes(dfs)
        df = trimmer.trim_to_match(df, voltage_threshold=args.threshold)
        df = analyzer.normalize(df)
        match = analyzer.build_match(match_id, df)
        matches.append(match)
        print(f"  {len(match.motors)} motors · {match.timestamps[-1]:.1f}s match duration")

    if not matches:
        print("error: no usable matches found in provided files", file=sys.stderr)
        sys.exit(1)

    names_config = _names.load(args.motor_names) if args.motor_names else None

    build_pdf = args.site is None
    if build_pdf:
        print(f"Building report → {args.output}")
        reporter.build_report(matches, args.output, per_motor=args.per_motor_graphs, names_config=names_config)
        print("Done.")

    if args.site is not None:
        for match in matches:
            mn = _names.resolve(match.match_id, names_config) if names_config else None
            match_dict = site_builder.serialize_match(match, mn)
            site_builder.write_match(match_dict, args.site)
        site_builder.update_manifest(args.site)
        site_builder.ensure_index(args.site)
        print(f"Site updated → {args.site / 'index.html'}")


if __name__ == "__main__":
    main()
