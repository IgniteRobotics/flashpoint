from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import loader, trimmer, analyzer, reporter


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m utils",
        description="Generate FRC motor power analysis PDF from telemetry CSVs.",
    )
    parser.add_argument(
        "files", nargs="+", type=Path, metavar="FILE",
        help="CSV telemetry files (one or more; multiple matches are supported)",
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=Path("report.pdf"),
        help="Output PDF path (default: report.pdf)",
    )
    parser.add_argument(
        "--per-motor-graphs", action="store_true",
        help="Include a per-motor graph page for each motor",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.5,
        help="Voltage threshold (V) for match start/end detection (default: 0.5)",
    )
    args = parser.parse_args()

    missing = [f for f in args.files if not f.exists()]
    if missing:
        for f in missing:
            print(f"error: file not found: {f}", file=sys.stderr)
        sys.exit(1)

    match_groups = loader.find_matches(args.files)
    matches = []

    for match_id, files in sorted(match_groups.items()):
        print(f"Loading {match_id} ({len(files)} file(s))...")
        df = loader.load_match(files)
        df = trimmer.trim_to_match(df, voltage_threshold=args.threshold)
        df = analyzer.normalize(df)
        match = analyzer.build_match(match_id, df)
        matches.append(match)
        print(f"  {len(match.motors)} motors · {match.timestamps[-1]:.1f}s match duration")

    print(f"Building report → {args.output}")
    reporter.build_report(matches, args.output, per_motor=args.per_motor_graphs)
    print("Done.")


if __name__ == "__main__":
    main()
