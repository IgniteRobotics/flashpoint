"""`flashpoint` command line: ingest, rebuild, doctor."""

import argparse
import platform
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import flashpoint
from flashpoint import config
from flashpoint.ingest import Ingestor, IngestReport
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.readers import hoot
from flashpoint.readers.hoot import HootError
from flashpoint.semantics.derive import Deriver
from flashpoint.semantics.robot_config import ConfigError, load_robots

EXIT_USAGE = 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flashpoint", description=__doc__)
    parser.add_argument(
        "-V", "--version", action="version", version=f"flashpoint {flashpoint.__version__}"
    )
    sub = parser.add_subparsers(dest="command")

    def lake_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--lake", type=Path, help="lake root (default: $FLASHPOINT_LAKE or ~/flashpoint-lake)"
        )

    ingest = sub.add_parser("ingest", help="ingest .wpilog/.hoot files or directories")
    ingest.add_argument("paths", nargs="+", type=Path)
    ingest.add_argument("--profile", choices=hoot.PROFILES, default="health")
    ingest.add_argument("--jobs", type=int, help="parallel hoot conversions")
    lake_arg(ingest)

    ingest.add_argument("--no-derive", action="store_true", help="skip deriving silver/gold")

    rebuild = sub.add_parser("rebuild", help="reprocess files from an older pipeline version")
    rebuild.add_argument("--profile", choices=hoot.PROFILES, default="health")
    lake_arg(rebuild)

    derive = sub.add_parser(
        "derive", help="sessions, alignment, identity, silver, and gold from bronze"
    )
    derive.add_argument(
        "--all", action="store_true", help="rebuild every session, not only changed ones"
    )
    lake_arg(derive)

    doctor = sub.add_parser("doctor", help="report environment and lake health")
    lake_arg(doctor)
    return parser


def _summarize(report: IngestReport) -> None:
    for result in report.results:
        if result.status == "quarantined":
            print(f"  QUARANTINED {result.path.name}: {result.reason}")
        elif result.warnings:
            print(f"  WARNING     {result.path.name}: {', '.join(result.warnings)}")
    print(
        f"{report.count('success')} ingested, {report.count('skipped')} skipped, "
        f"{report.count('quarantined')} quarantined in {report.elapsed_s:.1f}s"
    )


def _derive(lake: LakePaths, force: bool) -> int:
    try:
        deriver = Deriver(lake, config.config_root())
    except ConfigError as exc:
        print(f"robot configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    try:
        result = deriver.run(force=force)
    finally:
        deriver.close()
    print(f"derived {result['rebuilt']} of {result['sessions']} session(s)")
    return 0


def _doctor(lake: LakePaths) -> int:
    print(f"flashpoint {flashpoint.__version__} (pipeline v{config.PIPELINE_VERSION})")
    print(f"python     {platform.python_version()} on {hoot.platform_key()}")
    print(f"lake       {lake.root}{'' if lake.root.exists() else ' (not created yet)'}")
    if lake.ledger.is_file():
        ledger = Ledger(lake.ledger)
        for row in ledger.query(
            "SELECT stage, count(*) AS n FROM files GROUP BY stage ORDER BY stage"
        ):
            print(f"  {row['stage']:<12} {row['n']}")
        stale = ledger.query(
            "SELECT count(*) AS n FROM files WHERE pipeline_version IS NOT ?",
            (config.PIPELINE_VERSION,),
        )[0]["n"]
        if stale:
            print(f"  {stale} file(s) from an older pipeline version: run `flashpoint rebuild`")
        ledger.close()
    robots_dir = config.config_root() / "robots"
    print(f"robots     {robots_dir}")
    try:
        for robot in load_robots(robots_dir):
            slots = len(robot.slots)
            print(
                f"  {robot.robot:<14} season {robot.season} project {robot.project}: {slots} slots"
            )
    except ConfigError as exc:
        print(f"  INVALID: {exc}")
    registry = hoot.default_registry(config.cache_root())
    print(f"owlet cache {registry.cache_dir}")
    for compliancy in registry.compliancies():
        try:
            registry.binary_for(compliancy, download=False)
            state = "cached"
        except HootError as exc:
            state = exc.reason
        print(f"  C{compliancy:<3} owlet {registry.version_for(compliancy):<10} {state}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.command is None:
        _parser().print_help()
        return 0
    lake = LakePaths(config.lake_root(args.lake))
    if args.command == "doctor":
        return _doctor(lake)
    if args.command == "derive":
        return _derive(lake, force=args.all)
    registry = hoot.default_registry(config.cache_root())
    ingestor = Ingestor(
        lake, registry, profile=args.profile, jobs=getattr(args, "jobs", None), log=print
    )
    try:
        if args.command == "ingest":
            missing = [p for p in args.paths if not p.exists()]
            if missing:
                print(f"no such file or directory: {missing[0]}", file=sys.stderr)
                return EXIT_USAGE
            report = ingestor.ingest(args.paths)
        else:
            report = ingestor.rebuild()
    finally:
        ingestor.close()
    _summarize(report)
    if (args.command == "rebuild" or not args.no_derive) and report.count("success"):
        # A fresh process: peak memory is max(ingest, derive) rather than their sum, because
        # the allocator keeps ingest's high-water mark (measured: 1155 MB vs 870 / 589 MB).
        derive = [sys.executable, "-m", "flashpoint", "derive", "--lake", str(lake.root)]
        subprocess.run(derive + (["--all"] if args.command == "rebuild" else []), check=False)
    return report.exit_code
