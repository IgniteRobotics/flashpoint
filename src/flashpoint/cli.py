"""`flashpoint` command line: init, ingest, rebuild, derive, detect, backtest, report, serve,
acquire, backup, restore, doctor."""

import argparse
import logging
import platform
import sqlite3
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import flashpoint
from flashpoint import config
from flashpoint.acquire.backup import (
    BackupError,
    RestoreRefusedError,
    config_hash,
    failed_slot,
    ok_slot,
    restore,
    run_backup,
)
from flashpoint.acquire.config import AcquireConfig, AcquireConfigError
from flashpoint.acquire.watch import (
    EXIT_LOCKED,
    LOCK_FILE,
    AcquireLock,
    LockHeldError,
    StatusFormatError,
    describe_status,
    read_status,
    record_backup,
    run_acquire,
)
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.readers import hoot

if TYPE_CHECKING:
    from flashpoint.ingest import IngestReport
    from flashpoint.report.build import ReportBuilder

# The ingest, derive and robot-config modules pull in polars, duckdb and pyarrow; they are
# imported inside the handlers that need them to keep `flashpoint acquire` small (watch RSS budget).
EXIT_FAILED = 1
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

    init = sub.add_parser(
        "init", help="create your configuration (team number, lake folder, default settings)"
    )
    init.add_argument("--team", metavar="NUMBER", help="your FRC team number (asked if omitted)")
    init.add_argument(
        "--lake", type=Path, help="lake folder for your logs (asked if omitted; ~/flashpoint-lake)"
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

    detect = sub.add_parser(
        "detect", help="find anomalies in gold (runs after every derive; skips when unchanged)"
    )
    detect.add_argument("--all", action="store_true", help="run even when nothing changed")
    lake_arg(detect)

    backtest = sub.add_parser(
        "backtest",
        help="replay detection in match order against config/failure-labels.toml",
    )
    lake_arg(backtest)

    report = sub.add_parser(
        "report", help="build per-match Replay data (incremental); --static exports a folder"
    )
    report.add_argument(
        "--event", action="append", metavar="EVENT", help="only this event, e.g. 2026gacmp"
    )
    report.add_argument(
        "--match", action="append", metavar="KEY", help="only this match key, e.g. 2026gacmp_qm7"
    )
    report.add_argument("--all", action="store_true", help="rebuild even unchanged matches")
    report.add_argument(
        "--static", type=Path, metavar="OUT", help="also export Replay to a folder for file://"
    )
    report.add_argument(
        "--no-raw", action="store_true", help="with --static: leave the raw logs out"
    )
    lake_arg(report)

    serve = sub.add_parser(
        "serve",
        help="serve Replay, History, Anomalies, Setup and Live CAN (read-only but for triage"
        " and setup)",
    )
    serve.add_argument("--host", default="127.0.0.1", help="bind address (default 127.0.0.1)")
    serve.add_argument("--port", type=int, default=8000, help="port (default 8000; 0 = any)")
    serve.add_argument(
        "--robot-host",
        action="append",
        metavar="ADDRESS",
        help="robot address for the Live CAN view (repeatable; default: the acquire hosts)",
    )
    serve.add_argument(
        "--no-build", action="store_true", help="skip the incremental report build on start"
    )
    lake_arg(serve)

    acquire = sub.add_parser(
        "acquire", help="pull logs from the robot and USB sticks into the lake, then ingest"
    )
    acquire.add_argument("--watch", action="store_true", help="repeat every poll_s until stopped")
    acquire.add_argument(
        "--host",
        nargs="+",
        action="extend",
        metavar="H",
        help="robot address(es) to try, replacing the configured hosts",
    )
    acquire.add_argument(
        "--include-active", action="store_true", help="also pull logs still being written"
    )
    acquire.add_argument(
        "--dry-run", action="store_true", help="list what would be copied; change nothing"
    )
    acquire.add_argument("--no-usb", action="store_true", help="do not scan removable volumes")
    lake_arg(acquire)

    backup = sub.add_parser(
        "backup", help="copy raw files and a metadata snapshot to backup.remote now"
    )
    lake_arg(backup)

    restore_cmd = sub.add_parser(
        "restore", help="copy raw files and metadata from the backup remote into a new lake"
    )
    restore_cmd.add_argument(
        "--force", action="store_true", help="restore over a lake that already has a ledger"
    )
    restore_cmd.add_argument(
        "--remote", help="rclone remote:path to restore from (default: backup.remote)"
    )
    lake_arg(restore_cmd)

    doctor = sub.add_parser("doctor", help="report environment and lake health")
    lake_arg(doctor)
    return parser


def _init(team_text: str | None, lake: Path | None) -> int:
    from flashpoint.setup.init import InitError, default_lake, init_config, parse_team

    root = config.config_root()
    interactive = sys.stdin.isatty()
    try:
        if team_text is None:
            if not interactive:
                print("pass --team NUMBER (and --lake FOLDER) when not in a terminal",
                      file=sys.stderr)  # fmt: skip
                return EXIT_USAGE
            team_text = input("FRC team number: ")
        team = parse_team(team_text)
        if lake is None:
            answer = input(f"Lake folder [{default_lake()}]: ") if interactive else ""
            lake = Path(answer.strip()) if answer.strip() else default_lake()
        result = init_config(root, team, lake)
    except InitError as exc:
        print(exc, file=sys.stderr)
        return EXIT_USAGE
    if not result.created and result.existing:
        print(f"a configuration already exists in {root}; nothing changed:")
        for path in result.existing:
            print(f"  {path.relative_to(root).as_posix()}")
        print(f"to change the team number, edit `team` in {root / 'acquire.toml'}")
        return 0
    print(f"created the configuration in {root}:")
    for path in result.created:
        print(f"  {path.relative_to(root).as_posix()}")
    for path in result.existing:
        print(f"  {path.relative_to(root).as_posix()} (already there, kept)")
    print(f"next: add your robot file to {root / 'robots'}, then run `flashpoint doctor`")
    return 0


def _summarize(report: "IngestReport") -> None:
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
    from flashpoint.semantics.derive import Deriver
    from flashpoint.semantics.robot_config import ConfigError

    root = config.config_root()
    if not (root / "acquire.toml").is_file():
        print(f"warning: no configuration in {root}: run `flashpoint init`. Until then no robot,"
              " slot or signal is mapped.", file=sys.stderr)  # fmt: skip
    elif not any((root / "robots").glob("*.toml")):
        print(f"warning: no robot configured in {root / 'robots'}: slots and signals stay"
              " unmapped until you add one", file=sys.stderr)  # fmt: skip
    try:
        deriver = Deriver(lake, root)
    except ConfigError as exc:
        print(f"robot configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    try:
        result = deriver.run(force=force)
    finally:
        deriver.close()
    print(f"derived {result['rebuilt']} of {result['sessions']} session(s)")
    # Detection is part of every derive; it skips itself when gold and config are unchanged.
    # Its own failure (configuration, lock) is reported but does not fail the derive.
    _detect(lake, force=False)
    return 0


DETECT_LOCK = "detect.lock"


def _detect(lake: LakePaths, force: bool) -> int:
    from flashpoint.anomaly import detect

    try:
        with AcquireLock(lake.meta / DETECT_LOCK):
            result = detect.detect(lake, config.config_root(), force=force)
    except LockHeldError:
        print(f"detect is already running on this lake (lock: {lake.meta / DETECT_LOCK})",
              file=sys.stderr)  # fmt: skip
        return EXIT_LOCKED
    except detect.DetectConfigError as exc:
        print(f"detection configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    summary = result.summary
    for note in summary.get("notes", []) if result.ran else []:
        print(f"warning: {note}", file=sys.stderr)
    counts = summary.get("counts", {})
    severities = ", ".join(f"{counts.get(s, 0)} {s}" for s in ("HIGH", "MED", "LOW"))
    building = summary.get("building_baseline", {}).get("count", 0)
    state = "detected" if result.ran else "unchanged:"
    print(
        f"{state} {summary.get('anomalies', 0)} anomalies ({severities});"
        f" {building} unit(s) building baseline;"
        f" scanned through {summary.get('scanned_through') or 'no matches'}"
    )
    return 0


def _backtest(lake: LakePaths) -> int:
    from flashpoint.anomaly import backtest, detect

    try:
        result = backtest.backtest(lake, config.config_root())
    except detect.DetectConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    if result.message:
        print(result.message)
    normal = result.report["sensitivities"]["normal"]
    counts, fp = normal["counts"], normal["false_positives"]
    rate = ", ".join(
        f"{m} {v['per_10_robot_matches']:.2f}"
        for m, v in fp["by_method"].items()
        if v["count"] and v["per_10_robot_matches"] is not None
    )
    print(
        f"backtest (normal sensitivity): {sum(counts.values())} label(s): caught"
        f" {counts['caught']}, missed {counts['missed']}, not evaluable {counts['not evaluable']}"
    )
    print(f"  false positives per 10 robot-matches: {rate or 'none'}")
    for label in normal["labels"]:
        lead = f", lead {label['lead_matches']} match(es)" if label["result"] == "caught" else ""
        why = f" ({label['reason']})" if label["reason"] else ""
        print(f"  {label['match']} {label['unit'] or label['slot']}: {label['result']}{lead}{why}")
    print(f"report: {result.md_path} (data: {result.json_path.name})")
    return 0


REPORT_LOCK = "report.lock"


def _report(lake: LakePaths, args: argparse.Namespace) -> int:
    from flashpoint.report.build import ReportBuilder
    from flashpoint.report.settings import ReportConfigError
    from flashpoint.semantics.robot_config import ConfigError

    if args.no_raw and args.static is None:
        print("--no-raw only applies with --static OUT", file=sys.stderr)
        return EXIT_USAGE
    if not (lake.meta / "sessions.parquet").is_file():
        print(f"no derived sessions in {lake.root}; run `flashpoint ingest` first", file=sys.stderr)
        return EXIT_FAILED
    try:
        builder = ReportBuilder(lake, config.config_root())
    except (ConfigError, ReportConfigError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    try:
        with AcquireLock(lake.meta / REPORT_LOCK):
            return _build_report(lake, builder, args)
    except LockHeldError:
        print(f"a Replay build is already running on this lake (lock: {lake.meta / REPORT_LOCK})",
              file=sys.stderr)  # fmt: skip
        return EXIT_LOCKED
    finally:
        builder.close()


def _build_report(lake: LakePaths, builder: "ReportBuilder", args: argparse.Namespace) -> int:
    summary = builder.build(args.event, args.match, force=args.all)
    print(
        f"built {len(summary.built)}, unchanged {len(summary.unchanged)},"
        f" removed {len(summary.removed)}; {summary.matches_listed} match(es) listed"
        f" in {summary.seconds:.1f}s"
    )
    for key, buckets in sorted(summary.reduced.items()):
        print(f"  {key}: over the size budget; reduced to {buckets} buckets")
    if summary.sessions_without_match_key:
        print(
            f"  {summary.sessions_without_match_key} session(s) without a match key"
            " (no Replay entry; their usage still counts in History)"
        )
    if args.static is not None:
        from flashpoint.report.export import ExportError, export_static

        try:
            result = export_static(lake, args.static, include_raw=not args.no_raw)
        except ExportError as exc:
            print(f"export failed: {exc}", file=sys.stderr)
            return EXIT_FAILED
        print(
            f"exported {result.matches} match(es) to {args.static}"
            f" ({result.raw_files} raw file(s){', not included' if args.no_raw else ''});"
            f" open {args.static / 'index.html'}"
        )
    return 0


def _serve(lake: LakePaths, args: argparse.Namespace) -> int:
    from flashpoint.report.settings import ReportConfigError, load_report_config
    from flashpoint.semantics.robot_config import ConfigError, load_robots
    from flashpoint.views.anomalies import AnomalyQueries
    from flashpoint.views.queries import HistoryQueries
    from flashpoint.web import advantagescope
    from flashpoint.web.server import FlashpointServer, host_warning, run

    if not args.no_build and (lake.meta / "sessions.parquet").is_file():
        # A fresh process keeps the server's idle memory free of the build's high-water mark.
        build = [sys.executable, "-m", "flashpoint", "report", "--lake", str(lake.root)]
        subprocess.run(build, check=False)
    try:
        robots_dir = config.config_root() / "robots"
        robots = load_robots(robots_dir) if robots_dir.is_dir() else []
        report_config = load_report_config(config.config_root())
    except (ConfigError, ReportConfigError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    install, problem = advantagescope.find(config.config_root())
    root = advantagescope.staging_root()
    try:
        advantagescope.cleanup(root)
    except OSError as exc:  # a locked file must not stop the app from starting
        print(f"warning: could not clean {root}: {exc}", file=sys.stderr)
    try:
        anomalies = AnomalyQueries(lake, config.config_root())
        server = FlashpointServer(
            (args.host, args.port),
            lake,
            HistoryQueries(lake, robots, report_config, anomalies),
            launcher=advantagescope.Launcher(install, problem, root),
            anomalies=anomalies,
            robot_hosts=args.robot_host,
        )
    except (OSError, OverflowError) as exc:
        print(f"cannot listen on {args.host}:{args.port}: {exc}", file=sys.stderr)
        return EXIT_FAILED
    warning = host_warning(args.host)
    if warning:
        print(warning, file=sys.stderr)
    return run(server, f"Flashpoint (lake {lake.root}) at {server.url}")


def _acquire(lake: LakePaths, args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    try:
        settings = AcquireConfig.load(config.config_root()).with_overrides(
            hosts=args.host, removable_media=False if args.no_usb else None
        )
    except AcquireConfigError as exc:
        print(f"acquire configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    if not settings.robot_hosts():
        print(
            "no robot address: no team number and no hosts in"
            f" {config.config_root() / 'acquire.toml'}; run `flashpoint init --team <number>`",
            file=sys.stderr,
        )
        return EXIT_USAGE
    return run_acquire(
        settings,
        lake,
        watch=args.watch,
        include_active=args.include_active,
        dry_run=args.dry_run,
        config_root=config.config_root(),
    )


def _load_acquire_config() -> AcquireConfig | None:
    try:
        return AcquireConfig.load(config.config_root())
    except AcquireConfigError as exc:
        print(f"acquire configuration error: {exc}", file=sys.stderr)
        return None


def _backup(lake: LakePaths) -> int:
    settings = _load_acquire_config()
    if settings is None:
        return EXIT_USAGE
    remote = settings.backup.remote
    if not remote:
        print(f"backup not configured: set backup.remote in {config.config_root()}/acquire.toml")
        return 0
    if not lake.ledger.is_file():  # before the lock file: a mistyped --lake stays uncreated
        print(f"no ledger at {lake.ledger}; nothing to back up", file=sys.stderr)
        return EXIT_FAILED
    try:
        with AcquireLock(lake.meta / LOCK_FILE):
            started = datetime.now(UTC).isoformat()
            root = config.config_root()
            previous = (read_status(lake.status) or {}).get("backup") or {}
            try:
                config_sha = config_hash(root)  # before the copy: an edit during it shows later
                summary = run_backup(lake, remote, config_root=root)
            except (BackupError, OSError, sqlite3.Error) as exc:
                _record_backup(lake, failed_slot(previous, started, exc))
                print(f"backup failed: {exc}", file=sys.stderr)
                return EXIT_FAILED
            _record_backup(lake, ok_slot(started, summary, config_sha))
            print(summary)
    except LockHeldError as exc:
        print(f"{exc}; a running watch backs up on its own", file=sys.stderr)
        return EXIT_LOCKED
    except OSError as exc:
        print(f"backup failed: {exc}", file=sys.stderr)
        return EXIT_FAILED
    return 0


def _record_backup(lake: LakePaths, backup: dict[str, Any]) -> None:
    """Record the outcome in the status; a status that cannot be written only warns."""
    try:
        record_backup(lake.status, backup)
    except OSError as exc:
        print(f"warning: cannot update the acquire status {lake.status}: {exc}", file=sys.stderr)


def _restore(lake: LakePaths, remote: str | None, force: bool) -> int:
    settings = _load_acquire_config()
    if settings is None:
        return EXIT_USAGE
    remote = remote or settings.backup.remote
    if not remote:
        print("no remote: pass --remote or set backup.remote in acquire.toml", file=sys.stderr)
        return EXIT_USAGE
    if lake.ledger.exists() and not force:  # checked before the lock file is created
        print(f"{lake.ledger} already exists; restore into a new lake, or pass --force",
              file=sys.stderr)  # fmt: skip
        return EXIT_FAILED
    try:
        with AcquireLock(lake.meta / LOCK_FILE):
            result = restore(lake, remote, force=force, config_root=config.config_root())
    except LockHeldError as exc:
        print(exc, file=sys.stderr)
        return EXIT_LOCKED
    except RestoreRefusedError as exc:
        print(exc, file=sys.stderr)
        return EXIT_FAILED
    except (BackupError, OSError, sqlite3.Error) as exc:
        print(f"restore failed: {exc}", file=sys.stderr)
        return EXIT_FAILED
    # Everything restored is on the remote: the restored lake starts backed up (backup-status).
    restored_at = datetime.now(UTC).isoformat(timespec="seconds")
    try:
        sha = config_hash(config.config_root())
    except OSError:
        sha = None
    _record_backup(lake, ok_slot(restored_at, f"restored from {remote}", sha))
    print(f"restored {result.files} file(s) from {remote};"
          " bronze, silver and gold must be rebuilt")  # fmt: skip
    if result.config == "restored":
        print(f"restored the configuration into {result.config_path}")
    elif result.config == "kept":
        print(f"kept the configuration in {config.config_root()};"
              f" the backed-up copy is in {result.config_path}")  # fmt: skip
    else:
        print("the backup held no configuration")
    print("run: flashpoint rebuild")
    return 0


def _acquire_status(lake: LakePaths) -> list[str]:
    if not lake.status.exists():
        return ["no acquire status yet"]
    status = read_status(lake.status)
    try:
        if status is None:
            raise StatusFormatError("not a JSON object")
        return describe_status(status)
    except StatusFormatError as exc:
        return [f"unreadable acquire status {lake.status}: {exc}"]


CHECK_MARKS = {"ok": "ok", "warning": "WARN", "problem": "PROBLEM", "n/a": "n/a"}


def _doctor(lake: LakePaths) -> int:
    from flashpoint.semantics.robot_config import ConfigError, load_config_seasons, load_robots
    from flashpoint.setup.checks import CheckContext, has_problem, run_checks
    from flashpoint.web import advantagescope

    root = config.config_root()
    print(f"flashpoint {flashpoint.__version__} (pipeline v{config.PIPELINE_VERSION})")
    print(f"python     {platform.python_version()} on {hoot.platform_key()}")
    print(f"config     {root}{'' if root.is_dir() else ' (missing: run `flashpoint init`)'}")
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
    try:
        seasons = load_config_seasons(root)
    except ConfigError:
        seasons = None  # the checks below name the bad file
    print("seasons")
    for season in (seasons or {}).values():
        roots = " ".join(f"{k}={v}" for k, v in season.root_names().items())
        print(f"  {season.season}  {roots}")
    robots_dir = root / "robots"
    print(f"robots     {robots_dir}")
    try:
        for robot in load_robots(robots_dir, seasons):
            counts = f"{len(robot.slots)} slots, {len(robot.signals)} signals"
            print(f"  {robot.robot:<14} season {robot.season} project {robot.project}: {counts}")
    except ConfigError:
        pass  # reported by the checks
    registry = hoot.default_registry(config.cache_root())
    print(f"owlet cache {registry.cache_dir}")
    print("acquire")
    for line in _acquire_status(lake):
        print(f"  {line}")
    install, problem = advantagescope.find(root)
    for line in advantagescope.doctor_lines(
        install, problem, advantagescope.staging_root(), lake.raw
    ):
        print(line)
    results = run_checks(CheckContext.default(root, lake))
    defaults = [r.source for r in results if r.default and r.source]
    if defaults:
        print(f"defaults   {', '.join(defaults)} (bundled with Flashpoint)")
    print("checks")
    for result in results:
        print(f"  {CHECK_MARKS[result.status]:<8} {result.title:<28} {result.message}")
        if result.status in ("warning", "problem") and result.command:
            print(f"  {'':<8} {'':<28} fix: {result.command}")
    problems = sum(r.status == "problem" for r in results)
    if has_problem(results):
        print(f"{problems} problem(s)")
        return EXIT_FAILED
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.command is None:
        _parser().print_help()
        return 0
    if args.command == "init":
        return _init(args.team, args.lake)
    lake = LakePaths(config.lake_root(args.lake))
    if args.command == "doctor":
        return _doctor(lake)
    if args.command == "derive":
        return _derive(lake, force=args.all)
    if args.command == "detect":
        return _detect(lake, force=args.all)
    if args.command == "backtest":
        return _backtest(lake)
    if args.command == "report":
        return _report(lake, args)
    if args.command == "serve":
        return _serve(lake, args)
    if args.command == "backup":
        return _backup(lake)
    if args.command == "restore":
        return _restore(lake, args.remote, args.force)
    if args.command == "acquire":
        if args.watch and args.dry_run:
            print("--dry-run runs a single cycle; drop --watch", file=sys.stderr)
            return EXIT_USAGE
        return _acquire(lake, args)
    from flashpoint.ingest import Ingestor

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
