"""Setup checks: one list, printed by `flashpoint doctor` and shown in the Setup view.

Each check reports ok, warning, problem or n/a, a message, and how to fix it: a fix action the
app may run (`flashpoint.setup.fixes`) or the exact command. Checks never download or change
anything, and the whole run stays under 2 s.
"""

import datetime as dt
import platform
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Literal

from pydantic import ValidationError

from flashpoint import config
from flashpoint.acquire.config import CONFIG_FILE as ACQUIRE_FILE
from flashpoint.acquire.config import AcquireConfig, AcquireConfigError
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.readers import hoot

Status = Literal["ok", "warning", "problem", "n/a"]
PYTHON_RANGE = ((3, 11), (3, 13))
SERVICE_NAME = "flashpoint-acquire"
SERVICE_TIMEOUT_S = 5
# Fix actions the app can run, with the button label (see flashpoint.setup.fixes).
FIX_LABELS = {
    "init": "Create configuration",
    "owlet": "Download owlet",
    "advantagescope": "Set AdvantageScope path",
    "service-install": "Install the acquire service",
    "service-start": "Start the acquire service",
}
# Shared settings files, each the team's copy or else the bundled default.
SETTINGS_FILES = ("report.toml", "anomaly.toml", "anomaly-causes.toml")
INIT_COMMAND = "flashpoint init --team <number>"

Runner = Callable[[Sequence[str]], "subprocess.CompletedProcess[str]"]
Finder = Callable[[Path], tuple[Any, str | None]]


@dataclass(frozen=True)
class CheckResult:
    id: str
    title: str
    status: Status
    message: str
    fix_action: str | None = None
    command: str | None = None
    source: str | None = None  # the configuration file checked, relative to the root
    default: bool = False  # that file is the bundled default

    @property
    def fix_label(self) -> str | None:
        return FIX_LABELS.get(self.fix_action) if self.fix_action else None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"fix_label": self.fix_label}


def _run(args: Sequence[str]) -> "subprocess.CompletedProcess[str]":
    return subprocess.run(
        list(args), capture_output=True, text=True, timeout=SERVICE_TIMEOUT_S, check=False
    )


def _find_advantagescope(root: Path) -> tuple[Any, str | None]:
    from flashpoint.web import advantagescope

    return advantagescope.find(root)


def current_season() -> int:
    """The FRC season is the calendar year of its January kickoff."""
    return dt.date.today().year


@dataclass
class CheckContext:
    config_root: Path
    lake: LakePaths
    cache_root: Path
    system: str = field(default_factory=platform.system)
    season: int = field(default_factory=lambda: current_season())
    python: tuple[int, int] = (sys.version_info.major, sys.version_info.minor)
    run: Runner = _run
    find_advantagescope: Finder = _find_advantagescope
    registry: Callable[[Path], hoot.OwletRegistry] = hoot.default_registry
    home: Path = field(default_factory=Path.home)
    which: Callable[[str], str | None] = shutil.which

    @classmethod
    def default(cls, config_root: Path, lake: LakePaths) -> "CheckContext":
        return cls(config_root, lake, config.cache_root())


# --- configuration ------------------------------------------------------------------------


def _config_root(ctx: CheckContext) -> CheckResult:
    root = ctx.config_root
    if root.is_dir():
        return CheckResult("config-root", "Configuration", "ok", f"configuration in {root}")
    return CheckResult(
        "config-root",
        "Configuration",
        "problem",
        f"no configuration in {root}: Flashpoint doesn't know your team or robot",
        fix_action="init",
        command=INIT_COMMAND,
    )


def describe_error(exc: BaseException) -> str:
    """One line for a loading error: pydantic's per-field errors without the input dump."""
    found = exc if isinstance(exc, ValidationError) else exc.__cause__
    if not isinstance(found, ValidationError):
        return " ".join(str(exc).split())
    problems = []
    for error in found.errors():
        where = ".".join(str(part) for part in error["loc"])
        message = error["msg"].removeprefix("Value error, ")
        problems.append(f"{where}: {message}" if where else message)
    return "; ".join(problems)


def _file(ctx: CheckContext, name: str, load: Callable[[], object]) -> CheckResult:
    default = config.is_default(ctx.config_root, name)
    shown = f"{name} (bundled default)" if default else name
    from flashpoint.semantics.robot_config import MissingSeasonError

    try:
        load()
    except Exception as exc:  # each loader raises its own error type; all are shown as text
        command = f"fix {config.config_file(ctx.config_root, name)}"
        if isinstance(exc, MissingSeasonError):
            seasons = ctx.config_root / "seasons"
            command = (
                f"create {seasons / f'{exc.season}.toml'}: copy"
                f" {seasons / f'{exc.season - 1}.toml'}, set `season = {exc.season}`,"
                " and check the roots"
            )
        return CheckResult(
            f"file:{name}",
            name,
            "problem",
            f"{shown}: {describe_error(exc)}",
            command=command,
            source=name,
            default=default,
        )
    return CheckResult(f"file:{name}", name, "ok", f"{shown} loads", source=name, default=default)


def _files(ctx: CheckContext) -> tuple[list[CheckResult], list[Any], AcquireConfig | None]:
    from flashpoint.anomaly.catalogue import LABELS_FILE, load_causes, load_labels
    from flashpoint.anomaly.settings import load_anomaly_config
    from flashpoint.report.settings import load_report_config
    from flashpoint.semantics.robot_config import load_robot, load_season

    root = ctx.config_root
    results: list[CheckResult] = []
    acquire: list[AcquireConfig] = []
    if (root / ACQUIRE_FILE).is_file():
        results.append(_file(ctx, ACQUIRE_FILE, lambda: acquire.append(AcquireConfig.load(root))))
    loaders: dict[str, Callable[[], object]] = {
        "report.toml": lambda: load_report_config(root),
        "anomaly.toml": lambda: load_anomaly_config(root),
        "anomaly-causes.toml": lambda: load_causes(root),
    }
    results += [_file(ctx, name, loaders[name]) for name in SETTINGS_FILES]

    seasons: dict[int, Any] = {}

    def add_season(path: Path) -> None:
        season = load_season(path)
        seasons[season.season] = season

    years = {p.name for p in (config.DEFAULTS_ROOT / "seasons").glob("*.toml")}
    years |= {p.name for p in (root / "seasons").glob("*.toml")}
    for year_file in sorted(years):
        name = f"seasons/{year_file}"
        path = config.config_file(root, name)
        results.append(_file(ctx, name, partial(add_season, path)))

    robots: list[Any] = []

    def add_robot(path: Path) -> None:
        robots.append(load_robot(path, seasons))

    for path in sorted((root / "robots").glob("*.toml")):
        name = f"robots/{path.name}"
        results.append(_file(ctx, name, partial(add_robot, path)))
    if (root / LABELS_FILE).is_file():
        results.append(_file(ctx, LABELS_FILE, lambda: load_labels(root, robots)))
    return results, robots, acquire[0] if acquire else None


def _team(ctx: CheckContext, acquire: AcquireConfig | None) -> CheckResult:
    title = "Team"
    path = ctx.config_root / ACQUIRE_FILE
    if not ctx.config_root.is_dir():
        return CheckResult(
            "team",
            title,
            "problem",
            "no team number: no configuration yet",
            fix_action="init",
            command=INIT_COMMAND,
        )
    if acquire is None and path.is_file():
        return CheckResult(
            "team",
            title,
            "problem",
            f"{ACQUIRE_FILE} doesn't load (see above)",
            command=f"fix {path}",
            source=ACQUIRE_FILE,
        )
    if acquire is None:
        try:
            acquire = AcquireConfig.load(ctx.config_root)
        except AcquireConfigError:  # pragma: no cover - the file is absent here
            acquire = AcquireConfig()
    hosts = acquire.robot_hosts()
    if not hosts:
        return CheckResult(
            "team",
            title,
            "problem",
            "no team number: acquire can't find your robot",
            command=f"set `team = <number>` in {path}",
            source=ACQUIRE_FILE,
        )
    who = f"team {acquire.team}" if acquire.team is not None else "no team number"
    return CheckResult(
        "team", title, "ok", f"{who}; robot addresses {', '.join(hosts)}", source=ACQUIRE_FILE
    )


def _robot_season(ctx: CheckContext, robots: list[Any]) -> CheckResult:
    title = f"Robot for {ctx.season}"
    current = [r.robot for r in robots if r.season == ctx.season]
    if current:
        return CheckResult("robot-season", title, "ok", f"{', '.join(current)}")
    return CheckResult(
        "robot-season",
        title,
        "problem",
        f"no robot configured for season {ctx.season}: motors aren't matched to slots",
        command=f"add a robot file to {ctx.config_root / 'robots'}",
    )


# --- lake, owlet, AdvantageScope ----------------------------------------------------------


def _writable(folder: Path) -> bool:
    try:
        with tempfile.TemporaryFile(dir=folder):
            return True
    except OSError:
        return False


def _lake(ctx: CheckContext) -> CheckResult:
    root = ctx.lake.root
    if root.is_dir():
        if _writable(root):
            return CheckResult("lake", "Lake", "ok", f"{root}")
        return CheckResult(
            "lake",
            "Lake",
            "problem",
            f"can't write to {root}",
            command=f"choose another lake: set `lake` in {ACQUIRE_FILE}",
        )
    parent = root.parent
    while not parent.exists() and parent.parent != parent:
        parent = parent.parent
    if parent.is_dir() and _writable(parent):
        return CheckResult(
            "lake", "Lake", "warning", f"{root} not created yet; the first import creates it"
        )
    return CheckResult(
        "lake",
        "Lake",
        "problem",
        f"can't create {root}: {parent} isn't writable",
        command=f"choose another lake: set `lake` in {ACQUIRE_FILE}",
    )


def _lake_compliancies(lake: LakePaths) -> list[int]:
    if not lake.ledger.is_file():
        return []
    ledger = Ledger(lake.ledger)
    try:
        rows = ledger.query(
            "SELECT DISTINCT compliancy FROM hoot_logs WHERE compliancy IS NOT NULL"
        )
    finally:
        ledger.close()
    return sorted(int(r["compliancy"]) for r in rows)


def owlet_compliancies(ctx: CheckContext) -> tuple[list[int], bool]:
    """The hoot formats the lake holds (True), else the newest the registry knows (False)."""
    in_lake = _lake_compliancies(ctx.lake)
    return (in_lake, True) if in_lake else (ctx.registry(ctx.cache_root).compliancies()[-1:], False)


def _owlet(ctx: CheckContext) -> list[CheckResult]:
    registry = ctx.registry(ctx.cache_root)
    wanted, in_lake = owlet_compliancies(ctx)
    results = []
    for compliancy in wanted:
        check_id, title = f"owlet:C{compliancy}", f"owlet for hoot format C{compliancy}"
        try:
            version = registry.version_for(compliancy)
        except hoot.HootError:
            results.append(
                CheckResult(
                    check_id,
                    title,
                    "problem",
                    f"the lake has C{compliancy} hoots, which this Flashpoint can't read",
                    command="pipx upgrade flashpoint",
                )
            )
            continue
        try:
            registry.binary_for(compliancy, download=False)
        except hoot.HootError as exc:
            if exc.reason == "owlet-unavailable-for-platform":
                results.append(
                    CheckResult(
                        check_id,
                        title,
                        "problem",
                        f"owlet {version} has no build for {hoot.platform_key()}",
                    )
                )
                continue
            state = "checksum doesn't match" if "checksum" in exc.reason else "not downloaded"
            status: Status = "problem" if in_lake else "warning"
            why = "" if in_lake else " (needed for the first hoot import; needs internet)"
            results.append(
                CheckResult(
                    check_id,
                    title,
                    status,
                    f"owlet {version}: {state}{why}",
                    fix_action="owlet",
                    command="flashpoint ingest <a .hoot file> (downloads owlet)",
                )
            )
            continue
        results.append(CheckResult(check_id, title, "ok", f"owlet {version} downloaded, verified"))
    return results


def _advantagescope(ctx: CheckContext) -> CheckResult:
    title = "AdvantageScope"
    install, problem = ctx.find_advantagescope(ctx.config_root)
    if install is not None:
        return CheckResult("advantagescope", title, "ok", f"{install.path} ({install.found_by})")
    from flashpoint.web.advantagescope import NOT_FOUND

    command = "set [advantagescope] path in report.toml"
    if problem in (None, NOT_FOUND, "not found"):
        return CheckResult(
            "advantagescope",
            title,
            "warning",
            "not found: install WPILib, or set its path, to open logs from Replay",
            fix_action="advantagescope",
            command=command,
            source="report.toml",
        )
    return CheckResult(
        "advantagescope",
        title,
        "problem",
        f"{problem}",
        fix_action="advantagescope",
        command=command,
        source="report.toml",
    )


# --- acquire service, Python --------------------------------------------------------------


def _query(ctx: CheckContext, args: Sequence[str]) -> "subprocess.CompletedProcess[str] | None":
    try:
        return ctx.run(args)
    except (OSError, subprocess.SubprocessError):
        return None


def _service(ctx: CheckContext) -> CheckResult:
    check_id, title = "acquire-service", "Acquire service"
    if ctx.system == "Darwin":
        return CheckResult(
            check_id,
            title,
            "n/a",
            "not applicable on macOS: run `flashpoint acquire --watch` in a terminal",
        )
    if ctx.system == "Windows":
        query = _query(ctx, ["schtasks", "/Query", "/TN", SERVICE_NAME, "/FO", "CSV", "/NH"])
        if query is None:
            return CheckResult(check_id, title, "warning", "can't ask Task Scheduler")
        if query.returncode != 0:
            installed, running = False, False
        else:
            installed, running = True, '"Running"' in query.stdout
    else:
        enabled = _query(ctx, ["systemctl", "--user", "is-enabled", SERVICE_NAME])
        active = _query(ctx, ["systemctl", "--user", "is-active", SERVICE_NAME])
        if enabled is None or active is None:
            return CheckResult(
                check_id,
                title,
                "warning",
                "can't ask systemd (no user session?); run `flashpoint acquire --watch` yourself",
            )
        state = enabled.stdout.strip()
        installed = enabled.returncode == 0 or state in ("disabled", "static", "linked")
        running = active.returncode == 0
    if running:
        return CheckResult(check_id, title, "ok", "installed and running")
    if installed:
        return CheckResult(
            check_id,
            title,
            "warning",
            "installed but not running",
            fix_action="service-start",
            command=_start_command(ctx),
        )
    return CheckResult(
        check_id,
        title,
        "warning",
        "not installed: logs are imported only when you run `flashpoint acquire`",
        fix_action="service-install",
        command="see docs/guide/auto-import.md",
    )


def _start_command(ctx: CheckContext) -> str:
    if ctx.system == "Windows":
        return f"schtasks /Run /TN {SERVICE_NAME}"
    return f"systemctl --user enable --now {SERVICE_NAME}"


def _python(ctx: CheckContext) -> CheckResult:
    low, high = PYTHON_RANGE
    shown = f"{ctx.python[0]}.{ctx.python[1]}"
    if low <= ctx.python <= high:
        return CheckResult("python", "Python", "ok", f"Python {shown}")
    return CheckResult(
        "python",
        "Python",
        "problem",
        f"Python {shown} isn't supported (3.11 to 3.13)",
        command="pipx reinstall --python python3.12 flashpoint",
    )


def run_checks(ctx: CheckContext) -> list[CheckResult]:
    files, robots, acquire = _files(ctx)
    return [
        _config_root(ctx),
        *files,
        _team(ctx, acquire),
        _robot_season(ctx, robots),
        _lake(ctx),
        *_owlet(ctx),
        _advantagescope(ctx),
        _service(ctx),
        _python(ctx),
    ]


def has_problem(results: list[CheckResult]) -> bool:
    return any(r.status == "problem" for r in results)
