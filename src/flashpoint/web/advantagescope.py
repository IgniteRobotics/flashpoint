"""Open a match's raw logs in AdvantageScope from the local app (ADR-0014).

AdvantageScope has no URL scheme and opens only the first file it is given, so the server
stages the match's logs under readable names and starts AdvantageScope on the wpilog.
"""

import ipaddress
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from flashpoint.lake.paths import LakePaths
from flashpoint.lake.raw import raw_path
from flashpoint.report.settings import ReportConfigError, advantagescope_path

STAGING_DIR = "flashpoint-as"
MAX_AGE_S = 24 * 3600  # staged folders older than this are removed when the server starts
COPY_BUFFER = 1 << 20
MATCH_KEY = re.compile(r"^[0-9a-z_]+$")
NOT_FOUND = "AdvantageScope not found"
NETWORK = "shared on the network"
NOT_THIS_MACHINE = "the request is not from this machine"
MARKER = "X-Flashpoint"
LOCAL_NAMES = ("localhost", "127.0.0.1", "[::1]")

WPILIB_NAMES = {
    "darwin": "AdvantageScope (WPILib).app",
    "win32": "AdvantageScope (WPILib).exe",
    "linux": "AdvantageScope (WPILib)",
}


class LocateError(Exception):
    pass


@dataclass(frozen=True)
class Install:
    path: Path
    found_by: str  # "config", "WPILib <year>", or "standalone"


def _present(path: Path) -> bool:
    return path.is_dir() if path.suffix == ".app" else path.is_file()


def _wpilib_root(platform: str, home: Path, env: Mapping[str, str]) -> Path:
    if platform == "win32":
        return Path(env.get("PUBLIC", r"C:\Users\Public")) / "wpilib"
    return home / "wpilib"


def _wpilib(platform: str, home: Path, env: Mapping[str, str]) -> Install | None:
    name = WPILIB_NAMES.get(platform)
    root = _wpilib_root(platform, home, env)
    if name is None or not root.is_dir():
        return None
    years = sorted((p for p in root.iterdir() if p.name.isdigit()), key=lambda p: int(p.name))
    for year in reversed(years):
        candidate = year / "advantagescope" / name
        if _present(candidate):
            return Install(candidate, f"WPILib {year.name}")
    return None


def _standalone(
    platform: str, env: Mapping[str, str], which: Callable[[str], str | None], applications: Path
) -> Install | None:
    if platform == "darwin":
        candidate: Path | None = applications / "AdvantageScope.app"
    elif platform == "win32":
        local = env.get("LOCALAPPDATA")
        candidate = (
            Path(local) / "Programs" / "AdvantageScope" / "AdvantageScope.exe" if local else None
        )
    else:
        found = which("advantagescope")
        candidate = Path(found) if found else None
    return Install(candidate, "standalone") if candidate and _present(candidate) else None


def locate(
    explicit: Path | None,
    *,
    platform: str = sys.platform,
    home: Path | None = None,
    env: Mapping[str, str] | None = None,
    which: Callable[[str], str | None] = shutil.which,
    applications: Path = Path("/Applications"),
) -> Install | None:
    """The configured path, else the newest WPILib year, else a standalone install.

    A configured path that does not exist is an error: no fallback past it.
    """
    if explicit is not None:
        if not _present(explicit):
            raise LocateError(f"configured AdvantageScope path not found: {explicit}")
        return Install(explicit, "config")
    home = Path.home() if home is None else home
    env = os.environ if env is None else env
    return _wpilib(platform, home, env) or _standalone(platform, env, which, applications)


def find(config_dir: Path, **kwargs: object) -> tuple[Install | None, str | None]:
    """`locate` with the configured path; a problem is returned as text, never raised."""
    try:
        install = locate(advantagescope_path(config_dir), **kwargs)  # type: ignore[arg-type]
    except (LocateError, ReportConfigError) as exc:
        return None, str(exc)
    return (install, None) if install else (None, NOT_FOUND)


def staging_root() -> Path:
    return Path(tempfile.gettempdir()) / STAGING_DIR


def _existing(path: Path) -> Path:
    while not path.exists() and path.parent != path:
        path = path.parent
    return path


def links_allowed(platform: str = sys.platform) -> bool:
    """Windows can't delete a read-only file, and a hard link shares the raw store's read-only
    flag, so cleaning up a link would mean unprotecting the raw file: Windows copies."""
    return platform != "win32"


def staging_mode(raw_dir: Path, root: Path) -> str:
    """ "link" when staging and the raw store share a volume (no bytes copied), else "copy"."""
    if not links_allowed():
        return "copy"
    same = _existing(raw_dir).stat().st_dev == _existing(root).stat().st_dev
    return "link" if same else "copy"


def doctor_lines(
    install: Install | None, problem: str | None, root: Path, raw_dir: Path
) -> list[str]:
    if install is None:
        if problem == NOT_FOUND:
            return ["AdvantageScope: not found"]
        return [f"AdvantageScope: ERROR {problem}"]
    return [
        f"AdvantageScope: {install.path} ({install.found_by})",
        f"  staging {root} ({staging_mode(raw_dir, root)})",
    ]


# --- staging ---------------------------------------------------------------------------


class StagingError(Exception):
    def __init__(self, missing: list[str]) -> None:
        super().__init__(f"raw file(s) missing from the raw store: {', '.join(missing)}")
        self.missing = missing


@dataclass(frozen=True)
class Staged:
    path: Path
    part: str  # "wpilog" or the hoot's bus
    kind: str
    sha256: str
    incomplete: bool
    linked: bool  # a hard link to the raw store (False: a copy)


def _no_link(source: Path, target: Path) -> None:
    raise OSError("hard links are not used on this platform")


def _copy(source: Path, target: Path) -> None:
    tmp = target.with_name(target.name + ".part")
    with source.open("rb") as src, tmp.open("wb") as dst:
        shutil.copyfileobj(src, dst, COPY_BUFFER)
    tmp.replace(target)


def _place(source: Path, target: Path, link: Callable[[Path, Path], None]) -> bool:
    """Hard-link source to target, or copy it; True when linked. A matching file is kept."""
    if target.is_file() and not target.is_symlink():
        if target.stat().st_size == source.stat().st_size:
            return source.samefile(target)
        target.unlink()
    elif target.is_symlink():
        target.unlink()
    try:
        link(source, target)
        return True
    except OSError:
        _copy(source, target)
        return False


def stage(
    lake: LakePaths,
    match_key: str,
    sources: list[dict[str, Any]],
    root: Path,
    link: Callable[[Path, Path], None] | None = None,
) -> list[Staged]:
    """Put the match's raw logs in `root/<match_key>/` under their download names.

    The raw store is only read. Nothing is staged if any raw file is missing.
    """
    if not MATCH_KEY.match(match_key):
        raise ValueError(f"not a match key: {match_key!r}")
    raws = [raw_path(lake, s["sha256"], f".{s['kind']}") for s in sources]
    missing = [s["sha256"] for s, raw in zip(sources, raws, strict=True) if not raw.is_file()]
    if missing:
        raise StagingError(missing)
    if link is None:
        link = os.link if links_allowed() else _no_link
    folder = root / match_key
    folder.mkdir(parents=True, exist_ok=True)
    staged = []
    for source, raw in zip(sources, raws, strict=True):
        target = folder / source["download"]
        linked = _place(raw, target, link)
        staged.append(
            Staged(target, source["part"], source["kind"], source["sha256"],
                   bool(source.get("incomplete")), linked)
        )  # fmt: skip
    os.utime(folder)  # keeps a folder in use from aging out
    return staged


def cleanup(root: Path, now: float | None = None, max_age_s: float = MAX_AGE_S) -> list[Path]:
    """Remove staged match folders older than max_age_s. Only real directories directly under
    root are touched; symlinks (including a symlinked root) are never followed."""
    if root.is_symlink() or not root.is_dir():
        return []
    now = time.time() if now is None else now
    removed = []
    with os.scandir(root) as entries:
        for entry in entries:
            if not entry.is_dir(follow_symlinks=False):
                continue
            if now - entry.stat(follow_symlinks=False).st_mtime > max_age_s:
                shutil.rmtree(entry.path)
                removed.append(Path(entry.path))
    return sorted(removed)


# --- the guarded launch ----------------------------------------------------------------

Spawn = Callable[[list[str]], None]
Response = tuple[int, dict[str, Any]]


class Sources(Protocol):
    def sources(self, match_key: str) -> list[dict[str, Any]]: ...


def _loopback(host: str) -> bool:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    mapped = getattr(address, "ipv4_mapped", None)
    return bool(address.is_loopback or (mapped is not None and mapped.is_loopback))


def refusal(bound_local: bool, port: int, client: str, headers: Mapping[str, str]) -> str | None:
    """Why a launch request is refused, or None. Every check must pass (ADR-0014):
    the server is bound to loopback, the client is loopback, `Host` names this machine
    (DNS rebinding), and `Origin`, the marker header, and a JSON body force a CORS preflight
    that this server never answers, so no other web page can send it."""
    if not bound_local:
        return NETWORK
    if not _loopback(client):
        return NOT_THIS_MACHINE
    host = headers.get("Host", "")
    name, _, host_port = host.rpartition(":")
    if name not in LOCAL_NAMES or host_port != str(port):
        return "the Host header does not name this machine"
    if not host or headers.get("Origin") != f"http://{host}":
        return "cross-origin request refused"
    if headers.get(MARKER) != "launch":
        return f"missing {MARKER} header"
    if headers.get("Content-Type", "").split(";")[0].strip().lower() != "application/json":
        return "Content-Type must be application/json"
    return None


def command(install: Path, file: Path, platform: str = sys.platform) -> list[str]:
    """`open -a` for a macOS bundle (a running AdvantageScope opens a new window), else the
    executable itself. Always exactly one log argument."""
    if platform == "darwin" and install.suffix == ".app":
        return ["open", "-a", str(install), str(file)]
    return [str(install), str(file)]


def spawn(argv: list[str]) -> None:
    """Start detached and never wait: no shell, no inherited I/O."""
    options: dict[str, Any] = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "close_fds": True,
    }
    if sys.platform == "win32":
        options["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        options["start_new_session"] = True
    subprocess.Popen(argv, **options)  # noqa: S603 - argv list: located install + staged file


class Launcher:
    def __init__(
        self,
        install: Install | None,
        problem: str | None = None,
        root: Path | None = None,
        spawn: Spawn = spawn,
    ) -> None:
        self.install = install
        self.problem = None if install else problem or NOT_FOUND
        self.root = staging_root() if root is None else root
        self.spawn = spawn

    def availability(self, bound_local: bool, client: str) -> dict[str, Any]:
        reason = None
        if not bound_local:
            reason = NETWORK
        elif not _loopback(client):
            reason = NOT_THIS_MACHINE
        elif self.install is None:
            reason = self.problem
        return {
            "available": reason is None,
            "reason": reason,
            "app": str(self.install.path) if self.install else None,
            "found_by": self.install.found_by if self.install else None,
        }

    def launch(self, lake: LakePaths, queries: Sources, match_key: object) -> Response:
        def refused(status: int, reason: str) -> Response:
            return status, {"started": False, "reason": reason}

        if self.install is None:
            return refused(503, self.problem or NOT_FOUND)
        if not isinstance(match_key, str) or not MATCH_KEY.match(match_key):
            return refused(404, "no such match")
        sources = queries.sources(match_key)
        if not sources:
            return refused(404, f"no raw logs for {match_key}")
        if not any(s["part"] == "wpilog" for s in sources):
            return refused(409, f"{match_key} has no wpilog to open; download the hoots instead")
        try:
            staged = stage(lake, match_key, sources, self.root)
        except StagingError as exc:
            return refused(500, str(exc))
        except OSError as exc:
            return refused(500, f"could not stage the logs: {exc}")
        wpilog = next(s for s in staged if s.part == "wpilog")
        try:
            self.spawn(command(self.install.path, wpilog.path))
        except OSError as exc:
            return refused(500, f"AdvantageScope did not start: {exc}")
        return 200, {
            "started": True,
            "folder": str(wpilog.path.parent),
            "wpilog": wpilog.path.name,
            "hoots": [
                {"name": s.path.name, "part": s.part, "incomplete": s.incomplete}
                for s in staged
                if s.part != "wpilog"
            ],
            "others": [s.path.name for s in staged if s.part == "wpilog" and s is not wpilog],
        }
