"""Fix actions the Setup view may run on the machine running `serve` (ADR-0017).

A fixed allow-list of Python functions, never shell strings. Each takes the check context and
the request body, is safe to run twice, and returns what it changed; the server then re-runs
the checks.
"""

import os
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

from flashpoint.readers.hoot import HootError
from flashpoint.setup.checks import SERVICE_NAME, CheckContext, owlet_compliancies

Fix = Callable[[CheckContext, Any], dict[str, Any]]
DEPLOY = Path(__file__).resolve().parents[1] / "deploy"
SYSTEMD_UNIT = DEPLOY / "systemd" / f"{SERVICE_NAME}.service"
WINDOWS_TASK = DEPLOY / "windows" / f"{SERVICE_NAME}.xml"
UNIT_EXEC = "ExecStart=%h/.local/bin/flashpoint acquire --watch"
TASK_COMMAND = r"<Command>%USERPROFILE%\.local\bin\flashpoint.exe</Command>"


class FixError(ValueError):
    """The fix was refused or failed; the message says why. Nothing was left half-done."""


def _done(message: str, changed: list[str]) -> dict[str, Any]:
    return {"ok": True, "message": message, "changed": changed}


def _body(body: Any) -> dict[str, Any]:
    if body is None:
        return {}
    if not isinstance(body, dict):
        raise FixError("expected a JSON object")
    return body


# --- init, owlet, AdvantageScope ----------------------------------------------------------


def fix_init(ctx: CheckContext, body: Any) -> dict[str, Any]:
    from flashpoint.setup.init import InitError, default_lake, init_config, parse_team

    values = _body(body)
    team = values.get("team")
    if isinstance(team, bool) or not isinstance(team, int | str):
        raise FixError("enter your team number")
    lake = values.get("lake") or str(default_lake())
    if not isinstance(lake, str):
        raise FixError("the lake folder must be text")
    try:
        result = init_config(ctx.config_root, parse_team(str(team)), Path(lake))
    except InitError as exc:
        raise FixError(str(exc)) from exc
    if not result.created:
        return _done(f"a configuration already exists in {ctx.config_root}; nothing changed", [])
    changed = [p.relative_to(ctx.config_root).as_posix() for p in result.created]
    return _done(f"created the configuration in {ctx.config_root}", changed)


def fix_owlet(ctx: CheckContext, body: Any) -> dict[str, Any]:
    registry = ctx.registry(ctx.cache_root)
    changed = []
    for compliancy in owlet_compliancies(ctx)[0]:
        try:
            version = registry.version_for(compliancy)
            try:
                registry.binary_for(compliancy, download=False)
                continue  # already downloaded and verified
            except HootError:
                pass
            registry.binary_for(compliancy, download=True)
        except HootError as exc:
            raise FixError(f"owlet C{compliancy}: {exc}") from exc
        changed.append(f"owlet {version} (C{compliancy})")
    if not changed:
        return _done("owlet is already downloaded and verified", [])
    return _done(f"downloaded and verified {', '.join(changed)}", changed)


def fix_advantagescope(ctx: CheckContext, body: Any) -> dict[str, Any]:
    from flashpoint.setup import editor
    from flashpoint.web.advantagescope import LocateError, locate

    path = _body(body).get("path")
    if not isinstance(path, str) or not path.strip():
        raise FixError("enter the AdvantageScope path")
    try:
        locate(Path(path.strip()).expanduser())
    except LocateError as exc:
        raise FixError(str(exc)) from exc
    current = editor.toml_dict(ctx.config_root, "report.toml").get("advantagescope", {})
    if current.get("path") == path.strip():
        return _done("the AdvantageScope path is already set", [])
    try:
        result = editor.save_thresholds(
            ctx.config_root,
            "report.toml",
            {"advantagescope": {"path": path.strip()}},
            editor.file_sha(ctx.config_root, "report.toml"),
        )
    except editor.SaveError as exc:
        raise FixError(str(exc)) from exc
    return _done(f"set the AdvantageScope path in {result.path}", [result.path])


# --- the acquire service ------------------------------------------------------------------


def _not_on_macos(ctx: CheckContext) -> None:
    if ctx.system == "Darwin":
        raise FixError(
            "the acquire service is not available on macOS: run `flashpoint acquire --watch`"
        )


def _command(ctx: CheckContext, args: Sequence[str]) -> None:
    try:
        result = ctx.run(args)
    except OSError as exc:
        raise FixError(f"{args[0]}: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise FixError(f"`{' '.join(args)}` failed: {detail}")


def _executable(ctx: CheckContext) -> str:
    found = ctx.which("flashpoint")
    if not found:
        raise FixError("can't find the flashpoint command; install it with pipx first")
    return found


def systemd_unit(executable: str) -> str:
    text = SYSTEMD_UNIT.read_text(encoding="utf-8")
    return text.replace(UNIT_EXEC, f"ExecStart={executable} acquire --watch")


def windows_task(executable: str) -> str:
    text = WINDOWS_TASK.read_text(encoding="utf-8")
    return text.replace(TASK_COMMAND, f"<Command>{escape(executable)}</Command>")


def _systemd_dir(ctx: CheckContext) -> Path:
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg and ctx.home == Path.home() else ctx.home / ".config"
    return base / "systemd" / "user"


def fix_service_install(ctx: CheckContext, body: Any) -> dict[str, Any]:
    _not_on_macos(ctx)
    executable = _executable(ctx)
    if ctx.system == "Windows":
        fd, name = tempfile.mkstemp(prefix="flashpoint-acquire-", suffix=".xml")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(windows_task(executable))
            _command(ctx, ["schtasks", "/Create", "/XML", name, "/TN", SERVICE_NAME, "/F"])
        finally:
            Path(name).unlink(missing_ok=True)
        return _done(f"installed the {SERVICE_NAME} task (runs at logon)", [SERVICE_NAME])
    unit = _systemd_dir(ctx) / f"{SERVICE_NAME}.service"
    text = systemd_unit(executable)
    if unit.is_file() and unit.read_text(encoding="utf-8") == text:
        _command(ctx, ["systemctl", "--user", "enable", SERVICE_NAME])
        return _done(f"{unit} is already installed", [])
    unit.parent.mkdir(parents=True, exist_ok=True)
    unit.write_text(text, encoding="utf-8")
    _command(ctx, ["systemctl", "--user", "daemon-reload"])
    _command(ctx, ["systemctl", "--user", "enable", SERVICE_NAME])
    return _done(f"installed {unit}; start it to begin importing", [str(unit)])


def fix_service_start(ctx: CheckContext, body: Any) -> dict[str, Any]:
    _not_on_macos(ctx)
    if ctx.system == "Windows":
        _command(ctx, ["schtasks", "/Run", "/TN", SERVICE_NAME])
    else:
        _command(ctx, ["systemctl", "--user", "enable", "--now", SERVICE_NAME])
    return _done(f"started {SERVICE_NAME}", [])


FIXES: dict[str, Fix] = {
    "init": fix_init,
    "owlet": fix_owlet,
    "advantagescope": fix_advantagescope,
    "service-install": fix_service_install,
    "service-start": fix_service_start,
}
