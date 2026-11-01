"""The Setup view's configuration API: every section with its source file, and guarded saves
(`GET /api/config`, `PUT /api/config/<section>`). Saves go through `flashpoint.setup.editor`.
`POST /api/theme/preview` derives and checks a palette without saving it (team-branding D-f)."""

import datetime as dt
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import asdict
from functools import partial
from pathlib import Path
from typing import Any

from flashpoint import config
from flashpoint.setup import editor
from flashpoint.setup.checks import describe_error
from flashpoint.setup.editor import SaveError

Response = tuple[int, dict[str, Any]]


def _jsonable(value: Any) -> Any:
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, Mapping):
        return {k: _jsonable(v) for k, v in value.items()}
    return value


def _file(root: Path, rel: str, validate: Callable[[], object]) -> dict[str, Any]:
    """Source, default flag, hash, parsed data and the loader's error for one file."""
    data_bytes = editor.current_bytes(root, rel)
    entry: dict[str, Any] = {
        "source": rel,
        "default": config.is_default(root, rel) and data_bytes is not None,
        "sha256": editor.file_sha(root, rel),
        "data": None,
        "error": None,
    }
    if data_bytes is not None:
        try:
            entry["data"] = _jsonable(tomllib.loads(data_bytes.decode("utf-8")))
        except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
            entry["error"] = f"{rel}: {describe_error(exc)}"
            return entry
    try:
        validate()
    except Exception as exc:  # each loader has its own error type
        entry["error"] = f"{rel}: {describe_error(exc)}"
    return entry


def config_view(root: Path) -> dict[str, Any]:
    from flashpoint.acquire.config import CONFIG_FILE, AcquireConfig
    from flashpoint.anomaly.settings import load_anomaly_config
    from flashpoint.report.settings import load_report_config
    from flashpoint.semantics.robot_config import load_config_seasons, load_robot

    team = _file(root, CONFIG_FILE, lambda: AcquireConfig.load(root))
    team["values"] = team.pop("data") or {}
    try:
        team["hosts"] = AcquireConfig.load(root).robot_hosts()
    except Exception:
        team["hosts"] = []
    team["affects"] = editor.AFFECTS[CONFIG_FILE]

    try:
        seasons = load_config_seasons(root)
    except Exception:
        seasons = {}
    robots = []
    for path in sorted((root / "robots").glob("*.toml")):
        rel = f"robots/{path.name}"
        entry = _file(root, rel, partial(load_robot, path, seasons))
        robots.append({"id": path.stem, **entry, "affects": editor.AFFECTS["robots"]})

    loaders = {"report.toml": load_report_config, "anomaly.toml": load_anomaly_config}
    thresholds = [
        {
            "name": name,
            **_file(root, name, partial(loaders[name], root)),
            "affects": editor.AFFECTS[name],
        }
        for name in editor.THRESHOLD_FILES
    ]

    units = {
        "source": editor.UNITS_FILE,
        "default": config.is_default(root, editor.UNITS_FILE),
        "sha256": editor.file_sha(root, editor.UNITS_FILE),
        "columns": list(editor.UNIT_COLUMNS),
        "rows": editor.load_units(root),
        "error": None,
        "affects": editor.AFFECTS["units"],
    }
    from flashpoint.theme import PRESET_NAMES, THEME_FILE, load_theme

    theme = _file(root, THEME_FILE, lambda: load_theme(root))
    theme["presets"] = list(PRESET_NAMES)
    theme["affects"] = editor.AFFECTS[THEME_FILE]
    return {
        "team": team,
        "robots": robots,
        "thresholds": thresholds,
        "units": units,
        "theme": theme,
    }


def _object(body: Any, key: str, kind: type) -> Any:
    if not isinstance(body, dict):
        raise SaveError("expected a JSON object")
    value = body.get(key)
    if not isinstance(value, kind):
        raise SaveError(f"expected {key!r} as a JSON {'object' if kind is dict else 'array'}")
    return value


def _sha(body: Any) -> str | None:
    value = body.get("sha256") if isinstance(body, dict) else None
    return value if isinstance(value, str) else None


def save_section(root: Path, section: str, body: Any) -> Response:
    """Dispatch a `PUT /api/config/<section>`; the status and payload the API returns."""
    kind, _, name = section.partition("/")
    try:
        if kind == "team" and not name:
            result = editor.save_team(root, _object(body, "values", dict), _sha(body))
        elif kind == "theme" and not name:
            result = editor.save_theme(root, _object(body, "values", dict), _sha(body))
        elif kind == "units" and not name:
            result = editor.save_units(root, _object(body, "rows", list), _sha(body))
        elif kind == "thresholds" and name:
            if name not in editor.THRESHOLD_FILES:
                return 404, {"saved": False, "reason": f"no such threshold file: {name}"}
            result = editor.save_thresholds(root, name, _object(body, "tables", dict), _sha(body))
        elif kind == "robots" and name:
            robot = _object(body, "robot", dict)
            new = bool(body.get("new"))
            result = editor.save_robot(root, name, robot, _sha(body), new=new)
        else:
            return 404, {"saved": False, "reason": f"no such section: {section}"}
    except SaveError as exc:
        return exc.status, {"saved": False, "reason": str(exc)}
    return 200, {
        "saved": True,
        "path": result.path,
        "sha256": result.sha256,
        "affects": result.affects,
        "backup": str(result.backup) if result.backup else None,
    }


def preview_theme(body: Any) -> Response:
    """The derived tokens and every readability rule a palette breaks, with the nearest
    passing colours; the same checks as a save, but nothing is written."""
    from flashpoint.theme import THEME_FILE, ThemeError, parse_theme_config, resolve_palette

    if not isinstance(body, dict):
        return 400, {"ok": False, "reason": "expected a JSON object"}
    try:
        theme_config = parse_theme_config(body)
    except ThemeError as exc:
        return 400, {"ok": False, "reason": f"{THEME_FILE}: {describe_error(exc)}"}
    result = resolve_palette(theme_config)
    return 200, {
        "ok": result.ok,
        "tokens": result.tokens,
        "failures": [asdict(f) for f in result.failures],
        "suggestion": result.suggestion,
        "message": result.message,
    }
