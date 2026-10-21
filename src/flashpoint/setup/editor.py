"""Configuration saves from the Setup view (ADR-0017).

A save loads the file with tomlkit (the team's copy, else the bundled default), applies the
edit to the document so comments and untouched entries keep their bytes, validates the result
with the same loader the CLI uses, refuses it if the file changed since the view read it, keeps
the previous version in `<root>/.backups/`, and replaces the file atomically.
"""

import contextlib
import csv
import datetime as dt
import hashlib
import io
import os
import re
import shutil
import tempfile
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import tomlkit
from tomlkit.items import AoT, Table

from flashpoint import config
from flashpoint.setup.checks import describe_error

KEEP_BACKUPS = 20
BACKUPS_DIR = ".backups"
UNITS_FILE = "units/units-seed.csv"
UNIT_COLUMNS = (
    "serial",
    "model",
    "robot",
    "bus",
    "can_id",
    "role",
    "label",
    "first_installed",
    "notes",
)
ROBOT_ID = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
THRESHOLD_FILES = ("report.toml", "anomaly.toml")
# What a saved file affects, as the view says it (spec: "What an edit affects").
AFFECTS = {
    "acquire.toml": "a running acquire service picks this up after a restart",
    "report.toml": "flashpoint report",
    "anomaly.toml": "flashpoint detect",
    "robots": "flashpoint derive",
    "units": "nothing until the next derive (flashpoint derive)",
}
# Arrays of tables in a robot file, and the key that identifies each entry.
ROBOT_TABLES = {"slot": "id", "signal": "id", "bus": "name", "siblings": "name"}
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class SaveError(ValueError):
    """A refused save; nothing was written. `status` is the HTTP status the API returns."""

    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class SaveResult:
    path: str  # relative to the configuration root
    sha256: str
    backup: Path | None
    affects: str


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def current_bytes(root: Path, rel: str) -> bytes | None:
    """The file as the view sees it: the team's copy, else the bundled default, else None."""
    path = config.config_file(root, rel)
    return path.read_bytes() if path.is_file() else None


def file_sha(root: Path, rel: str) -> str | None:
    data = current_bytes(root, rel)
    return None if data is None else _sha(data)


def _check_unchanged(root: Path, rel: str, expected: str | None) -> bytes | None:
    data = current_bytes(root, rel)
    if (None if data is None else _sha(data)) != expected:
        raise SaveError(f"{rel} changed on disk since it was loaded; reload and redo the edit", 409)
    return data


def _backup(root: Path, rel: str, now: dt.datetime) -> Path | None:
    path = root / rel
    if not path.is_file():
        return None  # a bundled default or a new file: nothing of the team's is replaced
    folder = root / BACKUPS_DIR / rel
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"{now.strftime('%Y%m%dT%H%M%S.%fZ')}{path.suffix}"
    shutil.copy2(path, target)
    for old in sorted(folder.iterdir())[:-KEEP_BACKUPS]:
        old.unlink()
    return target


def _write(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            f.write(text)
        Path(tmp).replace(path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _validated(rel: str, text: str, validate: Callable[[Path], object]) -> None:
    """Run the loader on the new text, placed at `rel` in a scratch configuration root."""
    with tempfile.TemporaryDirectory(prefix="flashpoint-save-") as scratch:
        target = Path(scratch) / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        try:
            validate(Path(scratch))
        except Exception as exc:  # each loader has its own error type; all become the message
            raise SaveError(f"{rel}: {describe_error(exc)}") from exc


def _save(
    root: Path,
    rel: str,
    expected_sha: str | None,
    render: Callable[[bytes | None], str],
    validate: Callable[[Path], object],
    affects: str,
) -> SaveResult:
    current = _check_unchanged(root, rel, expected_sha)
    text = render(current)
    _validated(rel, text, validate)
    backup = _backup(root, rel, dt.datetime.now(dt.UTC))
    _write(root, rel, text)
    return SaveResult(rel, _sha(text.encode("utf-8")), backup, affects)


# --- tomlkit merging ----------------------------------------------------------------------


def _plain(value: Any) -> Any:
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, Mapping):
        return {k: _plain(v) for k, v in value.items()}
    return value


def _toml_value(key: str, value: Any) -> Any:
    if key == "swaps" and isinstance(value, list):
        return [
            dt.date.fromisoformat(v) if isinstance(v, str) and _DATE.match(v) else v for v in value
        ]
    return value


def _merge_table(table: Any, new: Mapping[str, Any], skip: tuple[str, ...] = ()) -> None:
    """Set changed keys, add new ones, remove absent ones; untouched keys keep their bytes.
    A None value removes the key."""
    for key in [k for k in list(table.keys()) if k not in new and k not in skip]:
        del table[key]
    for key, value in new.items():
        if key in skip:
            continue
        if value is None:
            if key in table:
                del table[key]
        elif isinstance(value, Mapping) and isinstance(table.get(key), Table | dict):
            _merge_table(table[key], value)
        elif key not in table or _plain(table[key]) != _plain(value):
            table[key] = _toml_value(key, value)


def _merge_aot(doc: Any, key: str, id_key: str, items: list[Mapping[str, Any]]) -> None:
    wanted = {str(item.get(id_key)): item for item in items}
    existing = doc.get(key)
    if existing is None:
        if items:
            aot = tomlkit.aot()
            for item in items:
                aot.append(tomlkit.item(dict(item)))
            doc.append(key, aot)
        return
    assert isinstance(existing, AoT)
    seen: set[str] = set()
    for index in reversed(range(len(existing))):
        name = str(existing[index].get(id_key))
        if name in wanted and name not in seen:
            _merge_table(existing[index], wanted[name])
            seen.add(name)
        else:
            del existing[index]
    for name, item in wanted.items():
        if name not in seen:
            existing.append(
                tomlkit.item({k: _toml_value(k, v) for k, v in item.items() if v is not None})
            )
    if not len(existing):
        del doc[key]


def _render(current: bytes | None, edit: Callable[[Any], None]) -> str:
    doc = tomlkit.parse(current.decode("utf-8") if current else "")
    edit(doc)
    return tomlkit.dumps(doc)


# --- sections -----------------------------------------------------------------------------


def save_team(root: Path, values: Mapping[str, Any], expected_sha: str | None) -> SaveResult:
    """Change keys in `acquire.toml` (team, lake, hosts, ...); a None value removes the key."""
    from flashpoint.acquire.config import CONFIG_FILE, AcquireConfig

    def edit(doc: Any) -> None:
        for key, value in values.items():
            if value is None:
                doc.pop(key, None)
            elif isinstance(value, Mapping):
                table = doc.setdefault(key, tomlkit.table())
                _merge_table(table, value)
            elif key not in doc or _plain(doc[key]) != _plain(value):
                doc[key] = value  # an unchanged key keeps its bytes (and inline comment)

    return _save(
        root,
        CONFIG_FILE,
        expected_sha,
        lambda current: _render(current, edit),
        AcquireConfig.load,
        AFFECTS[CONFIG_FILE],
    )


def save_thresholds(
    root: Path, name: str, tables: Mapping[str, Mapping[str, Any]], expected_sha: str | None
) -> SaveResult:
    """Merge `{table: {key: value}}` into report.toml or anomaly.toml."""
    from flashpoint.anomaly.settings import load_anomaly_config
    from flashpoint.report.settings import advantagescope_path, load_report_config

    if name not in THRESHOLD_FILES:
        raise SaveError(f"no such threshold file: {name}", 404)

    def edit(doc: Any) -> None:
        for table_name, values in tables.items():
            table = doc.setdefault(table_name, tomlkit.table())
            for key, value in values.items():
                if value is None:
                    table.pop(key, None)
                elif key not in table or _plain(table[key]) != _plain(value):
                    table[key] = value

    def validate(scratch: Path) -> None:
        if name == "report.toml":
            load_report_config(scratch)
            advantagescope_path(scratch)
        else:
            load_anomaly_config(scratch)

    return _save(
        root, name, expected_sha, lambda current: _render(current, edit), validate, AFFECTS[name]
    )


def robot_file(robot_id: str) -> str:
    if not ROBOT_ID.match(robot_id):
        raise SaveError(f"robot id {robot_id!r} must be lowercase letters, digits and dashes")
    return f"robots/{robot_id}.toml"


def save_robot(
    root: Path,
    robot_id: str,
    data: Mapping[str, Any],
    expected_sha: str | None,
    new: bool = False,
) -> SaveResult:
    """Save a robot (TOML key names: robot, season, project, battery_proxy, slot, signal, bus,
    siblings). Arrays of tables are merged by id, so untouched entries keep their comments."""
    from flashpoint.semantics.robot_config import load_config_seasons, load_robot, load_robots

    rel = robot_file(robot_id)
    if data.get("robot") != robot_id:
        raise SaveError(f"{rel}: robot {data.get('robot')!r} must match the file name")
    if new:
        robots_dir = root / "robots"
        taken = {p.stem for p in robots_dir.glob("*.toml")}
        if robots_dir.is_dir():
            with contextlib.suppress(Exception):  # a broken file still blocks its own name
                taken |= {r.robot for r in load_robots(robots_dir)}
        if robot_id in taken:
            raise SaveError(f"robot {robot_id!r} already exists", 409)
        expected_sha = None

    def edit(doc: Any) -> None:
        _merge_table(doc, data, skip=tuple(ROBOT_TABLES))
        for key, id_key in ROBOT_TABLES.items():
            _merge_aot(doc, key, id_key, list(data.get(key) or []))

    seasons = load_config_seasons(root)
    return _save(
        root,
        rel,
        expected_sha,
        lambda current: _render(current, edit),
        lambda scratch: load_robot(scratch / rel, seasons),
        AFFECTS["robots"],
    )


def _render_units(rows: Sequence[Mapping[str, Any]]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=UNIT_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        unknown = sorted(set(row) - set(UNIT_COLUMNS))
        if unknown:
            raise SaveError(f"{UNITS_FILE}: unknown columns {unknown}")
        writer.writerow({k: "" if row.get(k) is None else str(row.get(k)) for k in UNIT_COLUMNS})
    return out.getvalue()


def _check_units(rows: Sequence[Mapping[str, Any]]) -> None:
    for column, what in (("serial", "serial"), ("label", "label")):
        first: dict[str, int] = {}
        for n, row in enumerate(rows, start=1):
            value = str(row.get(column) or "").strip()
            if not value:
                if column == "serial":
                    raise SaveError(f"{UNITS_FILE}: row {n} has no serial")
                continue
            if value in first:
                raise SaveError(f"{UNITS_FILE}: {what} {value!r} is on rows {first[value]} and {n}")
            first[value] = n


def save_units(
    root: Path, rows: Sequence[Mapping[str, Any]], expected_sha: str | None
) -> SaveResult:
    _check_units(rows)
    text = _render_units(rows)

    def validate(scratch: Path) -> None:
        with (scratch / UNITS_FILE).open(encoding="utf-8", newline="") as f:
            header = next(csv.reader(f))
        if tuple(header) != UNIT_COLUMNS:
            raise ValueError(f"columns {header}")

    return _save(root, UNITS_FILE, expected_sha, lambda current: text, validate, AFFECTS["units"])


def load_units(root: Path) -> list[dict[str, str]]:
    data = current_bytes(root, UNITS_FILE)
    if data is None:
        return []
    return list(csv.DictReader(io.StringIO(data.decode("utf-8"))))


def toml_dict(root: Path, rel: str) -> dict[str, Any]:
    data = current_bytes(root, rel)
    return tomllib.loads(data.decode("utf-8")) if data else {}
