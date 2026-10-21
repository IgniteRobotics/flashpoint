"""Team-edited catalogues: likely causes (`config/anomaly-causes.toml`) and known failures
(`config/failure-labels.toml`, scored by the backtest)."""

import datetime as dt
import re
import tomllib
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from flashpoint.anomaly.settings import METHODS, METRICS, POWER
from flashpoint.config import config_file
from flashpoint.semantics.robot_config import RobotConfig

CAUSES_FILE = "anomaly-causes.toml"
LABELS_FILE = "failure-labels.toml"
MATCH_KEY = re.compile(r"^\d{4}[a-z0-9]+_(pm|qm|e)\d+(r\d+)?$")


class CatalogueError(Exception):
    pass


class Cause(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    method: str
    metric: str
    subsystem: str | None = None
    text: str = Field(min_length=1, max_length=300)

    @field_validator("method")
    @classmethod
    def _method(cls, value: str) -> str:
        if value not in METHODS:
            raise ValueError(f"method {value!r} is not one of {list(METHODS)}")
        return value

    @field_validator("metric")
    @classmethod
    def _metric(cls, value: str) -> str:
        if value not in METRICS:
            raise ValueError(f"metric {value!r} is not one of {list(METRICS)}")
        return value


class Label(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    robot: str
    unit: str | None = None
    slot: str | None = None
    match: str | None = None
    date: dt.date | None = None
    kind: Literal["failure", "degradation", "repair"]
    note: str
    metric: str | None = None

    @field_validator("match")
    @classmethod
    def _match(cls, value: str | None) -> str | None:
        if value is not None and not MATCH_KEY.match(value):
            raise ValueError(f"match {value!r} is not a match key like 2026gacmp_qm7")
        return value

    @field_validator("metric")
    @classmethod
    def _metric(cls, value: str | None) -> str | None:
        if value is not None and value not in METRICS:
            raise ValueError(f"metric {value!r} is not one of {list(METRICS)}")
        return value


def _read(path: Path, table: str) -> list[Any]:
    if not path.is_file():
        return []
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise CatalogueError(f"{path.name}: {exc}") from exc
    unknown = sorted(set(data) - {table})
    if unknown:
        raise CatalogueError(f"{path.name}: unknown keys {unknown} (expected [[{table}]])")
    items = data.get(table, [])
    if not isinstance(items, list):
        raise CatalogueError(f"{path.name}: {table} must be an array of tables [[{table}]]")
    return items


def load_causes(config_dir: Path) -> list[Cause]:
    path = config_file(config_dir, CAUSES_FILE)
    causes = []
    for n, item in enumerate(_read(path, "cause"), start=1):
        try:
            causes.append(Cause.model_validate(item))
        except ValidationError as exc:
            raise CatalogueError(f"{path.name}: cause {n}: {exc}") from exc
    return causes


def causes_for(causes: list[Cause], method: str, metric: str, subsystem: str | None) -> list[str]:
    """Subsystem-specific entries first, then general ones, in file order."""
    keyed = [c for c in causes if c.method == method and c.metric == metric]
    specific = [c.text for c in keyed if c.subsystem is not None and c.subsystem == subsystem]
    return specific + [c.text for c in keyed if c.subsystem is None]


def load_labels(config_dir: Path, robots: list[RobotConfig]) -> list[Label]:
    path = config_dir / LABELS_FILE
    by_name = {r.robot: r for r in robots}
    labels = []
    for n, item in enumerate(_read(path, "label"), start=1):
        try:
            label = Label.model_validate(item)
        except ValidationError as exc:
            raise CatalogueError(f"{path.name}: label {n}: {exc}") from exc

        def fail(message: str, n: int = n) -> CatalogueError:
            return CatalogueError(f"{path.name}: label {n}: {message}")

        robot = by_name.get(label.robot)
        if robot is None:
            raise fail(f"unknown robot {label.robot!r}")
        if label.unit is None and label.slot is None:
            raise fail("give a unit or a slot")
        if label.slot not in (None, POWER, *(s.id for s in robot.slots)):
            raise fail(f"robot {label.robot!r} has no slot {label.slot!r}")
        if label.match is None and label.date is None:
            raise fail("give a match key or a date")
        labels.append(label)
    return labels
