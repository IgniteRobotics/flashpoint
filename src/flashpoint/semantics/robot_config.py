"""Robot configuration: which device fills which role (slot) on which robot, plus the
NetworkTables signals it publishes, named relative to its season's roots."""

import re
import tomllib
from datetime import date
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

CANIVORE_ID = re.compile(r"^[0-9A-Fa-f]{32}$")
SNAKE_CASE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$")


class ConfigError(Exception):
    pass


class Slot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str
    bus: str  # "rio", "canivore" (any CANivore), or a specific 32-hex CANivore id
    model: str
    can_id: int = Field(ge=0, le=62)
    subsystem: str
    role: str
    gear_ratio: float | None = None
    swaps: list[date] = Field(default_factory=list)

    @field_validator("bus")
    @classmethod
    def _bus(cls, value: str) -> str:
        if value in ("rio", "canivore") or CANIVORE_ID.match(value):
            return value
        raise ValueError("bus must be 'rio', 'canivore', or a 32-hex CANivore id")

    def matches_bus(self, bus: str) -> bool:
        """`canivore` matches any non-rio bus: hoot files name it by its hex id, while the
        diagnostics server (CAN inventory) reports its configured name, e.g. "DriveTrain"."""
        if self.bus == "canivore":
            return bus.lower() != "rio"
        return self.bus.lower() == bus.lower()


class SeasonRoots(BaseModel):
    """Every root is spelled out in full: `NT:Robot/...` and `NT:/photonvision/` really differ."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    robot: str = Field(min_length=1)
    photonvision: str = Field(min_length=1)
    camera_publisher: str = Field(min_length=1)
    preferences: str = Field(min_length=1)
    fms: str = Field(min_length=1)


class SeasonConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    season: int
    roots: SeasonRoots

    def root_names(self) -> dict[str, str]:
        return self.roots.model_dump()


class Signal(BaseModel):
    """A NetworkTables entry, named relative to a season root, with stable labels."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(min_length=1)
    root: str
    entry: str = Field(min_length=1)
    subsystem: str = Field(min_length=1)
    component: str | None = None
    metric: str

    @field_validator("metric")
    @classmethod
    def _metric(cls, value: str) -> str:
        if SNAKE_CASE.match(value):
            return value
        raise ValueError(f"metric {value!r} must be snake_case")


class RobotConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    robot: str
    season: int
    project: str
    slots: list[Slot] = Field(alias="slot")
    signals: list[Signal] = Field(default_factory=list, alias="signal")

    @model_validator(mode="after")
    def _unique_signals(self) -> "RobotConfig":
        ids: dict[str, Signal] = {}
        names: dict[tuple[str, str], Signal] = {}
        for signal in self.signals:
            if signal.id in ids:
                raise ValueError(
                    f"duplicate signal id {signal.id!r}: entries"
                    f" {ids[signal.id].entry!r} and {signal.entry!r}"
                )
            key = (signal.root, signal.entry)
            if key in names:
                raise ValueError(f"signals {names[key].id!r} and {signal.id!r} both map {key}")
            ids[signal.id] = signal
            names[key] = signal
        return self

    def signal_names(self, season: SeasonConfig) -> dict[str, Signal]:
        """Full wpilog entry name (season root + entry, exact concatenation) -> signal."""
        roots = season.root_names()
        return {roots[s.root] + s.entry: s for s in self.signals}

    @model_validator(mode="after")
    def _unique_slots(self) -> "RobotConfig":
        seen: dict[tuple[str, str, int], str] = {}
        ids: set[str] = set()
        for slot in self.slots:
            key = (slot.bus.lower(), slot.model, slot.can_id)
            if key in seen:
                raise ValueError(
                    f"slots {seen[key]!r} and {slot.id!r} declare the same device {key}"
                )
            if slot.id in ids:
                raise ValueError(f"duplicate slot id {slot.id!r}")
            seen[key] = slot.id
            ids.add(slot.id)
        return self

    def slot_for(self, bus: str, model: str, can_id: int) -> Slot | None:
        model = model.replace(" ", "")  # inventory reports "Talon FX"; hoot says "TalonFX"
        for slot in self.slots:
            if slot.model == model and slot.can_id == can_id and slot.matches_bus(bus):
                return slot
        return None


def load_season(path: Path) -> SeasonConfig:
    try:
        with path.open("rb") as f:
            season = SeasonConfig.model_validate(tomllib.load(f))
    except (tomllib.TOMLDecodeError, ValidationError) as exc:
        raise ConfigError(f"{path.name}: {exc}") from exc
    if path.stem != str(season.season):
        raise ConfigError(f"{path.name}: season {season.season} does not match the file name")
    return season


def load_seasons(directory: Path) -> dict[int, SeasonConfig]:
    """Season configs by year, from `config/seasons/<year>.toml` (empty if the dir is absent)."""
    seasons = [load_season(path) for path in sorted(directory.glob("*.toml"))]
    return {s.season: s for s in seasons}


def load_robot(path: Path, seasons: dict[int, SeasonConfig] | None = None) -> RobotConfig:
    """Load and validate one robot; signals are checked against `seasons` (default: the
    `seasons/` directory next to the robot's directory)."""
    if seasons is None:
        seasons = load_seasons(path.parent.parent / "seasons")
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
        robot = RobotConfig.model_validate(data)
    except (tomllib.TOMLDecodeError, ValidationError) as exc:
        raise ConfigError(f"{path.name}: {exc}") from exc
    if robot.signals:
        season = seasons.get(robot.season)
        if season is None:
            raise ConfigError(
                f"{path.name}: declares signals, but season {robot.season} has no"
                f" seasons/{robot.season}.toml"
            )
        for signal in robot.signals:
            if signal.root not in season.root_names():
                raise ConfigError(
                    f"{path.name}: signal {signal.id!r} names root {signal.root!r},"
                    f" which season {robot.season} does not define"
                )
    return robot


def load_robots(
    directory: Path, seasons: dict[int, SeasonConfig] | None = None
) -> list[RobotConfig]:
    if seasons is None:
        seasons = load_seasons(directory.parent / "seasons")
    return [load_robot(path, seasons) for path in sorted(directory.glob("*.toml"))]


def select_robot(robots: list[RobotConfig], project: str | None, season: str) -> RobotConfig | None:
    """The robot whose code project and season match a log (None if no unique match).

    Without a project (hoot-only sessions have no wpilog), the season's only robot is used.
    """
    in_season = [r for r in robots if str(r.season) == season]
    if project is None:
        return in_season[0] if len(in_season) == 1 else None
    matches = [r for r in in_season if r.project == project]
    return matches[0] if len(matches) == 1 else None
