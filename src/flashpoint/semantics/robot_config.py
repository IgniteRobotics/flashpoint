"""Robot configuration: which device fills which role (slot) on which robot."""

import re
import tomllib
from datetime import date
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

CANIVORE_ID = re.compile(r"^[0-9A-Fa-f]{32}$")


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


class RobotConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    robot: str
    season: int
    project: str
    slots: list[Slot] = Field(alias="slot")

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


def load_robot(path: Path) -> RobotConfig:
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
        return RobotConfig.model_validate(data)
    except (tomllib.TOMLDecodeError, ValidationError) as exc:
        raise ConfigError(f"{path.name}: {exc}") from exc


def load_robots(directory: Path) -> list[RobotConfig]:
    return [load_robot(path) for path in sorted(directory.glob("*.toml"))]


def select_robot(robots: list[RobotConfig], project: str | None, season: str) -> RobotConfig | None:
    """The robot whose code project and season match a log (None if no unique match).

    Without a project (hoot-only sessions have no wpilog), the season's only robot is used.
    """
    in_season = [r for r in robots if str(r.season) == season]
    if project is None:
        return in_season[0] if len(in_season) == 1 else None
    matches = [r for r in in_season if r.project == project]
    return matches[0] if len(matches) == 1 else None
