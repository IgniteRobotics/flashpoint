"""Map live devices to robot-configuration slots: the robot whose `project` matches the published
`Project` (latest season wins), and its bus list to turn robot-code bus names into hoot buses."""

from pathlib import Path

from flashpoint.live.detector import MissingSlot, SlotInfo, SlotResolver
from flashpoint.semantics.robot_config import ConfigError, RobotConfig, load_robots


class RobotSlots:
    def __init__(self, config: RobotConfig) -> None:
        self.config = config
        self.robot = config.robot
        self._hoot = {b.name: b.hoot for b in config.buses}

    def slot_for(self, bus: str, model: str, can_id: int) -> SlotInfo | None:
        # A bus missing from [[bus]] is tried as a hoot bus name ("rio", "canivore").
        slot = self.config.slot_for(self._hoot.get(bus, bus), model, can_id)
        return SlotInfo(slot.id, slot.role, slot.subsystem) if slot else None

    def missing(self, reported: set[tuple[str, str, int]]) -> list[MissingSlot]:
        found = {info.id for key in reported if (info := self.slot_for(*key)) is not None}
        return [
            MissingSlot(s.id, s.role, s.model, s.can_id, self.config.bus_name_for(s.bus) or s.bus)
            for s in self.config.slots
            if s.id not in found
        ]


def resolver_for(robots: list[RobotConfig]) -> SlotResolver:
    def resolve(project: str) -> RobotSlots | None:
        matches = [r for r in robots if r.project == project]
        return RobotSlots(max(matches, key=lambda r: r.season)) if matches else None

    return resolve


def load_resolver(config_root: Path) -> SlotResolver:
    """A resolver over the robots in `config_root`; unreadable robot files map nothing (the
    Setup view and `doctor` report them)."""
    try:
        robots = load_robots(config_root / "robots")
    except ConfigError:
        robots = []
    return resolver_for(robots)
