"""Match phases (pre, auto, gap, teleop, post) on the wpilog clock."""

import bisect
from dataclasses import dataclass, field

ENABLED_MODES = {"Autonomous": "auto", "Teleop": "teleop"}
ROBOT_ENABLED = {"Autonomous", "Teleop", "Test"}


@dataclass(frozen=True)
class Phase:
    name: str  # pre | auto | gap | teleop | post | test
    start_us: int | None  # None: open-ended
    end_us: int | None


@dataclass(frozen=True)
class Framing:
    phases: list[Phase]
    match_start_us: int | None
    source: str = "none"  # hoot-robot-mode | ds | none
    # Enabled runs [start, end) in time order (end None: still enabled when the log ends).
    # Practice sessions toggle enable many times, so usage reads these, not the phases.
    enabled: list[tuple[int, int | None]] = field(default_factory=list)

    def phase_at(self, ts_us: int) -> str:
        starts = [p.start_us if p.start_us is not None else -(2**63) for p in self.phases]
        return self.phases[bisect.bisect_right(starts, ts_us) - 1].name


def frame(transitions: list[tuple[int, str]], source: str = "none") -> Framing:
    """Phases from (ts, robot mode) samples: "Disabled", "Autonomous", "Teleop", or "Test"."""
    runs: list[tuple[int, str]] = []
    for ts, mode in sorted(transitions):
        if not runs or runs[-1][1] != mode:
            runs.append((ts, mode))
    intervals = _enabled_intervals(runs)
    enabled = [i for i, (_, mode) in enumerate(runs) if mode in ENABLED_MODES]
    if not enabled:
        return Framing([Phase("pre", None, None)], None, source, intervals)
    first, last = enabled[0], enabled[-1]
    phases = [Phase("pre", None, runs[first][0])]
    for i in range(first, len(runs)):
        start, mode = runs[i]
        end = runs[i + 1][0] if i + 1 < len(runs) else None
        if i > last:
            phases.append(Phase("post", start, None))
            break
        name = ENABLED_MODES.get(mode, "test" if mode == "Test" else "gap")
        phases.append(Phase(name, start, end))
    return Framing(phases, runs[first][0], source, intervals)


def _enabled_intervals(runs: list[tuple[int, str]]) -> list[tuple[int, int | None]]:
    intervals: list[tuple[int, int | None]] = []
    start: int | None = None
    for ts, mode in runs:
        if mode in ROBOT_ENABLED and start is None:
            start = ts
        elif mode not in ROBOT_ENABLED and start is not None:
            intervals.append((start, ts))
            start = None
    if start is not None:
        intervals.append((start, None))
    return intervals


def modes_from_ds(
    enabled: list[tuple[int, bool]], autonomous: list[tuple[int, bool]]
) -> list[tuple[int, str]]:
    """Robot mode transitions from the wpilog's DS:enabled and DS:autonomous booleans."""
    events = sorted([(t, "e", v) for t, v in enabled] + [(t, "a", v) for t, v in autonomous])
    state = {"e": False, "a": False}
    modes: list[tuple[int, str]] = []
    for ts, key, value in events:
        state[key] = value
        mode = ("Autonomous" if state["a"] else "Teleop") if state["e"] else "Disabled"
        if modes and modes[-1][0] == ts:
            modes[-1] = (ts, mode)
        elif not modes or modes[-1][1] != mode:
            modes.append((ts, mode))
    return modes
