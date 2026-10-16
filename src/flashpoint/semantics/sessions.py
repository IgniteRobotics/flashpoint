"""Group one robot power-on's logs: a wpilog plus every hoot group that overlaps it."""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

# A hoot group joins a wpilog only if it starts within the wpilog's span (it may start a little
# before the wpilog is renamed with its UTC time). Corpus E10: the hoot logger restarted 65 s
# after the wpilog ended; a loose tolerance wrongly joined them.
EARLY_START = timedelta(seconds=60)


@dataclass(frozen=True)
class WpilogLog:
    log_id: str
    utc_start: datetime | None
    utc_end: datetime | None
    event: str | None = None
    match: str | None = None  # filename label, e.g. "Q7"


@dataclass(frozen=True)
class HootLog:
    log_id: str
    stamp: str  # filename session stamp, YYYY-MM-DD_HH-MM-SS (UTC)
    bus: str
    event: str | None = None
    match: str | None = None

    @property
    def start(self) -> datetime:
        return datetime.strptime(self.stamp, "%Y-%m-%d_%H-%M-%S").replace(tzinfo=UTC)


@dataclass
class HootGroup:
    """Hoots started together (one per CAN bus); they share one clock."""

    stamp: str
    buses: dict[str, str] = field(default_factory=dict)  # log_id -> bus
    event: str | None = None
    match: str | None = None

    @property
    def log_ids(self) -> list[str]:
        return list(self.buses)

    @property
    def start(self) -> datetime:
        return datetime.strptime(self.stamp, "%Y-%m-%d_%H-%M-%S").replace(tzinfo=UTC)


@dataclass
class Session:
    session_id: str
    wpilog_id: str | None
    hoot_groups: list[HootGroup] = field(default_factory=list)


def _same(a: str | None, b: str | None) -> bool:
    return a is None or b is None or a.lower() == b.lower()


def group_sessions(wpilogs: list[WpilogLog], hoots: list[HootLog]) -> list[Session]:
    groups: dict[str, HootGroup] = {}
    for h in sorted(hoots, key=lambda h: (h.stamp, h.log_id)):
        group = groups.setdefault(h.stamp, HootGroup(h.stamp, event=h.event, match=h.match))
        group.buses[h.log_id] = h.bus
    sessions = {w.log_id: Session(w.log_id, w.log_id) for w in wpilogs}
    for group in groups.values():
        candidates = [
            w
            for w in wpilogs
            if w.utc_start is not None
            and w.utc_end is not None
            and w.utc_start - EARLY_START <= group.start <= w.utc_end
            and _same(w.event, group.event)
            and _same(w.match, group.match)
        ]
        if candidates:
            best = min(candidates, key=lambda w: abs((group.start - w.utc_start).total_seconds()))  # type: ignore[operator]
            sessions[best.log_id].hoot_groups.append(group)
        else:
            session_id = f"hoot-{group.stamp}-{group.log_ids[0][:12]}"
            sessions[session_id] = Session(session_id, None, [group])
    for session in sessions.values():
        session.hoot_groups.sort(key=lambda g: g.stamp)
    return sorted(sessions.values(), key=lambda s: s.session_id)
