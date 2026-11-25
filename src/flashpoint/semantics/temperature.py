"""Why a device's temperature is missing, decided in one place for gold (History, detection) and
Replay (mvp-outside-team-logs design §4; ADR-0005 amendment).

- `logged`: there is a temperature value.
- `not-tracked`: Flashpoint tracks no temperature for the device model. A CANcoder has no
  temperature signal; a Pigeon 2 has one (its IMU's), but the health profile neither exports
  nor maps it. An unknown model counts as temperature-capable, so it never hides a problem.
- `needs-pro`: a temperature-capable device whose hoots were not Phoenix Pro-licensed. The
  free export has no continuous DeviceTemp.
- `missing`: a temperature-capable device on Pro-licensed (or unknown) hoots, with no value.
"""

from collections.abc import Iterable

NO_TEMPERATURE_MODELS = frozenset({"CANcoder", "Pigeon2"})
LOGGED, NEEDS_PRO, NOT_TRACKED, MISSING = "logged", "needs-pro", "not-tracked", "missing"
TEMP_STATUSES = (LOGGED, NEEDS_PRO, NOT_TRACKED, MISSING)


def tracks_temperature(model: str | None) -> bool:
    return model not in NO_TEMPERATURE_MODELS


def temp_status(model: str | None, pro_licensed: bool | None, has_temperature: bool) -> str:
    if has_temperature:
        return LOGGED
    if not tracks_temperature(model):
        return NOT_TRACKED
    return NEEDS_PRO if pro_licensed is False else MISSING


def pro_by_bus(hoots: Iterable[tuple[str | None, object]]) -> dict[str, bool | None]:
    """Per hoot bus, from (bus, pro_licensed) of a session's hoots: True if any hoot on it is
    Pro-licensed, False if every known one is not, None if none is known."""
    out: dict[str, bool | None] = {}
    for bus, pro in hoots:
        if bus is None:
            continue
        if pro is None:
            out.setdefault(bus, None)
        elif pro:
            out[bus] = True
        else:
            out[bus] = out.get(bus) or False  # a Pro hoot already seen keeps the bus Pro
    return out
