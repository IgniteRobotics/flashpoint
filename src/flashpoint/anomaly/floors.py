"""CAN-gap spread floors scaled by frame rate (anomaly-detection, "CAN-gap floors scale with
frame rate"; ADR-0010 amendment).

A device at a roboRIO-bus status rate (at most `CAN_FLOOR_RIO_MAX_HZ`) keeps the configured
floors. Above that, the floors scale with rate ÷ `CAN_FLOOR_REFERENCE_HZ`, so timing jitter on a
250 Hz CANivore device (5 times) stays inside the band while a real dropout still stands out. These
are constants, not configuration keys: a team can still raise `floors` in anomaly.toml.
"""

import numpy as np

CAN_FLOOR_RIO_MAX_HZ = 125.0
CAN_FLOOR_REFERENCE_HZ = 50.0
RATE_SCALED_METRICS = frozenset({"isolated_gaps", "missed_per_min"})

Floor = float | np.ndarray  # one floor, or one per index of the judged series


def rate_scale(rate_hz: float) -> float:
    return rate_hz / CAN_FLOOR_REFERENCE_HZ if rate_hz > CAN_FLOOR_RIO_MAX_HZ else 1.0


def floors(metric: str, floor: float, rates: np.ndarray | None, window: int) -> Floor:
    """The floor per index of a series whose frame rates are `rates` (NaN: not recorded).

    At index i it is the configured floor scaled by the median rate of index i and up to
    `window` indices before it. A row with no rate keeps the configured floor. Metrics other
    than the two CAN-gap ones, or a series with no rates at all, get the configured floor."""
    if metric not in RATE_SCALED_METRICS or rates is None or np.isnan(rates).all():
        return floor
    out = np.full(rates.size, floor, dtype=float)
    for i in range(rates.size):
        if np.isnan(rates[i]):
            continue
        seen = rates[max(0, i - window) : i + 1]
        out[i] = floor * rate_scale(float(np.median(seen[~np.isnan(seen)])))
    return out


def floor_at(floor: Floor, i: int) -> float:
    return float(floor[i]) if isinstance(floor, np.ndarray) else floor
