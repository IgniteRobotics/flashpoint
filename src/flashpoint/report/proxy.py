"""The battery proxy: what stands in for the battery voltage, which the logs don't carry.

It is the supply voltage of the robot's declared proxy device (a low-current device on the
main power bus, such as the gyro). When the robot declares none, or that device logged no supply
voltage in the window, it is the median supply voltage across the devices in each 20 ms bin,
over bins with at least 3 devices. Replay's battery track, its power markers, and detection's
brownout count all read this one series.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

MEDIAN_BIN_US = 20_000
MEDIAN_MIN_DEVICES = 3
# Part of the brownout cache's thresholds hash: changing the fallback must recount.
FALLBACK_RULE = f"median/{MEDIAN_BIN_US}us/{MEDIAN_MIN_DEVICES}"


@dataclass(frozen=True)
class Proxy:
    slot: str | None  # the declared slot, or None for the median fallback
    devices: int  # devices the median is taken over (1 for a declared slot)

    @property
    def label(self) -> str:
        return self.slot if self.slot is not None else f"median of {self.devices} devices"


def _source(path: Path) -> str:
    return "read_parquet('" + str(path / "*.parquet").replace("'", "''") + "')"


def resolve(
    con: duckdb.DuckDBPyConnection, silver: Path, start_us: int, end_us: int, declared: str | None
) -> Proxy | None:
    """The proxy for [start, end): the declared slot if it logged supply voltage there, else the
    median fallback; None when fewer than 3 devices logged supply voltage."""
    found = con.execute(
        f"SELECT slot_id FROM {_source(silver)}"  # noqa: S608 - internal path
        " WHERE metric = 'supply_voltage' AND t_us >= ? AND t_us < ? GROUP BY slot_id",
        [start_us, end_us],
    ).fetchall()
    slots = {s for (s,) in found}
    if declared is not None and declared in slots:
        return Proxy(declared, 1)
    return Proxy(None, len(slots)) if len(slots) >= MEDIAN_MIN_DEVICES else None


def series_sql(proxy: Proxy, silver: Path, start_us: int, end_us: int) -> tuple[str, list[Any]]:
    """A query of the proxy series (t_us, value) over [start, end), and its parameters."""
    if proxy.slot is not None:
        return (
            f"SELECT t_us, value FROM {_source(silver)}"  # noqa: S608 - internal path
            " WHERE metric = 'supply_voltage' AND slot_id = ? AND t_us >= ? AND t_us < ?",
            [proxy.slot, start_us, end_us],
        )
    # Each device's mean in the bin, then the median across devices.
    return (
        f"""
        SELECT ? + bin * {MEDIAN_BIN_US} AS t_us, median(value) AS value FROM (
            SELECT (t_us - ?) // {MEDIAN_BIN_US} AS bin, slot_id, avg(value) AS value
            FROM {_source(silver)}
            WHERE metric = 'supply_voltage' AND t_us >= ? AND t_us < ?
            GROUP BY ALL
        ) GROUP BY bin HAVING count(*) >= {MEDIAN_MIN_DEVICES}
        """,  # noqa: S608 - internal path and constants
        [start_us, start_us, start_us, end_us],
    )
