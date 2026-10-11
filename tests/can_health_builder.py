"""Synthetic hoot bronze for CAN-health tests: per-device frame clocks, robot enable, and reboot
signals, written where derive reads a hoot's bronze partition."""

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from flashpoint.lake.bronze import EMPTY_SCHEMA
from flashpoint.lake.paths import LakePaths

S = 1_000_000


@dataclass
class Device:
    name: str  # e.g. "TalonFX-2"
    period_ms: float = 8.0
    silences: list[tuple[float, float]] = field(default_factory=list)  # (start_s, duration_s)
    stop_s: float | None = None  # frames stop for good at this time
    intervals_ms: list[float] | None = None  # cycled instead of a fixed period (jitter)
    reset_counts: list[tuple[float, int]] = field(default_factory=list)  # (t_s, count)
    boot_during_enable: list[tuple[float, float]] = field(default_factory=list)  # (t_s, 0/1)


@dataclass
class Hoot:
    devices: list[Device]
    duration_s: float = 30.0
    enabled: list[tuple[float, float]] = field(default_factory=list)  # (on_s, off_s)
    enable_period_ms: float = 45.0

    def frame_times(self, device: Device) -> np.ndarray:
        """Frame times (µs) of one device: its clock, minus silences and anything after stop."""
        end_s = self.duration_s if device.stop_s is None else device.stop_s
        steps = np.array(device.intervals_ms or [device.period_ms]) * 1000
        n = int(end_s * S / steps.mean()) + len(steps) + 1
        frames = np.concatenate([[0], np.cumsum(np.resize(steps, n))])
        frames = np.round(frames[frames <= end_s * S]).astype(np.int64)
        keep = np.ones(len(frames), bool)
        for start, duration in device.silences:
            keep &= ~((frames > round(start * S)) & (frames < round((start + duration) * S)))
        return frames[keep]

    def enabled_at(self, ts_us: np.ndarray) -> np.ndarray:
        on = np.zeros(len(ts_us), bool)
        for start, end in self.enabled:
            on |= (ts_us >= start * S) & (ts_us < end * S)
        return on

    def table(self) -> pa.Table:
        parts: list[pa.Table] = []

        def add(
            name: str,
            type_: str,
            ts: np.ndarray,
            f64: np.ndarray | None = None,
            i64: np.ndarray | None = None,
            flag: np.ndarray | None = None,
        ) -> None:
            n = len(ts)
            parts.append(
                pa.table(
                    {
                        "signal": pa.array([name] * n, pa.string()),
                        "type": pa.array([type_] * n, pa.string()),
                        "ts_us": pa.array(ts, pa.int64()),
                        "v_f64": pa.array(f64, pa.float64()) if f64 is not None
                        else pa.nulls(n, pa.float64()),
                        "v_i64": pa.array(i64, pa.int64()) if i64 is not None
                        else pa.nulls(n, pa.int64()),
                        "v_bool": pa.array(flag, pa.bool_()) if flag is not None
                        else pa.nulls(n, pa.bool_()),
                    }
                )
            )  # fmt: skip

        enable_ts = np.arange(0, self.duration_s * S, self.enable_period_ms * 1000).astype(np.int64)
        add("RobotEnable", "boolean", enable_ts, flag=self.enabled_at(enable_ts))
        for device in self.devices:
            prefix = f"Phoenix6/{device.name}/"
            frames = self.frame_times(device)
            add(prefix + "Timestamp", "double", frames, f64=frames / S)
            if device.reset_counts:
                t, v = zip(*device.reset_counts, strict=True)
                add(prefix + "ResetCount", "int64", np.round(np.array(t) * S).astype(np.int64),
                    i64=np.array(v))  # fmt: skip
            if device.boot_during_enable:
                t, v = zip(*device.boot_during_enable, strict=True)
                add(prefix + "Fault_BootDuringEnable", "double",
                    np.round(np.array(t) * S).astype(np.int64), f64=np.array(v, float))  # fmt: skip
        table = pa.concat_tables(parts)
        n = table.num_rows
        return pa.table(
            {
                "signal": table["signal"].combine_chunks().dictionary_encode(),
                "type": table["type"].combine_chunks().dictionary_encode(),
                "ts_us": table["ts_us"],
                "v_f64": table["v_f64"],
                "v_i64": table["v_i64"],
                "v_bool": table["v_bool"],
                "v_str": pa.nulls(n, pa.large_string()),
                "v_bytes": pa.nulls(n, pa.large_binary()),
            }
        ).cast(EMPTY_SCHEMA)


def write_hoot(lake: LakePaths, log_id: str, hoot: Hoot, season: str = "2026") -> Path:
    """Write the hoot's bronze partition, as ingest would."""
    path = lake.bronze / f"season={season}" / f"log_id={log_id}"
    path.mkdir(parents=True, exist_ok=True)
    pq.write_table(hoot.table(), path / "part-0.parquet")
    return path


# --- fixtures named in tasks.md 2.2 -------------------------------------------------------


def _rio(count: int = 10, period_ms: float = 8.0) -> list[Device]:
    return [Device(f"TalonFX-{i}", period_ms) for i in range(1, count + 1)]


def q105_like() -> Hoot:
    """Ten rio Talons at 8 ms; TalonFX-2 goes silent for 820 ms, then has rebooted."""
    devices = _rio()
    roller = devices[1]
    roller.silences = [(344.14, 0.82)]
    roller.reset_counts = [(0.42, 0), (344.96, 1)]
    roller.boot_during_enable = [(t / 4, 1.0 if 345.16 <= t / 4 < 345.66 else 0.0)
                                 for t in range(4 * 10, 4 * 359)]  # fmt: skip
    return Hoot(devices, duration_s=360.0, enabled=[(200.0, 215.0), (218.0, 355.0)])


def bus_wide() -> Hoot:
    """Six of ten devices silent over windows that overlap the same 60 ms but start up to
    5 ms apart."""
    devices = _rio()
    for i, device in enumerate(devices[:6]):
        device.silences = [(20.0 + i / 1000, 0.060)]
    return Hoot(devices, duration_s=30.0, enabled=[(6.0, 29.0)])


def whole_bus() -> Hoot:
    """Every device silent for 1.4 s."""
    devices = _rio()
    for device in devices:
        device.silences = [(20.0, 1.4)]
    return Hoot(devices, duration_s=30.0, enabled=[(6.0, 29.0)])


def stops_early() -> Hoot:
    """TalonFX-3 stops 3 s before the enabled window ends; the others keep going."""
    devices = _rio()
    devices[2].stop_s = 22.0
    return Hoot(devices, duration_s=30.0, enabled=[(6.0, 25.0)])


def no_reset_count() -> Hoot:
    """A 2025-style hoot: boot-during-enable, but no reset-count signal."""
    devices = _rio()
    devices[1].boot_during_enable = [(10.0, 0.0), (20.0, 1.0), (20.25, 0.0)]
    return Hoot(devices, duration_s=30.0, enabled=[(6.0, 29.0)])
