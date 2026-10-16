"""Shared helpers for the end-to-end and budget tests of the acquisition service."""

import hashlib
import os
from pathlib import Path

from flashpoint.lake.ledger import Ledger
from tests.wpilog_builder import WpilogBuilder

S = 10**9


def synthetic_wpilog(records: int = 2) -> bytes:
    """A small valid wpilog: an event name and `records` samples of one double."""
    b = (
        WpilogBuilder()
        .start(1, "NT:/FMSInfo/EventName", "string")
        .start(2, "motor/current", "double")
    )
    b.string(1, 10, "GACMP")
    for i in range(records):
        b.double(2, 20 + i, float(i % 50))
    return b.raw_bytes()


def put_robot_file(root: Path, relpath: str, data: bytes | Path, mtime_s: int) -> Path:
    """Create a file under the fake robot's root with a fixed mtime (seconds)."""
    target = root / relpath.lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, Path):
        target.write_bytes(data.read_bytes())
    else:
        target.write_bytes(data)
    os.utime(target, ns=(mtime_s * S, mtime_s * S))
    return target


def set_dir_mtime(path: Path, mtime_s: int) -> None:
    os.utime(path, ns=(mtime_s * S, mtime_s * S))


def tree(root: Path) -> dict[str, tuple[int, int, str]]:
    """Every file under `root` with its size, mtime and hash."""
    return {
        p.relative_to(root).as_posix(): (
            p.stat().st_size,
            p.stat().st_mtime_ns,
            hashlib.sha256(p.read_bytes()).hexdigest(),
        )
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def ledger_rows(db: Path, sql: str) -> list[dict[str, object]]:
    ledger = Ledger(db)
    try:
        return ledger.query(sql)
    finally:
        ledger.close()
