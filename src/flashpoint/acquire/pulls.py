"""Pull ledger: one row per source file seen, in the lake's SQLite ledger.

Rows are keyed by (source_id, remote_path, size, mtime_ns), so a file that grows or is
rewritten is a new entry. The watch process owns this connection while the ingest
subprocess writes the other ledger tables, hence the long busy timeout.
"""

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

MAX_ATTEMPTS = 3
BUSY_TIMEOUT_MS = 30_000

SCHEMA = """
CREATE TABLE IF NOT EXISTS pulls (
    source_kind TEXT NOT NULL, source_id TEXT NOT NULL, remote_path TEXT NOT NULL,
    size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, sha256 TEXT, status TEXT NOT NULL,
    attempts INTEGER NOT NULL, reason TEXT, include_active INTEGER NOT NULL,
    first_seen TEXT NOT NULL, pulled_at TEXT,
    PRIMARY KEY (source_id, remote_path, size, mtime_ns)
);
"""


class PullStatus(StrEnum):
    PENDING = "pending"
    VERIFIED = "verified"
    SIZE_VERIFIED = "size-verified"
    FAILED = "failed"


_DONE = (PullStatus.VERIFIED, PullStatus.SIZE_VERIFIED)


@dataclass(frozen=True)
class PullKey:
    source_kind: str  # "robot" or "volume"
    source_id: str  # host for robots, volume UUID for sticks
    remote_path: str
    size: int
    mtime_ns: int

    @property
    def _identity(self) -> tuple[str, str, int, int]:
        return (self.source_id, self.remote_path, self.size, self.mtime_ns)


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class PullLedger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, isolation_level=None, timeout=BUSY_TIMEOUT_MS / 1000)
        self._db.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        self._db.close()

    def get(self, key: PullKey) -> dict[str, Any] | None:
        rows = self.query(
            "SELECT * FROM pulls WHERE source_id = ? AND remote_path = ? AND size = ?"
            " AND mtime_ns = ?",
            key._identity,
        )
        return rows[0] if rows else None

    def is_pulled(self, key: PullKey) -> bool:
        """True if this exact source file already has a `verified` or `size-verified` pull."""
        row = self.get(key)
        return row is not None and row["status"] in _DONE

    def should_pull(self, key: PullKey) -> bool:
        """False once the file is pulled, or has failed for good (no automatic retries)."""
        row = self.get(key)
        return row is None or row["status"] == PullStatus.PENDING

    def record_failure(self, key: PullKey, reason: str) -> PullStatus:
        """Count a failed attempt. The third one makes the pull `failed`.

        A `verified` or `size-verified` row is left as it is; its status is returned.
        """
        now = _now()
        with self._db:
            self._db.execute(
                "INSERT INTO pulls (source_kind, source_id, remote_path, size, mtime_ns, status,"
                " attempts, reason, include_active, first_seen) VALUES (?, ?, ?, ?, ?, ?, 0, ?,"
                " 0, ?) ON CONFLICT DO NOTHING",
                (key.source_kind, *key._identity, PullStatus.PENDING, reason, now),
            )
            self._db.execute(
                "UPDATE pulls SET attempts = attempts + 1, reason = ?,"
                " status = CASE WHEN attempts + 1 >= ? THEN ? ELSE ? END"
                " WHERE source_id = ? AND remote_path = ? AND size = ? AND mtime_ns = ?"
                " AND status NOT IN (?, ?)",  # never demote a finished pull
                (
                    reason,
                    MAX_ATTEMPTS,
                    PullStatus.FAILED,
                    PullStatus.PENDING,
                    *key._identity,
                    *_DONE,
                ),
            )
        row = self.get(key)
        assert row is not None
        return PullStatus(row["status"])

    def record_success(
        self, key: PullKey, sha256: str, status: PullStatus, include_active: bool
    ) -> None:
        if status not in _DONE:
            raise ValueError(f"not a success status: {status!r}")
        now = _now()
        with self._db:
            self._db.execute(
                "INSERT INTO pulls (source_kind, source_id, remote_path, size, mtime_ns, sha256,"
                " status, attempts, reason, include_active, first_seen, pulled_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?, ?)"
                " ON CONFLICT (source_id, remote_path, size, mtime_ns) DO UPDATE SET"
                " sha256 = excluded.sha256, status = excluded.status, reason = NULL,"
                " include_active = excluded.include_active, pulled_at = excluded.pulled_at",
                (
                    key.source_kind,
                    *key._identity,
                    sha256,
                    status,
                    int(include_active),
                    now,
                    now,
                ),
            )

    def failed(self) -> list[dict[str, Any]]:
        """Pulls that gave up after the last attempt, for the health report."""
        return self.query(
            "SELECT * FROM pulls WHERE status = ? ORDER BY first_seen", (PullStatus.FAILED,)
        )

    def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        cursor = self._db.execute(sql, args)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row, strict=True)) for row in cursor]
