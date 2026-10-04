"""Import ledger: per-file state, aliases, and per-log metadata (SQLite, WAL mode).

The tables are exported to `meta/*.parquet` after each run, so analytics read
everything through DuckDB without extensions.
"""

import sqlite3
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    sha256 TEXT PRIMARY KEY, kind TEXT NOT NULL, size INTEGER NOT NULL,
    stage TEXT NOT NULL, reason TEXT, warnings TEXT, pipeline_version INTEGER,
    first_seen TEXT NOT NULL, updated TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS aliases (
    sha256 TEXT NOT NULL, path TEXT NOT NULL, name TEXT NOT NULL, seen TEXT NOT NULL,
    PRIMARY KEY (sha256, path)
);
CREATE TABLE IF NOT EXISTS logs (
    log_id TEXT PRIMARY KEY, kind TEXT NOT NULL, season TEXT NOT NULL, filename TEXT,
    fms_event TEXT, fms_match_type INTEGER, fms_match_number INTEGER, fms_replay INTEGER,
    fms_red_alliance INTEGER, fms_station INTEGER,
    file_event TEXT, file_match_type TEXT, file_match_number INTEGER,
    project TEXT, build_date TEXT, commit_hash TEXT, git_branch TEXT, git_dirty TEXT,
    utc_start TEXT, utc_end TEXT, anchor_source TEXT, utc_offset_us INTEGER,
    first_ts_us INTEGER, last_ts_us INTEGER, record_count INTEGER,
    truncated_bytes INTEGER, orphan_records INTEGER, inventory TEXT
);
CREATE TABLE IF NOT EXISTS hoot_logs (
    log_id TEXT PRIMARY KEY, bus TEXT, bus_description TEXT, session_stamp TEXT,
    compliancy INTEGER, owlet_version TEXT, pro_licensed INTEGER, profile TEXT,
    signal_count INTEGER, read_status TEXT, integrity TEXT
);
CREATE TABLE IF NOT EXISTS entries (
    log_id TEXT NOT NULL, idx INTEGER NOT NULL, entry_id INTEGER NOT NULL, name TEXT NOT NULL,
    type TEXT NOT NULL, metadata TEXT, PRIMARY KEY (log_id, idx)
);
CREATE TABLE IF NOT EXISTS inventory (
    log_id TEXT NOT NULL, ts_us INTEGER NOT NULL, payload TEXT NOT NULL,
    valid INTEGER NOT NULL, error TEXT
);
"""
_SQL_TO_ARROW = {"TEXT": pa.string(), "INTEGER": pa.int64(), "REAL": pa.float64()}
METADATA_TABLES = ("logs", "hoot_logs", "entries", "inventory")


class Stage(StrEnum):
    RECEIVED = "received"
    BRONZE = "bronze"
    SUCCESS = "success"
    QUARANTINED = "quarantined"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class Ledger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.executescript(SCHEMA)

    def close(self) -> None:
        self._db.close()

    def register(self, sha256: str, kind: str, size: int, path: Path) -> bool:
        """Record a sighting of a file. Returns True if the hash is new."""
        now = _now()
        with self._db:
            new = (
                self._db.execute(
                    "INSERT OR IGNORE INTO files (sha256, kind, size, stage, first_seen, updated)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (sha256, kind, size, Stage.RECEIVED, now, now),
                ).rowcount
                == 1
            )
            self._db.execute(
                "INSERT OR IGNORE INTO aliases (sha256, path, name, seen) VALUES (?, ?, ?, ?)",
                (sha256, str(path), path.name, now),
            )
        return new

    def aliases(self, sha256: str) -> list[str]:
        rows = self._db.execute("SELECT path FROM aliases WHERE sha256 = ?", (sha256,))
        return [r[0] for r in rows]

    def stage(self, sha256: str) -> Stage | None:
        row = self._db.execute("SELECT stage FROM files WHERE sha256 = ?", (sha256,)).fetchone()
        return Stage(row[0]) if row else None

    def reason(self, sha256: str) -> str | None:
        row = self._db.execute("SELECT reason FROM files WHERE sha256 = ?", (sha256,)).fetchone()
        return row[0] if row else None

    def needs_processing(self, sha256: str, pipeline_version: int) -> bool:
        row = self._db.execute(
            "SELECT stage, pipeline_version FROM files WHERE sha256 = ?", (sha256,)
        ).fetchone()
        if row is None:
            return True
        stage, version = row
        return not (stage in (Stage.SUCCESS, Stage.QUARANTINED) and version == pipeline_version)

    def set_stage(
        self, sha256: str, stage: Stage, pipeline_version: int, warnings: str | None = None
    ) -> None:
        with self._db:
            self._db.execute(
                "UPDATE files SET stage = ?, reason = NULL, warnings = ?, pipeline_version = ?,"
                " updated = ? WHERE sha256 = ?",
                (stage, warnings, pipeline_version, _now(), sha256),
            )

    def quarantine(self, sha256: str, reason: str, pipeline_version: int) -> None:
        with self._db:
            self._db.execute(
                "UPDATE files SET stage = ?, reason = ?, pipeline_version = ?, updated = ?"
                " WHERE sha256 = ?",
                (Stage.QUARANTINED, reason, pipeline_version, _now(), sha256),
            )

    def files(self, stage: Stage | None = None) -> list[dict[str, Any]]:
        query, args = "SELECT * FROM files", tuple[Any, ...]()
        if stage is not None:
            query, args = query + " WHERE stage = ?", (stage,)
        cursor = self._db.execute(query, args)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row, strict=True)) for row in cursor]

    def replace_metadata(self, log_id: str, rows: dict[str, list[dict[str, Any]]]) -> None:
        """Atomically replace every metadata row for a log (table name -> rows)."""
        with self._db:
            for table in METADATA_TABLES:
                self._db.execute(f"DELETE FROM {table} WHERE log_id = ?", (log_id,))  # noqa: S608
            for table, table_rows in rows.items():
                if table not in METADATA_TABLES:
                    raise ValueError(f"unknown metadata table {table!r}")
                for row in table_rows:
                    cols = ", ".join(row)
                    marks = ", ".join("?" for _ in row)
                    self._db.execute(
                        f"INSERT INTO {table} ({cols}) VALUES ({marks})",  # noqa: S608
                        tuple(row.values()),
                    )

    def execute(self, sql: str, args: tuple[Any, ...] = ()) -> None:
        with self._db:
            self._db.execute(sql, args)

    def query(self, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        cursor = self._db.execute(sql, args)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row, strict=True)) for row in cursor]

    def export_snapshots(self, meta_dir: Path) -> None:
        meta_dir.mkdir(parents=True, exist_ok=True)
        tables = [
            r[0] for r in self._db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        ]
        for table in tables:
            info = self._db.execute(f"PRAGMA table_info({table})").fetchall()
            schema = pa.schema([(c[1], _SQL_TO_ARROW.get(c[2], pa.string())) for c in info])
            rows = self.query(f"SELECT * FROM {table}")  # noqa: S608
            tmp = meta_dir / f"{table}.parquet.part"
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), tmp)
            tmp.replace(meta_dir / f"{table}.parquet")
