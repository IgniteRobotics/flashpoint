"""A synthetic lake for Replay builds: meta snapshots, silver, gold, and raw files."""

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from flashpoint.lake.paths import LakePaths
from flashpoint.lake.raw import raw_path
from flashpoint.semantics.silver import silver_dir
from tests.report.silver import S, points, series, write_silver

COMP_SLOTS = ("drive-fl", "hood", "intake-roller")


def quiet_rows(start_s: float = 0, end_s: float = 180, hz: float = 50) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for i, slot in enumerate(COMP_SLOTS):
        rows += series(slot, "supply_voltage", start_s, end_s, hz, 12.0)
        rows += series(slot, "supply_current", start_s, end_s, hz, 10.0 + i)
        rows += series(slot, "stator_current", start_s, end_s, hz, 20.0 + i)
        rows += series(slot, "rotor_velocity_rps", start_s, end_s, hz, 40.0)
        rows += series(slot, "motor_voltage", start_s, end_s, hz, 6.0)
        rows += series(slot, "stall_current", start_s, start_s + 1, 1, 300.0)
        rows += points(slot, "temp_c", [(start_s, 25.0 + i), (100.0, 35.0 + i * 5)])
    return rows


@dataclass
class MatchLake:
    root: Path
    tables: dict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: {
            name: []
            for name in (
                "sessions",
                "logs",
                "hoot_logs",
                "session_hoots",
                "match_phases",
                "derived_state",
                "files",
            )
        }
    )

    @property
    def lake(self) -> LakePaths:
        return LakePaths(self.root)

    def raw(self, name: str, content: bytes, kind: str) -> str:
        sha = hashlib.sha256(content).hexdigest()
        path = raw_path(self.lake, sha, f".{kind}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        self.tables["files"].append(
            {"sha256": sha, "kind": kind, "size": len(content), "stage": "success"}
        )
        self.tables["logs"].append(
            {"log_id": sha, "kind": kind, "season": "2026", "filename": name, "utc_start": None}
        )
        return sha

    def add_match(
        self,
        key: str | None,
        session_id: str,
        rows: list[dict[str, Any]] | None,
        robot: str = "2026-comp",
        phases: list[tuple[str, float | None, float | None]] | None = None,
        confidence: str = "high",
        wpilog_name: str | None = None,
        start_utc: str = "2026-04-09T21:50:34+00:00",
        pro_licensed: int = 1,
        fingerprint: str = "fp-1",
        hoot_buses: tuple[str, ...] = ("rio", "6E9415C3394C485320202050101C18FF"),
        with_wpilog: bool = True,
        season: str = "2026",
    ) -> None:
        wpilog_id = None
        if with_wpilog:
            name = wpilog_name or f"FRC_{session_id}.wpilog"
            wpilog_id = self.raw(name, f"wpilog {session_id}".encode(), "wpilog")
            self.tables["logs"][-1]["utc_start"] = start_utc
        self.tables["sessions"].append(
            {"session_id": session_id, "wpilog_id": wpilog_id, "robot": robot, "season": season,
             "match_key": key, "match_source": "fms" if key else "none",
             "kind": "match" if key else "non-match", "warnings": None}
        )  # fmt: skip
        for bus in hoot_buses:
            sha = self.raw(
                f"GACMP_{session_id}_{bus}.hoot", f"hoot {session_id} {bus}".encode(), "hoot"
            )
            self.tables["hoot_logs"].append(
                {"log_id": sha, "bus": bus, "pro_licensed": pro_licensed}
            )
            self.tables["session_hoots"].append(
                {"log_id": sha, "session_id": session_id, "hoot_group": "g", "bus": bus,
                 "offset_us": 0 if rows is not None else None,
                 "method": "payload-match" if rows is not None else None,
                 "confidence": confidence if rows is not None else None}
            )  # fmt: skip
        for name, start, end in phases if phases is not None else default_phases():
            self.tables["match_phases"].append(
                {"session_id": session_id, "phase": name,
                 "start_us": None if start is None else round(start * S),
                 "end_us": None if end is None else round(end * S), "source": "hoot-robot-mode"}
            )  # fmt: skip
        self.tables["derived_state"].append(
            {"session_id": session_id, "fingerprint": fingerprint,
             "silver_rows": len(rows or []), "gold_rows": 0}
        )  # fmt: skip
        if rows is not None:
            write_silver(
                silver_dir(self.lake) / f"season={season}" / f"session_id={session_id}",
                rows,
                session_id,
            )

    def set_fingerprint(self, session_id: str, fingerprint: str) -> None:
        for row in self.tables["derived_state"]:
            if row["session_id"] == session_id:
                row["fingerprint"] = fingerprint

    def write_meta(self) -> LakePaths:
        self.lake.meta.mkdir(parents=True, exist_ok=True)
        for name, rows in self.tables.items():
            path = self.lake.meta / f"{name}.parquet"
            if rows:
                pl.DataFrame(rows, infer_schema_length=None).write_parquet(path)
            else:
                path.unlink(missing_ok=True)
        return self.lake


def default_phases() -> list[tuple[str, float | None, float | None]]:
    """A 165 s match from T=10 s: auto 15 s, a 2 s gap, teleop 148 s."""
    return [
        ("pre", None, 10.0),
        ("auto", 10.0, 25.0),
        ("gap", 25.0, 27.0),
        ("teleop", 27.0, 175.0),
        ("post", 175.0, None),
    ]
