"""Per-match Replay data: `<lake>/report/data/<match_key>.js` plus the match index.

Each data file calls `FP.register(<json>)` and the index calls `FP.index(<json>)`, so the app
loads them with <script> tags, which work from file:// where fetch() does not (#59). Builds are
incremental: `site-state.json` records each match's derived fingerprint, and the index is
rewritten from those summaries without re-reading any data file (#37).
"""

import hashlib
import json
import math
import os
import re
import tempfile
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import duckdb

from flashpoint.lake.paths import LakePaths
from flashpoint.report import envelope, markers
from flashpoint.report.names import download_name
from flashpoint.report.paths import data_dir, file_stem, state_path
from flashpoint.report.settings import ReportConfig, load_report_config
from flashpoint.semantics.robot_config import RobotConfig, load_robots
from flashpoint.semantics.silver import silver_dir

REPORT_VERSION = 2
BUDGET_BYTES = 2_000_000
MIN_BUCKETS = 100
DUCKDB_MEMORY_LIMIT = "256MB"
DUCKDB_THREADS = 2
SLOW_HZ = 20.0  # a slot whose current logs slower than this gets a header note
ALIGNMENT_RANK = {"high": 0, "low": 1, "very-low": 2}
_LABELS = {"qm": "Q", "pm": "P", "e": "E", "sf": "SF", "f": "F"}
_FEATURES = (
    "temp_max_c",
    "supply_current_p95",
    "stator_current_p95",
    "supply_energy_wh",
    "motor_energy_wh",
    "stall_s",
    "supply_voltage_min",
)


def match_label(match_key: str) -> str:
    """2026gacmp_qm7 -> Q7, 2026gacmp_e10 -> E10."""
    found = re.match(r"^[^_]*_([a-z]+)(\d+)(r\d+)?$", match_key)
    if not found:
        return match_key
    level, number, replay = found.groups()
    return f"{_LABELS.get(level, level.upper())}{number}{replay or ''}"


def event_of(match_key: str) -> str:
    return match_key.split("_", 1)[0]


def to_script(call: str, payload: Any) -> str:
    """`call(<json>);` safe to load as a script: `</` and U+2028/2029 are escaped."""
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    text = text.replace("</", "<\\/").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    return f"{call}({text});\n"


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        Path(tmp).chmod(0o644)  # mkstemp creates 0600; the files are shared and exported
        Path(tmp).replace(path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _finite(value: Any) -> Any:
    return value if not isinstance(value, float) or math.isfinite(value) else None


@dataclass
class BuildSummary:
    built: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    reduced: dict[str, int] = field(default_factory=dict)  # match key -> buckets used
    sessions_without_match_key: int = 0
    matches_listed: int = 0
    seconds: float = 0.0


class LakeMeta:
    """The meta Parquet snapshots a build reads (never the SQLite ledger)."""

    TABLES = (
        "sessions",
        "logs",
        "hoot_logs",
        "session_hoots",
        "match_phases",
        "derived_state",
        "files",
    )

    def __init__(self, lake: LakePaths, con: duckdb.DuckDBPyConnection) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = {}
        for name in self.TABLES:
            path = lake.meta / f"{name}.parquet"
            if not path.is_file():
                self.tables[name] = []
                continue
            cursor = con.execute("SELECT * FROM read_parquet(?)", [str(path)])
            columns = [d[0] for d in cursor.description]
            self.tables[name] = [dict(zip(columns, r, strict=True)) for r in cursor.fetchall()]

    def rows(self, table: str, **where: Any) -> list[dict[str, Any]]:
        return [r for r in self.tables[table] if all(r.get(k) == v for k, v in where.items())]

    def one(self, table: str, **where: Any) -> dict[str, Any] | None:
        found = self.rows(table, **where)
        return found[0] if found else None


class ReportBuilder:
    def __init__(
        self,
        lake: LakePaths,
        config_dir: Path,
        budget_bytes: int = BUDGET_BYTES,
        buckets: int = envelope.BUCKETS,
    ) -> None:
        self.lake = lake
        self.config_dir = config_dir
        self.budget_bytes = budget_bytes
        self.buckets = buckets
        robots_dir = config_dir / "robots"
        self.robots: dict[str, RobotConfig] = (
            {r.robot: r for r in load_robots(robots_dir)} if robots_dir.is_dir() else {}
        )
        self.report: ReportConfig = load_report_config(config_dir)
        self.con = duckdb.connect()
        self.con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
        self.con.execute(f"SET threads = {DUCKDB_THREADS}")  # per-thread buffers cost ~75 MB
        self.meta = LakeMeta(lake, self.con)

    def close(self) -> None:
        self.con.close()

    # --- fingerprints and state -------------------------------------------------------

    def _config_hash(self) -> str:
        digest = hashlib.sha256(json.dumps(asdict(self.report), sort_keys=True).encode())
        for robot in sorted(self.robots):
            digest.update(self.robots[robot].model_dump_json().encode())
        digest.update(f"{self.budget_bytes}:{self.buckets}".encode())
        return digest.hexdigest()

    def fingerprint(self, sessions: list[dict[str, Any]], config_hash: str) -> str:
        derived = sorted(
            (s["session_id"], (self.meta.one("derived_state", session_id=s["session_id"]) or {})
             .get("fingerprint"))
            for s in sessions
        )  # fmt: skip
        payload = json.dumps([REPORT_VERSION, config_hash, derived], default=str)
        return hashlib.sha256(payload.encode()).hexdigest()

    def _load_state(self) -> dict[str, Any]:
        path = state_path(self.lake)
        if path.is_file():
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(state, dict) and isinstance(state.get("matches"), dict):
                    return state
            except json.JSONDecodeError:
                pass
        return {"version": REPORT_VERSION, "matches": {}}

    # --- build ------------------------------------------------------------------------

    def matches(self) -> dict[str, list[dict[str, Any]]]:
        keyed: dict[str, list[dict[str, Any]]] = {}
        for session in self.meta.tables["sessions"]:
            if session["match_key"]:
                keyed.setdefault(session["match_key"], []).append(session)
        return keyed

    def build(
        self,
        events: Iterable[str] | None = None,
        match_keys: Iterable[str] | None = None,
        force: bool = False,
    ) -> BuildSummary:
        started = time.perf_counter()
        summary = BuildSummary()
        summary.sessions_without_match_key = sum(
            1 for s in self.meta.tables["sessions"] if not s["match_key"]
        )
        state = self._load_state()
        known = self.matches()
        wanted_events = {e.lower() for e in events or []}
        wanted_keys = set(match_keys or [])
        selecting = bool(wanted_events or wanted_keys)
        config_hash = self._config_hash()
        for key in sorted(known):
            if selecting and key not in wanted_keys and event_of(key) not in wanted_events:
                continue
            fp = self.fingerprint(known[key], config_hash)
            previous = state["matches"].get(key)
            data_file = data_dir(self.lake) / f"{file_stem(key)}.js"
            if (
                not force
                and previous
                and previous.get("fingerprint") == fp
                and previous.get("version") == REPORT_VERSION
                and data_file.is_file()
            ):
                summary.unchanged.append(key)
                continue
            payload, buckets = self.match_payload(key, known[key])
            if buckets is not None and buckets < self.buckets:
                summary.reduced[key] = buckets
            _write_atomic(data_file, to_script("FP.register", payload))
            state["matches"][key] = {
                "fingerprint": fp,
                "version": REPORT_VERSION,
                "summary": self._summary(payload, data_file.name),
            }
            summary.built.append(key)
        if not selecting:
            for key in sorted(set(state["matches"]) - set(known)):
                (data_dir(self.lake) / f"{file_stem(key)}.js").unlink(missing_ok=True)
                del state["matches"][key]
                summary.removed.append(key)
        self.write_index(state)
        _write_atomic(state_path(self.lake), json.dumps(state, indent=1, sort_keys=True))
        summary.matches_listed = len(state["matches"])
        summary.seconds = time.perf_counter() - started
        return summary

    def write_index(self, state: dict[str, Any]) -> None:
        entries = sorted(
            (m["summary"] for m in state["matches"].values()),
            key=lambda e: (e["event"], e["start_utc"] or "", e["key"]),
        )
        _write_atomic(
            data_dir(self.lake) / "matches.js",
            to_script(
                "FP.index",
                {
                    "version": REPORT_VERSION,
                    "lake": str(self.lake.root),
                    "sessions_without_match_key": sum(
                        1 for s in self.meta.tables["sessions"] if not s["match_key"]
                    ),
                    "matches": entries,
                },
            ),
        )

    @staticmethod
    def _summary(payload: dict[str, Any], file_name: str) -> dict[str, Any]:
        counts = {"WARN": 0, "FAULT": 0}
        for marker in payload.get("markers", []):
            counts[marker["level"]] += 1
        return {
            "key": payload["match_key"],
            "label": payload["label"],
            "event": payload["event"],
            "robot": payload["robot"],
            "season": payload["season"],
            "start_utc": payload["start_utc"],
            "status": payload["status"],
            "reason": payload["reason"],
            "alignment": payload["alignment"],
            "low_alignment": payload["low_alignment"],
            "markers": counts,
            "file": f"data/{file_name}",
        }

    # --- one match --------------------------------------------------------------------

    def _sources(self, key: str, sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        for session in sessions:
            ids = [(session["wpilog_id"], "wpilog")] if session["wpilog_id"] else []
            ids += [
                (h["log_id"], h["bus"] or "hoot")
                for h in sorted(
                    self.meta.rows("session_hoots", session_id=session["session_id"]),
                    key=lambda h: h["log_id"],
                )
            ]
            for log_id, part in ids:
                log = self.meta.one("logs", log_id=log_id) or {}
                file = self.meta.one("files", sha256=log_id) or {}
                kind = log.get("kind") or file.get("kind") or "hoot"
                name = log.get("filename") or f"{log_id}.{kind}"
                bus = "rio" if part.lower() == "rio" else part
                out.append(
                    {
                        "sha256": log_id,
                        "name": name,
                        "kind": kind,
                        "part": "wpilog" if part == "wpilog" else bus,
                        "size": file.get("size"),
                        "suffix": f".{kind}",
                        "session_id": session["session_id"],
                        "download": download_name(key, "wpilog" if part == "wpilog" else bus, name),
                    }
                )
        return out

    def _alignment(self, session_id: str) -> tuple[str, str | None]:
        hoots = self.meta.rows("session_hoots", session_id=session_id)
        if not hoots:
            return "none", None
        worst = max(hoots, key=lambda h: ALIGNMENT_RANK.get(h["confidence"], 3))
        return worst["confidence"] or "none", worst["method"]

    def _silver(self, session: dict[str, Any]) -> Path | None:
        path = (
            silver_dir(self.lake)
            / f"season={session['season']}"
            / (f"session_id={session['session_id']}")
        )
        return path if path.is_dir() and any(path.glob("*.parquet")) else None

    def _temperature_reason(self, sessions: list[dict[str, Any]]) -> str:
        licensed = [
            (self.meta.one("hoot_logs", log_id=h["log_id"]) or {}).get("pro_licensed")
            for s in sessions
            for h in self.meta.rows("session_hoots", session_id=s["session_id"])
        ]
        if licensed and not any(licensed):
            return "The hoots are not Pro-licensed, so they carry no temperature."
        return "No temperature was logged for this match."

    def match_payload(
        self, key: str, sessions: list[dict[str, Any]]
    ) -> tuple[dict[str, Any], int | None]:
        aligned = [s for s in sessions if self._silver(s) is not None]
        sources = self._sources(key, sessions)
        if not aligned:
            primary = sessions[0]
            log = self.meta.one("logs", log_id=primary["wpilog_id"]) or {}
            return {
                "version": REPORT_VERSION,
                "match_key": key,
                "label": match_label(key),
                "event": event_of(key),
                "robot": primary["robot"],
                "season": primary["season"],
                "start_utc": log.get("utc_start"),
                "status": "no-aligned-samples",
                "reason": "No aligned samples: this match has no wpilog-aligned device data"
                " (for example, hoots without a wpilog). Only the raw logs are available.",
                "alignment": "none",
                "low_alignment": True,
                "sources": sources,
                "markers": [],
            }, None
        primary = max(
            aligned,
            key=lambda s: (self.meta.one("derived_state", session_id=s["session_id"]) or {}).get(
                "silver_rows"
            )
            or 0,
        )
        silver = self._silver(primary)
        assert silver is not None
        phases = self.meta.rows("match_phases", session_id=primary["session_id"])
        enabled = [p for p in phases if p["phase"] in ("auto", "teleop", "test")]
        framed = bool(enabled) and enabled[0]["start_us"] is not None
        span = self.con.execute(
            "SELECT min(t_us), max(t_us) FROM read_parquet(?)", [str(silver / "*.parquet")]
        ).fetchone()
        assert span is not None
        first_us, last_us = int(span[0]), int(span[1])
        if framed:
            match_start = int(enabled[0]["start_us"])
            match_end = max((p["end_us"] for p in enabled if p["end_us"] is not None), default=None)
            match_end = int(match_end) if match_end is not None else last_us
        else:
            match_start, match_end = first_us, last_us
        notes: list[str] = []
        if not framed:
            notes.append(
                "No match phases were found (the robot was never enabled in this log), so"
                " time is measured from the first sample and the whole log is shown."
            )
        others = [s for s in sessions if s is not primary]
        if others:
            notes.append(
                f"{len(others)} more session(s) share this match key"
                f" ({sum(1 for s in others if self._silver(s) is None)} without aligned"
                " samples); their raw logs are listed below."
            )
        alignment, method = self._alignment(primary["session_id"])
        log = self.meta.one("logs", log_id=primary["wpilog_id"]) or {}
        robot = self.robots.get(primary["robot"])
        slot_config = {s.id: s for s in robot.slots} if robot else {}
        units = self.con.execute(
            "SELECT slot_id, list(DISTINCT unit_id ORDER BY unit_id) FROM read_parquet(?)"
            " GROUP BY slot_id ORDER BY slot_id",
            [str(silver / "*.parquet")],
        ).fetchall()
        features = self._features(primary)
        temps_window = envelope.window_for(match_start, match_end, self.buckets)
        temps = envelope.temperature_points(self.con, silver, temps_window)
        found_markers = markers.match_markers(
            self.con,
            silver,
            temps_window,
            match_end if framed else None,
            temps,
            self.report,
        )
        buckets = self.buckets
        while True:
            window = envelope.window_for(match_start, match_end, buckets)
            payload = envelope.series_payload(self.con, silver, window, temps, match_end)
            payload.update(
                {
                    "version": REPORT_VERSION,
                    "match_key": key,
                    "label": match_label(key),
                    "event": event_of(key),
                    "robot": primary["robot"],
                    "season": primary["season"],
                    "session_id": primary["session_id"],
                    "t0_us": window.t0_us,
                    "start_utc": log.get("utc_start"),
                    "duration_s": round((match_end - match_start) / envelope.US_PER_S, 3),
                    "framed": framed,
                    "status": "ok",
                    "reason": None,
                    "alignment": alignment,
                    "alignment_method": method,
                    "low_alignment": alignment != "high",
                    "phases": [
                        {
                            "name": p["phase"],
                            "start": None
                            if p["start_us"] is None
                            else round(window.seconds(p["start_us"]), 3),
                            "end": None
                            if p["end_us"] is None
                            else round(window.seconds(p["end_us"]), 3),
                        }
                        for p in sorted(
                            phases,
                            key=lambda p: -(2**62) if p["start_us"] is None else p["start_us"],
                        )
                    ],
                    "slots": [
                        {
                            "id": slot_id,
                            "units": unit_ids,
                            "role": slot_config[slot_id].role if slot_id in slot_config else None,
                            "subsystem": slot_config[slot_id].subsystem
                            if slot_id in slot_config
                            else None,
                            "model": slot_config[slot_id].model if slot_id in slot_config else None,
                            "features": features.get(slot_id, {}),
                        }
                        for slot_id, unit_ids in units
                    ],
                    "markers": found_markers,
                    "sources": sources,
                    "limits": {
                        "temp_warn_c": self.report.temp_warn_c,
                        "temp_fault_c": self.report.temp_fault_c,
                        "brownout_v": self.report.brownout_v,
                        "sag_v": self.report.sag_v,
                    },
                }
            )
            slow = sorted(slot for slot, hz in payload["rates"].items() if 0 < hz < SLOW_HZ)
            payload["notes"] = notes + (
                [
                    f"{len(slow)} slot(s) log current slower than {SLOW_HZ:.0f} Hz"
                    f" ({', '.join(slow)}); spikes between their samples are not visible."
                ]
                if slow
                else []
            )
            if not payload["temperature"]["available"]:
                payload["temperature"]["reason"] = self._temperature_reason(sessions)
            size = len(to_script("FP.register", payload).encode())
            if size <= self.budget_bytes or buckets <= MIN_BUCKETS:
                payload["bytes"] = size
                return payload, buckets
            buckets = max(MIN_BUCKETS, int(buckets * self.budget_bytes / size * 0.95))

    def _features(self, session: dict[str, Any]) -> dict[str, dict[str, Any]]:
        gold = self.lake.root / "gold" / "match_features"
        files = list(gold.glob(f"season=*/session_id={session['session_id']}/*.parquet"))
        if not files:
            return {}
        cursor = self.con.execute(
            "SELECT * FROM read_parquet(?, union_by_name = true)", [[str(f) for f in files]]
        )
        columns = [d[0] for d in cursor.description]
        out: dict[str, dict[str, Any]] = {}
        for row in cursor.fetchall():
            record = dict(zip(columns, row, strict=True))
            phase = out.setdefault(record["slot_id"], {}).setdefault(record["phase"], {})
            for name in _FEATURES:
                value = _finite(record.get(name))
                phase[name] = envelope.round_sig([value])[0] if value is not None else None
        return out


def build_reports(
    lake: LakePaths,
    config_dir: Path,
    events: Iterable[str] | None = None,
    match_keys: Iterable[str] | None = None,
    force: bool = False,
) -> BuildSummary:
    builder = ReportBuilder(lake, config_dir)
    try:
        return builder.build(events, match_keys, force)
    finally:
        builder.close()
