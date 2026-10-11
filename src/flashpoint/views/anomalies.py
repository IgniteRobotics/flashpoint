"""Anomalies for the app: the detection table joined with live triage, causes and "seen this
before" resolved at read time, and the one write the app accepts, a triage change.

Reads `gold/anomalies/` (Parquet and summary JSON) and the ledger's `anomaly_triage` table
(read-only connections); writes only by appending a triage row through the ledger.
"""

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

import polars as pl

from flashpoint.anomaly import restarts
from flashpoint.anomaly.catalogue import CatalogueError, causes_for, load_causes
from flashpoint.anomaly.episodes import SENSITIVITIES, shown_at
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths

ANOMALIES = Path("gold") / "anomalies" / "anomalies.parquet"
TABS = ("open", "resolved", "dismissed")
STATES = ("new", "investigating", "resolved", "dismissed")
TAB_OF = {"new": "open", "investigating": "open", "resolved": "resolved", "dismissed": "dismissed"}
TRANSITIONS = {
    "new": ("investigating", "dismissed"),
    "investigating": ("resolved", "dismissed"),
    "resolved": ("new",),  # REOPEN
    "dismissed": ("new",),
}
NEEDS_OUTCOME = ("resolved", "dismissed")
MAX_OUTCOME = 500
SEVERITY_RANK = {"HIGH": 0, "MED": 1, "LOW": 2}
NO_CAUSES = "No causes recorded for this pattern yet"
ITEM_COLUMNS = (
    "id", "subject", "subject_key", "subject_kind", "robot", "season", "slot_id", "role",
    "subsystem", "model", "method", "metric", "metric_name", "metric_unit", "severity",
    "sensitivity", "title", "onset_match_key", "last_match_key", "matches_affected",
    "detected_match_key", "confidence", "low_alignment", "last_start",
)  # fmt: skip


class TriageError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


def serial_of(subject: str) -> str | None:
    if subject.startswith("ctre:"):
        return subject.removeprefix("ctre:")
    return None


class AnomalyQueries:
    def __init__(self, lake: LakePaths, config_dir: Path) -> None:
        self.lake = lake
        self.config_dir = config_dir
        self._cache: tuple[tuple[int, int], dict[str, dict[str, Any]]] | None = None
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()

    # --- reads ---------------------------------------------------------------------------

    def _table(self) -> dict[str, dict[str, Any]]:
        """Detected anomalies by id, re-read only when the file changes."""
        path = self.lake.root / ANOMALIES
        try:
            stat = path.stat()
        except FileNotFoundError:
            return {}
        stamp = (stat.st_mtime_ns, stat.st_size)
        with self._lock:
            if self._cache is None or self._cache[0] != stamp:
                rows = pl.read_parquet(path).to_dicts()
                self._cache = (stamp, {r["id"]: r for r in rows})
            return self._cache[1]

    def _triage(self, anomaly_id: str | None = None) -> list[dict[str, Any]]:
        if not self.lake.ledger.is_file():
            return []
        db = sqlite3.connect(f"{self.lake.ledger.as_uri()}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        try:
            if anomaly_id is None:
                rows = db.execute("SELECT * FROM anomaly_triage ORDER BY seq").fetchall()
            else:
                rows = db.execute(
                    "SELECT * FROM anomaly_triage WHERE anomaly_id = ? ORDER BY seq",
                    (anomaly_id,),
                ).fetchall()
        except sqlite3.OperationalError:  # a ledger from before the triage table
            return []
        finally:
            db.close()
        return [dict(r) for r in rows]

    def _states(self) -> dict[str, dict[str, Any]]:
        return {r["anomaly_id"]: r for r in self._triage()}  # the last row wins

    def summary_file(self) -> dict[str, Any]:
        path = self.lake.root / restarts.SUMMARY_PATH
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        return data if isinstance(data, dict) else {}

    def _item(self, row: dict[str, Any], state: dict[str, Any] | None) -> dict[str, Any]:
        item = {k: row.get(k) for k in ITEM_COLUMNS}
        item["flagged_keys"] = json.loads(row["flagged_keys"])
        item["serial"] = serial_of(row["subject"])
        item["state"] = state["to_state"] if state else "new"
        item["outcome"] = state["outcome"] if state else None
        item["detected"] = True
        return item

    @staticmethod
    def _orphan(state: dict[str, Any]) -> dict[str, Any]:
        """A triaged anomaly the latest detection run no longer produces."""
        item: dict[str, Any] = dict.fromkeys(ITEM_COLUMNS)
        power = state["subject"].startswith("power:")
        item.update({
            "id": state["anomaly_id"], "subject": "power" if power else state["subject"],
            "subject_key": state["subject"], "metric": state["metric"],
            "onset_match_key": state["onset_match_key"],
            "title": f"No longer detected: {state['metric']} from {state['onset_match_key']}",
            "flagged_keys": [], "serial": serial_of(state["subject"]),
            "state": state["to_state"], "outcome": state["outcome"], "detected": False,
        })  # fmt: skip
        return item

    def items(self) -> list[dict[str, Any]]:
        table, states = self._table(), self._states()
        found = [self._item(row, states.get(i)) for i, row in table.items()]
        found += [self._orphan(s) for i, s in states.items() if i not in table]
        return found

    @staticmethod
    def _shown(item: dict[str, Any], sens: str) -> bool:
        return not item["detected"] or shown_at(item["sensitivity"], sens)

    @staticmethod
    def _sorted(items: Any) -> list[dict[str, Any]]:
        """By severity, then the most recent last-flagged match first."""
        by_time = sorted(items, key=lambda i: (i["last_start"] or "", i["id"]), reverse=True)
        return sorted(by_time, key=lambda i: SEVERITY_RANK.get(i["severity"], 3))

    def queue(
        self, sens: str = "normal", tab: str = "open", unit: str | None = None,
        match: str | None = None,
    ) -> dict[str, Any]:  # fmt: skip
        if sens not in SENSITIVITIES:
            raise TriageError(400, f"unknown sensitivity {sens!r}; one of {list(SENSITIVITIES)}")
        if tab not in TABS:
            raise TriageError(400, f"unknown tab {tab!r}; one of {list(TABS)}")
        chosen = [
            i for i in self.items()
            if self._shown(i, sens)
            and (unit is None or i["subject_key"] == unit or i["subject"] == unit)
            and (match is None or match in i["flagged_keys"])
        ]  # fmt: skip
        counts = {t: sum(1 for i in chosen if TAB_OF[i["state"]] == t) for t in TABS}
        listed = self._sorted(i for i in chosen if TAB_OF[i["state"]] == tab)
        summary = self.summary_file()
        return {
            "sensitivity": sens,
            "tab": tab,
            "items": listed,
            "counts": counts,
            "high": sum(
                1 for i in chosen if TAB_OF[i["state"]] == "open" and i["severity"] == "HIGH"
            ),
            "scanned_through": summary.get("scanned_through"),
            "built_at": summary.get("built_at"),
            "building_baseline": summary.get("building_baseline", {}).get("count", 0),
            "detected": bool(summary),
            "restart_pending": restarts.restart_pending(self.lake),
        }

    def badge(self) -> dict[str, Any]:
        """The navigation badge: open anomalies at normal sensitivity."""
        opened = [
            i for i in self.items()
            if i["detected"] and TAB_OF[i["state"]] == "open"
            and shown_at(i["sensitivity"], "normal")
        ]  # fmt: skip
        return {
            "open": len(opened),
            "high": sum(1 for i in opened if i["severity"] == "HIGH"),
            "restart_pending": restarts.restart_pending(self.lake),
        }

    def _causes(self, row: dict[str, Any]) -> dict[str, Any]:
        try:
            found = causes_for(
                load_causes(self.config_dir), row["method"], row["metric"], row["subsystem"]
            )
        except CatalogueError as exc:
            return {"items": [], "note": f"The cause catalogue has an error: {exc}"}
        return {"items": found, "note": None if found else NO_CAUSES}

    def _seen_before(
        self, row: dict[str, Any], items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        same = [
            i for i in items
            if i["id"] != row["id"] and i["detected"] and i["state"] in ("resolved", "dismissed")
            and i["metric"] == row["metric"] and i["method"] == row["method"]
            and (i["subject_key"] == row["subject_key"]
                 or (i["robot"] == row["robot"] and i["slot_id"] == row["slot_id"]))
        ]  # fmt: skip
        keys = ("id", "subject", "slot_id", "title", "onset_match_key", "state", "outcome")
        return [{k: i[k] for k in keys} for i in self._sorted(same)]

    def detail(self, anomaly_id: str) -> dict[str, Any] | None:
        items = self.items()
        item = next((i for i in items if i["id"] == anomaly_id), None)
        if item is None:
            return None
        history = [
            {k: r[k] for k in ("seq", "at", "from_state", "to_state", "outcome",
                               "restart_baseline")}
            for r in self._triage(anomaly_id)
        ]  # fmt: skip
        detail = {**item, "history": history, "actions": list(TRANSITIONS[item["state"]])}
        row = self._table().get(anomaly_id)
        if row is None:
            return {**detail, "evidence": None, "causes": {"items": [], "note": None},
                    "seen_before": []}  # fmt: skip
        return {
            **detail,
            **{k: row[k] for k in ("actual", "expected", "why", "baseline_n", "max_z",
                                   "peer_name", "detected_normal_match_key")},
            "evidence": json.loads(row["evidence"]),
            "causes": self._causes(row),
            "seen_before": self._seen_before(row, items),
        }  # fmt: skip

    # --- the write ------------------------------------------------------------------------

    def triage(self, anomaly_id: str, body: Any) -> dict[str, Any]:
        """Append one triage change; raises TriageError (status, message) when refused."""
        if not isinstance(body, dict) or set(body) - {"to", "outcome", "restart_baseline"}:
            raise TriageError(400, 'expected {"to": ..., "outcome": ..., "restart_baseline": ...}')
        to, outcome = body.get("to"), body.get("outcome")
        restart = body.get("restart_baseline", False)
        if to not in STATES:
            raise TriageError(400, f"unknown state {to!r}; one of {list(STATES)}")
        if outcome is not None and not isinstance(outcome, str):
            raise TriageError(400, "outcome must be text")
        if not isinstance(restart, bool):
            raise TriageError(400, "restart_baseline must be true or false")
        outcome = (outcome or "").strip() or None
        if to in NEEDS_OUTCOME and outcome is None:
            raise TriageError(400, "an outcome is required: say what was found or done")
        if outcome is not None and len(outcome) > MAX_OUTCOME:
            raise TriageError(400, f"the outcome is longer than {MAX_OUTCOME} characters")
        if restart and to != "dismissed":
            raise TriageError(400, "restart_baseline applies only when dismissing")
        with self._write_lock:
            row = self._table().get(anomaly_id)
            history = self._triage(anomaly_id)
            if row is None and not history:
                raise TriageError(404, f"no anomaly {anomaly_id}")
            state = history[-1]["to_state"] if history else "new"
            if to not in TRANSITIONS[state]:
                raise TriageError(409, f"cannot go from {state} to {to}")
            source = row or {"subject_key": history[-1]["subject"],
                             "metric": history[-1]["metric"],
                             "onset_match_key": history[-1]["onset_match_key"]}  # fmt: skip
            ledger = Ledger(self.lake.ledger)
            try:
                ledger.add_triage({
                    "anomaly_id": anomaly_id, "from_state": state, "to_state": to,
                    "outcome": outcome, "restart_baseline": restart,
                    "subject": source["subject_key"], "metric": source["metric"],
                    "onset_match_key": source["onset_match_key"],
                })  # fmt: skip
            finally:
                ledger.close()
        return {"state": to, "restart_baseline": restart, "detail": self.detail(anomaly_id)}
