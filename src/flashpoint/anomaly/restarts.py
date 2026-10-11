"""Baseline restarts from triage, read without the heavy libraries: the acquire watch checks
`restart_pending` every cycle and must stay small."""

import json
import sqlite3
from pathlib import Path

from flashpoint.lake.paths import LakePaths

SUMMARY_PATH = Path("gold") / "anomalies" / "summary.json"


def restarts(lake: LakePaths) -> tuple[dict[tuple[str, str], str], int]:
    """(subject, metric) -> onset match of its latest restart, and the latest restart's seq."""
    if not lake.ledger.is_file():
        return {}, 0
    db = sqlite3.connect(f"{lake.ledger.as_uri()}?mode=ro", uri=True)
    try:
        rows = db.execute(
            "SELECT seq, subject, metric, onset_match_key FROM anomaly_triage"
            " WHERE restart_baseline = 1 ORDER BY seq"
        ).fetchall()
    except sqlite3.OperationalError:  # a ledger from before the triage table
        return {}, 0
    finally:
        db.close()
    latest = {(subject, metric): onset for _, subject, metric, onset in rows}
    return latest, max((r[0] for r in rows), default=0)


def consumed_seq(lake: LakePaths) -> int:
    """The latest restart the last detection run applied."""
    try:
        summary = json.loads((lake.root / SUMMARY_PATH).read_text(encoding="utf-8"))
        return int(summary.get("restart_seq", 0))
    except (OSError, ValueError, TypeError, AttributeError):
        return 0


def restart_pending(lake: LakePaths) -> bool:
    """A restart was written after the last detection run applied restarts."""
    return restarts(lake)[1] > consumed_seq(lake)
