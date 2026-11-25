"""The Phoenix Pro coverage check (team-setup "Phoenix Pro coverage check"; 6.8)."""

from pathlib import Path
from typing import Any

import pytest

from flashpoint.docs_links import docs_url
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.setup import checks
from tests.setup.test_checks import IGNITE, _by_id, _context

CANIVORE = "6E9415C3394C485320202050101C18FF"


def mixed_lake(root: Path) -> LakePaths:
    """4 Pro CANivore hoots, 4 non-Pro rio hoots, and a hoot quarantined before conversion."""
    lake = LakePaths(root)
    lake.ensure()
    ledger = Ledger(lake.ledger)
    try:
        for n in range(4):
            for bus, pro in ((CANIVORE, 1), ("rio", 0)):
                sha = f"{bus[:4]}{n}".ljust(64, "0")
                ledger.register(sha, "hoot", 1, lake.raw / sha)
                ledger.execute("UPDATE files SET stage = 'success' WHERE sha256 = ?", (sha,))
                ledger.execute(
                    "INSERT INTO hoot_logs (log_id, bus, pro_licensed) VALUES (?, ?, ?)",
                    (sha, bus, pro),
                )
        ledger.register("e" * 64, "hoot", 0, lake.raw / "e")
        ledger.quarantine("e" * 64, "empty-file", 1)
    finally:
        ledger.close()
    return lake


def _check(lake: LakePaths, tmp_path: Path) -> Any:
    ctx = _context(IGNITE, lake.root)
    return _by_id(checks.run_checks(ctx))["pro-coverage"], checks.run_checks(ctx)


def test_empty_lake(tmp_path: Path) -> None:
    check, _ = _check(LakePaths(tmp_path / "lake"), tmp_path)
    assert (check.status, check.message) == ("ok", "no hoots yet")
    assert check.fix_action is None


def test_mixed_lake(tmp_path: Path) -> None:
    check, results = _check(mixed_lake(tmp_path / "lake"), tmp_path)
    assert check.status == "ok"
    assert check.message.startswith("4 of 8 hoots Pro-licensed, with 4 non-Pro on `rio`")
    assert "1 more not converted (unknown)" in check.message
    assert "temperature trends and temperature anomalies need a Phoenix Pro-licensed device" in (
        check.message
    )
    assert check.command == f"see {docs_url('pro-and-temperature')}"
    assert check.fix_action is None
    assert check.status not in ("warning", "problem")
    assert not [r for r in results if r.id == "pro-coverage" and r.status != "ok"]


def test_all_pro_message(tmp_path: Path) -> None:
    lake = mixed_lake(tmp_path / "lake")
    ledger = Ledger(lake.ledger)
    try:
        ledger.execute("UPDATE hoot_logs SET pro_licensed = 1")
    finally:
        ledger.close()
    check, _ = _check(lake, tmp_path)
    assert check.status == "ok"
    assert check.message == (
        "every converted hoot is Pro-licensed (8 of 8); 1 more not converted (unknown)"
    )
    assert check.command is None


@pytest.mark.corpus
def test_all_pro_corpus(corpus_lake: LakePaths, tmp_path: Path) -> None:
    ledger = Ledger(corpus_lake.ledger)
    try:
        converted = ledger.query("SELECT count(*) AS n FROM hoot_logs")[0]["n"]
        unknown = ledger.query(
            "SELECT count(*) AS n FROM files f LEFT JOIN hoot_logs h ON h.log_id = f.sha256"
            " WHERE f.kind = 'hoot' AND h.log_id IS NULL"
        )[0]["n"]
    finally:
        ledger.close()
    assert unknown >= 1  # the empty quarantined hoot
    check, _ = _check(corpus_lake, tmp_path)
    assert check.status == "ok"
    assert check.message.startswith(
        f"every converted hoot is Pro-licensed ({converted} of {converted})"
    )
    assert f"{unknown} more not converted (unknown)" in check.message


def test_checks_stay_fast_with_the_coverage_check(tmp_path: Path) -> None:
    import time

    ctx = _context(IGNITE, mixed_lake(tmp_path / "lake").root)
    start = time.perf_counter()
    checks.run_checks(ctx)
    assert time.perf_counter() - start < 2.0
