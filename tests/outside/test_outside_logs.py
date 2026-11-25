"""Outside teams' logs, end to end (mvp-outside-team-logs design §10, task 1.2).

Each team folder under `$FLASHPOINT_OUTSIDE_LOGS` (default `~/flashpoint-outside-logs/`) holds
that team's logs, its permission note, and a private `expect.toml`. Nothing about a team is
committed: the expectations live only in that file.

    team = 1                    # for `flashpoint init`: any number from 1 to 99999 (default 1)
    [pro]                       # expected Phoenix Pro flag per bus: "rio", or "canivore" (any)
    rio = false
    canivore = true
    [[match]]
    key = "2026abcd_qm7"        # a match key the lake must have
    alignment = "wall-clock"    # optional: the alignment method of its hoots
    replay = true               # optional (default true): a built Replay entry with status ok

The logs are ingested the way an outside team would: a fresh `flashpoint init` configuration
and the first-derive robot draft. Run with `pytest -m outside`; it skips when the folder is
absent. The self-test builds a team folder from corpus Q7 (`-m corpus`).
"""

import json
import os
import tomllib
from pathlib import Path
from typing import Any

import pytest

from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.report.paths import data_dir

OUTSIDE_ENV = "FLASHPOINT_OUTSIDE_LOGS"
LOG_SUFFIXES = (".wpilog", ".hoot")


def outside_root() -> Path:
    return Path(os.environ.get(OUTSIDE_ENV, Path.home() / "flashpoint-outside-logs")).expanduser()


def team_folders(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(d for d in root.iterdir() if (d / "expect.toml").is_file())


def _bus_kind(bus: str) -> str:
    return "rio" if bus.lower() == "rio" else "canivore"


def check_team(folder: Path, work: Path, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Ingest one team folder into a fresh lake and compare it with its expect.toml."""
    from flashpoint.cli import main
    from flashpoint.setup.init import init_config

    expect = tomllib.loads((folder / "expect.toml").read_text(encoding="utf-8"))
    root, lake = work / "cfg", work / "lake"
    init_config(root, team=int(expect.get("team", 1)), lake=lake)
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    logs = sorted(str(p) for p in folder.rglob("*") if p.suffix.lower() in LOG_SUFFIXES)
    if not logs:
        return [f"{folder.name}: no .wpilog or .hoot files"]
    problems: list[str] = []
    if main(["ingest", "--lake", str(lake), *logs]) != 0:
        problems.append("ingest exited non-zero")
    paths = LakePaths(lake)
    ledger = Ledger(paths.ledger)
    try:
        sessions = ledger.query("SELECT * FROM sessions")
        hoots = ledger.query(
            "SELECT h.bus, h.pro_licensed, sh.session_id, sh.method FROM hoot_logs h"
            " LEFT JOIN session_hoots sh USING (log_id)"
        )
    finally:
        ledger.close()
    for bus_kind, pro in (expect.get("pro") or {}).items():
        found = {bool(h["pro_licensed"]) for h in hoots if _bus_kind(h["bus"] or "") == bus_kind}
        if found != {bool(pro)}:
            problems.append(f"pro on {bus_kind}: expected {bool(pro)}, found {sorted(found)}")
    index_path = data_dir(paths) / "matches.js"
    index: dict[str, Any] = {"matches": []}
    if index_path.is_file():
        text = index_path.read_text(encoding="utf-8")
        index = json.loads(text[text.index("(") + 1 : -3])
    entries = {m["key"]: m for m in index["matches"]}
    by_key = {s["match_key"]: s for s in sessions if s["match_key"]}
    for match in expect.get("match", []):
        key = match["key"]
        session = by_key.get(key)
        if session is None:
            problems.append(f"{key}: no session with this match key")
            continue
        if "alignment" in match:
            methods = {h["method"] for h in hoots if h["session_id"] == session["session_id"]}
            if match["alignment"] not in methods:
                problems.append(f"{key}: alignment {sorted(map(str, methods))}, expected"
                                f" {match['alignment']}")  # fmt: skip
        if match.get("replay", True) and entries.get(key, {}).get("status") != "ok":
            problems.append(f"{key}: Replay entry {entries.get(key, {}).get('status')!r}")
    return problems


TEAMS = team_folders(outside_root())


@pytest.mark.outside
@pytest.mark.parametrize("folder", TEAMS or [None], ids=lambda f: f.name if f else "none")
def test_outside_team(folder: Path | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    if folder is None:
        pytest.skip(f"no outside logs in {outside_root()} (set ${OUTSIDE_ENV})")
    assert check_team(folder, tmp_path, monkeypatch) == []


@pytest.mark.corpus
def test_harness_on_corpus_q7(
    corpus_group: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = tmp_path / "outside" / "self-test"
    folder.mkdir(parents=True)
    for path in corpus_group("2026-gacmp-q7"):
        (folder / path.name).symlink_to(path)
    (folder / "expect.toml").write_text(
        "team = 6829\n[pro]\nrio = true\ncanivore = true\n"
        '[[match]]\nkey = "2026gacmp_qm7"\nalignment = "payload-match"\n',
        encoding="utf-8",
    )
    assert team_folders(tmp_path / "outside") == [folder]
    assert check_team(folder, tmp_path / "work", monkeypatch) == []


def test_harness_reports_a_wrong_expectation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The harness fails loudly: a folder whose logs don't match its expect.toml."""
    from tests.wpilog_builder import WpilogBuilder

    folder = tmp_path / "team"
    folder.mkdir()
    WpilogBuilder().start(1, "x", "double").double(1, 5, 1.0).write(
        folder / "FRC_20260409_215113.wpilog"
    )
    (folder / "expect.toml").write_text('[[match]]\nkey = "2026abcd_qm7"\n', encoding="utf-8")
    assert check_team(folder, tmp_path / "work", monkeypatch) == [
        "2026abcd_qm7: no session with this match key"
    ]
