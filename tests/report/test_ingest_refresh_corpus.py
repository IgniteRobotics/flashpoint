"""#15: a wpilog that arrives after its hoots refreshes Replay with no `flashpoint report`."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from flashpoint.cli import main
from flashpoint.lake.paths import LakePaths
from flashpoint.report.paths import data_dir

pytestmark = pytest.mark.corpus

KEY = "2026gacmp_qm7"


def _load(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    return json.loads(text[text.index("(") + 1 : -3])


def _status(lake: Path) -> str:
    index = _load(data_dir(LakePaths(lake)) / "matches.js")
    return str(next(m["status"] for m in index["matches"] if m["key"] == KEY))


def test_wpilog_after_hoots_rebuilds_replay(
    tmp_path: Path, corpus_group: Callable[[str], list[Path]]
) -> None:
    lake = tmp_path / "lake"
    files = corpus_group("2026-gacmp-q7")
    hoots = [str(p) for p in files if p.suffix == ".hoot"]
    (wpilog,) = [str(p) for p in files if p.suffix == ".wpilog"]

    assert main(["ingest", "--no-report", "--lake", str(lake), *hoots]) == 0
    assert main(["report", "--lake", str(lake)]) == 0
    assert _status(lake) == "no-aligned-samples"

    assert main(["ingest", "--lake", str(lake), wpilog]) == 0

    assert _status(lake) == "ok"
    data = _load(data_dir(LakePaths(lake)) / f"{KEY}.js")
    assert data["alignment"] != "none"
