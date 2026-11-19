"""The first derive drafts the robot (spec: team-setup, "First derive drafts the robot")."""

import hashlib
import json
import os
import shutil
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.report.paths import data_dir
from flashpoint.semantics.robot_config import load_config_seasons, load_robots, select_robot
from flashpoint.setup.init import init_config

CANIVORE = "0123456789abcdef0123456789abcdef"
DRAFT_COMMENT = "# Drafted automatically by flashpoint from "


# --- synthetic lakes ------------------------------------------------------------------------


class SyntheticLake:
    """Logs, hoots and their device entries in a ledger, the inputs session grouping reads."""

    def __init__(self, root: Path) -> None:
        self.lake = LakePaths(root)
        self.lake.meta.mkdir(parents=True, exist_ok=True)
        self.ledger = Ledger(self.lake.ledger)
        self.n = 0

    def _row(self, table: str, row: dict[str, Any]) -> None:
        columns, marks = ", ".join(row), ", ".join("?" for _ in row)
        self.ledger.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({marks})", tuple(row.values())
        )

    def _file(self, log_id: str, kind: str) -> None:
        self._row("files", {"sha256": log_id, "kind": kind, "size": 1, "stage": "success",
                            "first_seen": "2026-04-09T00:00:00+00:00",
                            "updated": "2026-04-09T00:00:00+00:00"})  # fmt: skip

    def _devices(self, log_id: str, devices: list[str]) -> None:
        for name in devices:
            self.n += 1
            entry = {"log_id": log_id, "idx": self.n, "entry_id": self.n,
                     "name": f"Phoenix6/{name}/Timestamp", "type": "double"}  # fmt: skip
            self._row("entries", entry)

    def session(
        self,
        name: str,
        project: str | None,
        stamp: str,
        rio: list[str],
        canivore: list[str] | None = None,
        season: str = "2026",
    ) -> None:
        """A match: a wpilog (unless project is None: hoot-only) and hoots that start with it."""
        start = f"{stamp[:10]}T{stamp[11:13]}:{stamp[14:16]}:{stamp[17:19]}+00:00"
        end = f"{stamp[:10]}T{stamp[11:13]}:{int(stamp[14:16]) + 3:02d}:00+00:00"
        if project is not None:
            wpilog = f"w-{name}"
            self._file(wpilog, "wpilog")
            self._row("logs", {"log_id": wpilog, "kind": "wpilog", "season": season,
                               "filename": f"FRC_{name}.wpilog", "project": project,
                               "utc_start": start, "utc_end": end})  # fmt: skip
        for bus, devices in (("rio", rio), (CANIVORE, canivore or [])):
            if not devices:
                continue
            hoot_id = f"h-{name}-{bus[:3]}"
            self._file(hoot_id, "hoot")
            self._row("logs", {"log_id": hoot_id, "kind": "hoot", "season": season,
                               "filename": f"{bus}_{stamp}.hoot"})  # fmt: skip
            self._row("hoot_logs", {"log_id": hoot_id, "bus": bus, "session_stamp": stamp})
            self._devices(hoot_id, devices)

    def close(self) -> LakePaths:
        self.ledger.close()
        return self.lake


def _configured(tmp_path: Path) -> Path:
    root = tmp_path / "cfg"
    init_config(root, team=254, lake=tmp_path / "lake")
    return root


def _robots(root: Path) -> dict[str, Any]:
    return {r.robot: r for r in load_robots(root / "robots", load_config_seasons(root))}


def _tree_hash(folder: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(folder.rglob("*")):
        if path.is_file():
            digest.update(path.relative_to(folder).as_posix().encode() + path.read_bytes())
    return digest.hexdigest()


# --- unit: auto_draft -----------------------------------------------------------------------


def test_two_robot_projects(tmp_path: Path) -> None:
    from flashpoint.setup.draft import auto_draft

    syn = SyntheticLake(tmp_path / "lake")
    syn.session("comp-q1", "Robot-2026", "2026-04-09_19-00-00", ["TalonFX-1", "TalonFX-2"],
                ["TalonFX-11"])  # fmt: skip
    syn.session("comp-q2", "Robot-2026", "2026-04-09_20-00-00", ["TalonFX-1"])
    syn.session("practice-1", "Practice-2026", "2026-04-10_18-00-00", ["TalonFX-3"])
    lake = syn.close()
    root = _configured(tmp_path)

    drafted = auto_draft(lake, root)

    assert sorted((d.robot, d.project) for d in drafted) == [
        ("2026-comp", "Robot-2026"),
        ("2026-practice", "Practice-2026"),
    ]
    robots = _robots(root)
    assert len(robots["2026-comp"].slots) == 3  # from comp-q1, the session with most devices
    assert select_robot(list(robots.values()), "Robot-2026", "2026").robot == "2026-comp"
    assert select_robot(list(robots.values()), "Practice-2026", "2026").robot == "2026-practice"
    for robot in ("2026-comp", "2026-practice"):
        first = (root / "robots" / f"{robot}.toml").read_text("utf-8").splitlines()[0]
        assert first.startswith(DRAFT_COMMENT) and "Setup > Robots" in first


def test_draft_comes_from_the_session_with_most_devices_newest_on_a_tie(tmp_path: Path) -> None:
    from flashpoint.setup.draft import auto_draft

    syn = SyntheticLake(tmp_path / "lake")
    syn.session("old", "Robot-2026", "2026-04-09_19-00-00", ["TalonFX-1", "TalonFX-2"])
    syn.session("new", "Robot-2026", "2026-04-10_19-00-00", ["TalonFX-1", "TalonFX-4"])
    lake = syn.close()
    root = _configured(tmp_path)

    (drafted,) = auto_draft(lake, root)

    assert drafted.devices == 2 and "new" in drafted.source
    assert {s.id for s in _robots(root)["2026-comp"].slots} == {"talonfx-1", "talonfx-4"}


def test_hoot_only_sessions_draft_nothing(tmp_path: Path) -> None:
    from flashpoint.setup.draft import auto_draft

    syn = SyntheticLake(tmp_path / "lake")
    syn.session("hoots", None, "2026-04-11_19-29-15", ["TalonFX-1"], ["TalonFX-11"])
    lake = syn.close()
    root = _configured(tmp_path)

    assert auto_draft(lake, root) == []
    assert not list((root / "robots").glob("*.toml"))


def test_a_season_with_a_robot_file_is_never_touched(tmp_path: Path) -> None:
    from flashpoint.setup.draft import auto_draft

    syn = SyntheticLake(tmp_path / "lake")
    syn.session("q1", "Robot-2026", "2026-04-09_19-00-00", ["TalonFX-1"])
    syn.session("p1", "Practice-2026", "2026-04-10_18-00-00", ["TalonFX-3"])
    lake = syn.close()
    root = tmp_path / "cfg"
    shutil.copytree(config.config_root(), root)  # Ignite's robots, 2026 included
    before = _tree_hash(root)

    assert auto_draft(lake, root) == []
    assert _tree_hash(root) == before


def test_no_configuration_drafts_nothing(tmp_path: Path) -> None:
    from flashpoint.setup.draft import auto_draft

    syn = SyntheticLake(tmp_path / "lake")
    syn.session("q1", "Robot-2026", "2026-04-09_19-00-00", ["TalonFX-1"])
    lake = syn.close()

    assert auto_draft(lake, tmp_path / "none") == []
    assert not (tmp_path / "none").exists()


@pytest.mark.skipif(sys.platform == "win32" or os.geteuid() == 0, reason="POSIX permissions")
def test_read_only_configuration_is_refused(tmp_path: Path) -> None:
    from flashpoint.setup.draft import ReadOnlyConfig, auto_draft

    syn = SyntheticLake(tmp_path / "lake")
    syn.session("q1", "Robot-2026", "2026-04-09_19-00-00", ["TalonFX-1"])
    lake = syn.close()
    root = _configured(tmp_path)
    (root / "robots").chmod(0o500)
    try:
        with pytest.raises(ReadOnlyConfig):
            auto_draft(lake, root)
    finally:
        (root / "robots").chmod(0o700)
    assert not list((root / "robots").glob("*.toml"))


# --- corpus: the CLI path a team takes --------------------------------------------------------


def _index(lake: Path) -> dict[str, Any]:
    text = (data_dir(LakePaths(lake)) / "matches.js").read_text("utf-8")
    loaded: dict[str, Any] = json.loads(text[text.index("(") + 1 : text.rindex(")")])
    return {m["key"]: m for m in loaded["matches"]}


def _session_robots(lake: Path) -> dict[str, str]:
    ledger = Ledger(LakePaths(lake).ledger)
    try:
        return {r["match_key"]: r["robot"] for r in ledger.query("SELECT * FROM sessions")}
    finally:
        ledger.close()


@pytest.fixture
def init_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = _configured(tmp_path)
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    return root


@pytest.mark.corpus
def test_first_import_of_q7_then_a_later_import(
    tmp_path: Path,
    init_root: Path,
    corpus_group: Callable[[str], list[Path]],
    capfd: pytest.CaptureFixture[str],
) -> None:
    from flashpoint.cli import main

    lake = tmp_path / "lake"
    q7 = [str(p) for p in corpus_group("2026-gacmp-q7")]
    assert main(["ingest", "--lake", str(lake), *q7]) == 0
    out = capfd.readouterr().out
    robot = init_root / "robots" / "2026-comp.toml"
    assert robot.read_text("utf-8").splitlines()[0].startswith(DRAFT_COMMENT + "2026gacmp_qm7")
    drafted = _robots(init_root)["2026-comp"]
    assert len(drafted.slots) == 22 and drafted.project == "Robot-2026"
    (line,) = [ln for ln in out.splitlines() if "drafted robot 2026-comp" in ln]
    assert "2026gacmp_qm7" in line and "22 devices" in line and "Setup > Robots" in line
    assert _index(lake)["2026gacmp_qm7"]["status"] == "ok"

    # Later import keeps the draft.
    before = _tree_hash(init_root / "robots")
    e10 = [str(p) for p in corpus_group("2026-gacmp-e10")]
    main(["ingest", "--lake", str(lake), *e10])
    out = capfd.readouterr().out
    assert _tree_hash(init_root / "robots") == before
    assert "drafted" not in out.lower()
    assert _session_robots(lake)["2026gacmp_e10"] == "2026-comp"


@pytest.mark.corpus
def test_teams_own_robot_untouched(
    tmp_path: Path,
    corpus_group: Callable[[str], list[Path]],
    capfd: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from flashpoint.cli import main

    root = tmp_path / "cfg"
    shutil.copytree(config.config_root(), root)
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    before = _tree_hash(root)
    q7 = [str(p) for p in corpus_group("2026-gacmp-q7")]
    assert main(["ingest", "--no-report", "--lake", str(tmp_path / "lake"), *q7]) == 0
    assert _tree_hash(root) == before
    assert "draft" not in capfd.readouterr().out.lower()


@pytest.mark.corpus
def test_hoot_only_first_import(
    tmp_path: Path,
    init_root: Path,
    corpus_group: Callable[[str], list[Path]],
    capfd: pytest.CaptureFixture[str],
) -> None:
    from flashpoint.cli import main

    hoots = [str(p) for p in corpus_group("2026-gacmp-e10") if "19-29-15" in p.name]
    assert len(hoots) == 2
    main(["ingest", "--no-report", "--lake", str(tmp_path / "lake"), *hoots])
    captured = capfd.readouterr()
    assert not list((init_root / "robots").glob("*.toml"))
    assert "Draft robot" in captured.err


@pytest.mark.corpus
def test_opted_out_then_an_existing_lake_gets_its_draft(
    tmp_path: Path,
    init_root: Path,
    corpus_group: Callable[[str], list[Path]],
    capfd: pytest.CaptureFixture[str],
) -> None:
    from flashpoint.cli import main

    lake = tmp_path / "lake"
    q7 = [str(p) for p in corpus_group("2026-gacmp-q7")]
    assert main(["ingest", "--no-robot-draft", "--lake", str(lake), *q7]) == 0
    captured = capfd.readouterr()
    assert not list((init_root / "robots").glob("*.toml"))
    assert "Draft robot" in captured.err
    assert _index(lake)["2026gacmp_qm7"]["status"] == "no-aligned-samples"

    # Existing lake gets its draft: a plain derive (no --all), then a report.
    assert main(["derive", "--lake", str(lake)]) == 0
    assert (init_root / "robots" / "2026-comp.toml").is_file()
    assert main(["report", "--lake", str(lake)]) == 0
    assert _index(lake)["2026gacmp_qm7"]["status"] == "ok"


@pytest.mark.corpus
def test_no_configuration_root(
    tmp_path: Path,
    corpus_group: Callable[[str], list[Path]],
    capfd: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from flashpoint.cli import main

    root = tmp_path / "no-config"
    monkeypatch.setenv("FLASHPOINT_CONFIG", str(root))
    q7 = [str(p) for p in corpus_group("2026-gacmp-q7")]
    main(["ingest", "--no-report", "--lake", str(tmp_path / "lake"), *q7])
    assert not root.exists()
    assert "flashpoint init" in capfd.readouterr().err
