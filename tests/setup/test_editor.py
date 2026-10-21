import difflib
import shutil
import time
import tomllib
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.setup import editor
from flashpoint.setup.editor import SaveError

SAVE_BUDGET_S = 0.2
IGNITE = config.config_root()
ROBOT = "robots/2026-comp.toml"


@pytest.fixture
def root(tmp_path: Path) -> Path:
    target = tmp_path / "cfg"
    shutil.copytree(IGNITE, target)
    return target


def _robot(root: Path) -> dict[str, Any]:
    with (root / ROBOT).open("rb") as f:
        return tomllib.load(f)


def _with_slot(data: dict[str, Any], slot_id: str, **changes: Any) -> dict[str, Any]:
    slots = [dict(s, **changes) if s["id"] == slot_id else s for s in data["slot"]]
    return data | {"slot": slots}


def _changed_lines(before: str, after: str) -> list[str]:
    diff = difflib.unified_diff(before.splitlines(), after.splitlines(), lineterm="", n=0)
    return [line for line in diff if line[:1] in "+-" and line[:3] not in ("+++", "---")]


def test_valid_slot_edit(root: Path) -> None:
    before = (root / ROBOT).read_text()
    sha = editor.file_sha(root, ROBOT)
    result = editor.save_robot(
        root, "2026-comp", _with_slot(_robot(root), "drive-fl", can_id=19), sha
    )
    after = (root / ROBOT).read_text()
    assert _changed_lines(before, after) == ["-can_id = 11", "+can_id = 19"]
    assert result.sha256 == editor.file_sha(root, ROBOT) != sha
    (backup,) = (root / ".backups" / ROBOT).iterdir()
    assert backup.read_text() == before and backup.suffix == ".toml"
    assert result.affects == "flashpoint derive"


def test_duplicate_slot_refused(root: Path) -> None:
    before = (root / ROBOT).read_bytes()
    data = _with_slot(_robot(root), "steer-fl", can_id=11)
    with pytest.raises(SaveError, match=r"(?s)drive-fl.*steer-fl") as caught:
        editor.save_robot(root, "2026-comp", data, editor.file_sha(root, ROBOT))
    assert caught.value.status == 400 and ROBOT in str(caught.value)
    assert (root / ROBOT).read_bytes() == before
    assert not (root / ".backups").exists()


def test_changed_on_disk(root: Path) -> None:
    sha = editor.file_sha(root, "report.toml")  # the bundled default the view loaded
    (root / "report.toml").write_text("[thresholds]\ntemp_warn_c = 60.0  # hand edit\n")
    with pytest.raises(SaveError, match="report.toml") as caught:
        editor.save_thresholds(root, "report.toml", {"thresholds": {"temp_warn_c": 61.0}}, sha)
    assert caught.value.status == 409
    assert "hand edit" in (root / "report.toml").read_text()


def test_threshold_save_starts_from_the_bundled_default(root: Path) -> None:
    assert config.is_default(root, "report.toml")
    sha = editor.file_sha(root, "report.toml")
    result = editor.save_thresholds(root, "report.toml", {"thresholds": {"temp_warn_c": 60.0}}, sha)
    text = (root / "report.toml").read_text()
    assert "temp_warn_c = 60.0" in text
    assert "# Thresholds for Replay rule markers" in text  # the default's comments came along
    assert not (root / ".backups").exists()  # nothing of the team's was replaced
    assert result.affects == "flashpoint report"
    anomaly = editor.save_thresholds(
        root, "anomaly.toml", {"spec": {"stall_med_s": 6.0}}, editor.file_sha(root, "anomaly.toml")
    )
    assert anomaly.affects == "flashpoint detect"


def test_invalid_threshold_refused_with_the_loader_message(root: Path) -> None:
    sha = editor.file_sha(root, "report.toml")
    with pytest.raises(SaveError, match="dropout_warn_s"):
        editor.save_thresholds(root, "report.toml", {"thresholds": {"dropout_warn_s": 0.01}}, sha)
    assert config.is_default(root, "report.toml")


def test_unknown_threshold_file_refused(root: Path) -> None:
    with pytest.raises(SaveError) as caught:
        editor.save_thresholds(root, "../acquire.toml", {}, None)
    assert caught.value.status == 404


def test_team_save(root: Path, tmp_path: Path) -> None:
    sha = editor.file_sha(root, "acquire.toml")
    result = editor.save_team(root, {"team": 254, "lake": str(tmp_path / "lake")}, sha)
    data = tomllib.loads((root / "acquire.toml").read_text())
    assert data["team"] == 254 and data["lake"] == str(tmp_path / "lake")
    assert "# Ignite Robotics" in (root / "acquire.toml").read_text()
    assert "restart" in result.affects
    with pytest.raises(SaveError, match="team"):
        editor.save_team(root, {"team": 0}, result.sha256)
    removed = editor.save_team(root, {"lake": None}, result.sha256)
    assert "lake" not in tomllib.loads((root / "acquire.toml").read_text())
    assert removed.sha256 == editor.file_sha(root, "acquire.toml")


def test_cross_file_robot_checks(root: Path) -> None:
    data = _robot(root)
    signal = dict(data["signal"][0], root="nope")
    with pytest.raises(SaveError, match="nope"):
        editor.save_robot(
            root,
            "2026-comp",
            data | {"signal": [signal, *data["signal"][1:]]},
            editor.file_sha(root, ROBOT),
        )
    with pytest.raises(SaveError, match="battery proxy"):
        editor.save_robot(
            root, "2026-comp", data | {"battery_proxy": "ghost"}, editor.file_sha(root, ROBOT)
        )


def test_slots_added_and_removed(root: Path) -> None:
    data = _robot(root)
    new = {
        "id": "climber",
        "bus": "rio",
        "model": "TalonFX",
        "can_id": 20,
        "subsystem": "climber",
        "role": "climb",
    }
    slots = [s for s in data["slot"] if s["id"] != "hood"] + [new]
    siblings = data.get("siblings", [])
    result = editor.save_robot(
        root,
        "2026-comp",
        data | {"slot": slots, "siblings": siblings},
        editor.file_sha(root, ROBOT),
    )
    ids = [s["id"] for s in _robot(root)["slot"]]
    assert "hood" not in ids and ids[-1] == "climber"
    assert result.sha256 == editor.file_sha(root, ROBOT)


def test_new_robot_and_existing_robot_id(root: Path) -> None:
    data = _robot(root)
    with pytest.raises(SaveError, match="2026-comp") as caught:
        editor.save_robot(root, "2026-comp", data, None, new=True)
    assert caught.value.status == 409
    copy = data | {"robot": "2026-practice"}
    editor.save_robot(root, "2026-practice", copy, None, new=True)
    assert _robot(root)["robot"] == "2026-comp"
    with (root / "robots" / "2026-practice.toml").open("rb") as f:
        assert tomllib.load(f)["robot"] == "2026-practice"
    with pytest.raises(SaveError, match="file name"):
        editor.save_robot(root, "other", copy | {"robot": "mismatch"}, None, new=True)


@pytest.mark.parametrize("robot_id", ["../evil", "a/b", "", "UPPER"])
def test_robot_id_is_a_file_name(root: Path, robot_id: str) -> None:
    with pytest.raises(SaveError):
        editor.save_robot(root, robot_id, _robot(root) | {"robot": robot_id}, None, new=True)


def test_backups_are_pruned_to_20(root: Path) -> None:
    sha = editor.file_sha(root, ROBOT)
    for can_id in range(15, 41):
        if can_id in (21, 22, 23, 31, 32, 33):
            continue
        sha = editor.save_robot(
            root, "2026-comp", _with_slot(_robot(root), "drive-fl", can_id=can_id), sha
        ).sha256
    backups = sorted((root / ".backups" / ROBOT).iterdir())
    assert len(backups) == editor.KEEP_BACKUPS == 20
    assert "can_id = 39" in backups[-1].read_text()  # the newest backups are kept


def test_units_save(root: Path) -> None:
    sha = editor.file_sha(root, editor.UNITS_FILE)
    rows = [
        {
            "serial": "6E9415C3394C4853",
            "model": "Talon FX",
            "robot": "2026-comp",
            "bus": "rio",
            "can_id": "1",
            "role": "intake",
            "label": "M-001",
            "first_installed": "2026-01-20",
            "notes": "",
        },
        {
            "serial": "AAAA",
            "model": "Talon FX",
            "robot": "shelf",
            "bus": "",
            "can_id": "",
            "role": "",
            "label": "M-002",
            "first_installed": "",
            "notes": "spare",
        },
    ]
    editor.save_units(root, rows, sha)
    lines = (root / editor.UNITS_FILE).read_text().splitlines()
    assert lines[0] == ",".join(editor.UNIT_COLUMNS) and len(lines) == 3


def test_duplicate_serial(root: Path) -> None:
    sha = editor.file_sha(root, editor.UNITS_FILE)
    row = dict.fromkeys(editor.UNIT_COLUMNS, "") | {"serial": "AAAA", "label": "M-001"}
    before = (root / editor.UNITS_FILE).read_bytes()
    with pytest.raises(SaveError, match="rows 1 and 2"):
        editor.save_units(root, [row, row | {"label": "M-002"}], sha)
    with pytest.raises(SaveError, match=r"label.*rows 1 and 2"):
        editor.save_units(root, [row, row | {"serial": "BBBB"}], sha)
    assert (root / editor.UNITS_FILE).read_bytes() == before


def test_save_is_fast(root: Path) -> None:
    sha = editor.file_sha(root, ROBOT)
    started = time.perf_counter()
    editor.save_robot(root, "2026-comp", _with_slot(_robot(root), "hood", role="hood angle"), sha)
    assert time.perf_counter() - started < SAVE_BUDGET_S


def test_unchanged_team_values_keep_their_bytes(root: Path) -> None:
    (root / "acquire.toml").write_text("team = 6829  # Ignite\npoll_s = 30\n")
    sha = editor.file_sha(root, "acquire.toml")
    editor.save_team(root, {"team": 6829, "poll_s": 20}, sha)
    assert (root / "acquire.toml").read_text() == "team = 6829  # Ignite\npoll_s = 20\n"
