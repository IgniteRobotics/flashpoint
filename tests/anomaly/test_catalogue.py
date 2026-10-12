from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.anomaly.catalogue import (
    CatalogueError,
    causes_for,
    load_causes,
    load_labels,
)
from flashpoint.semantics.robot_config import load_robots

ROBOTS = load_robots(config.config_root() / "robots")


def _labels(tmp_path: Path, text: str) -> Path:
    (tmp_path / "failure-labels.toml").write_text(text)
    return tmp_path


def _causes(tmp_path: Path, text: str) -> Path:
    (tmp_path / "anomaly-causes.toml").write_text(text)
    return tmp_path


LABEL = """
[[label]]
robot = "2026-comp"
slot = "intake-roller"
match = "2026johnson_qm105"
kind = "failure"
note = "rebooted"
"""


def test_seeded_labels_load_and_resolve() -> None:
    labels = load_labels(config.config_root(), ROBOTS)
    assert len(labels) >= 3
    by_slot = {(lb.match, lb.slot): lb for lb in labels}
    roller = by_slot[("2026johnson_qm105", "intake-roller")]
    assert (roller.robot, roller.kind, roller.metric) == ("2026-comp", "failure", "reboots")
    follower = by_slot[("2026johnson_qm105", "intake-extension-follower")]
    assert follower.kind == "degradation"
    power = by_slot[("2026gacmp_qm7", "power")]
    assert (power.kind, power.metric) == ("failure", "brownouts")
    assert all(lb.robot and lb.slot for lb in labels)


def test_label_by_unit_resolves_no_slot(tmp_path: Path) -> None:
    text = LABEL.replace('slot = "intake-roller"', 'unit = "ctre:ROLLER-A"')
    (label,) = load_labels(_labels(tmp_path, text), ROBOTS)
    assert (label.unit, label.slot) == ("ctre:ROLLER-A", None)


def test_label_by_date(tmp_path: Path) -> None:
    text = LABEL.replace('match = "2026johnson_qm105"', 'date = "2026-03-20"')
    (label,) = load_labels(_labels(tmp_path, text), ROBOTS)
    assert label.match is None and str(label.date) == "2026-03-20"


def test_missing_file_has_no_labels(tmp_path: Path) -> None:
    assert load_labels(tmp_path, ROBOTS) == []


@pytest.mark.parametrize(
    ("old", "new", "pattern"),
    [
        ('slot = "intake-roller"', 'slot = "drive-xx"', r"label 1.*drive-xx"),
        ('slot = "intake-roller"', "", r"label 1.*unit or a slot"),
        ('robot = "2026-comp"', 'robot = "2031-bot"', r"label 1.*2031-bot"),
        ('match = "2026johnson_qm105"', 'match = "johnson q105"', r"label 1.*johnson q105"),
        ('match = "2026johnson_qm105"', 'date = "2026-13-01"', r"label 1.*date"),
        ('match = "2026johnson_qm105"', "", r"label 1.*match key or a date"),
        ('kind = "failure"', 'kind = "broken"', r"label 1.*kind"),
        ('note = "rebooted"', 'note = "x"\ncolour = "red"', r"label 1.*colour"),
        ('kind = "failure"', 'kind = "failure"\nmetric = "volts"', r"label 1.*volts"),
    ],
)
def test_bad_label_names_the_label(tmp_path: Path, old: str, new: str, pattern: str) -> None:
    with pytest.raises(CatalogueError, match=rf"(?s)failure-labels\.toml.*{pattern}"):
        load_labels(_labels(tmp_path, LABEL.replace(old, new)), ROBOTS)


def test_seeded_causes() -> None:
    causes = load_causes(config.config_root())
    assert causes_for(causes, "SPEC", "reboots", "intake")
    assert causes_for(causes, "SPEC", "brownouts", "power")
    drive = causes_for(causes, "DRIFT", "stator_current_p95", "drivetrain")
    assert "Wheel bearing wear" in drive


def test_cause_lookup_prefers_subsystem_then_general(tmp_path: Path) -> None:
    causes = load_causes(
        _causes(
            tmp_path,
            '[[cause]]\nmethod = "STEP"\nmetric = "temp_max_c"\ntext = "general"\n'
            '[[cause]]\nmethod = "STEP"\nmetric = "temp_max_c"\nsubsystem = "shooter"\n'
            'text = "shooter only"\n',
        )
    )
    assert causes_for(causes, "STEP", "temp_max_c", "shooter") == ["shooter only", "general"]
    assert causes_for(causes, "STEP", "temp_max_c", "intake") == ["general"]
    assert causes_for(causes, "DRIFT", "temp_max_c", "intake") == []


@pytest.mark.parametrize(
    ("text", "pattern"),
    [
        ('[[cause]]\nmethod = "GUESS"\nmetric = "temp_max_c"\ntext = "x"\n', r"cause 1.*GUESS"),
        ('[[cause]]\nmethod = "STEP"\nmetric = "volts"\ntext = "x"\n', r"cause 1.*volts"),
        ('[[cause]]\nmethod = "STEP"\nmetric = "temp_max_c"\ntext = ""\n', r"cause 1.*text"),
        ('[[cause]]\nmethod = "STEP"\nmetric = "temp_max_c"\ntext = "x"\nwhy = 1\n', r"why"),
        ("[[reason]]\n", r"reason"),
    ],
)
def test_bad_causes(tmp_path: Path, text: str, pattern: str) -> None:
    with pytest.raises(CatalogueError, match=rf"(?s)anomaly-causes\.toml.*{pattern}"):
        load_causes(_causes(tmp_path, text))
