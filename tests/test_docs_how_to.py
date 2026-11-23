"""How-to shape (spec "Documentation structure": "How-to shape", "Required how-tos exist").

Every how-to has "Before you start", numbered steps, "Check it worked" and a link into
Troubleshooting, and the how-to set has a page for each task the spec lists.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HOW_TO = REPO / "docs" / "guide" / "how-to"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "how-to"

# The spec's list of how-tos, by the page that does each one (design D-d).
REQUIRED = {
    "set-up-a-robot-file.md": "set up a robot file (draft it from a log)",
    "import-from-usb.md": "import logs from a USB stick",
    "auto-import.md": "import logs from the robot automatically",
    "back-up-the-lake.md": "back up the lake and run the restore drill",
    "move-to-another-laptop.md": "move Flashpoint to another laptop",
    "start-a-new-season.md": "start a new season",
    "replay-on-a-usb-stick.md": "take Replay on a USB stick",
    "share-on-the-pit-network.md": "show the app to others on the pit network",
    "live-can-health.md": "watch live CAN health during a shake test",
    "triage-an-anomaly.md": "triage an anomaly",
    "open-in-advantagescope.md": "open a match in AdvantageScope",
    "share-logs-privately.md": "share logs privately for a bug report",
    "update-flashpoint.md": "update Flashpoint",
    "install-offline.md": "install without internet",
    "team-name-and-colours.md": "set the team name and colours",
}
HEADING = re.compile(r"^#{2,3} (.+?)\s*$", re.MULTILINE)
NUMBERED = re.compile(r"^\d+\. \S", re.MULTILINE)
TROUBLESHOOTING = re.compile(r"\]\((?:\.\./)+troubleshooting\.md(?:#[^)]*)?\)")


def shape_problems(page: Path, root: Path) -> list[str]:
    """The parts of the how-to shape a page is missing."""
    text = page.read_text(encoding="utf-8")
    headings = {h.lower() for h in HEADING.findall(text)}
    name = page.relative_to(root).as_posix()
    problems = [
        f"{name}: no '{part}' section"
        for part in ("Before you start", "Check it worked")
        if part.lower() not in headings
    ]
    if not NUMBERED.search(text):
        problems.append(f"{name}: no numbered steps")
    if not TROUBLESHOOTING.search(text):
        problems.append(f"{name}: no link into troubleshooting.md")
    return problems


def test_fixture_good_how_to() -> None:
    assert shape_problems(FIXTURES / "good.md", FIXTURES) == []


def test_fixture_how_to_missing_check_it_worked_fails_by_name() -> None:
    assert shape_problems(FIXTURES / "no-check.md", FIXTURES) == [
        "no-check.md: no 'Check it worked' section"
    ]


def test_fixture_how_to_missing_steps_and_link() -> None:
    assert shape_problems(FIXTURES / "no-steps.md", FIXTURES) == [
        "no-steps.md: no numbered steps",
        "no-steps.md: no link into troubleshooting.md",
    ]


def test_every_how_to_has_the_shape() -> None:
    problems = [p for page in sorted(HOW_TO.glob("*.md")) for p in shape_problems(page, REPO)]
    assert problems == [], "\n".join(problems)


def test_the_required_how_tos_exist() -> None:
    missing = [
        f"{name} ({task})" for name, task in REQUIRED.items() if not (HOW_TO / name).is_file()
    ]
    assert missing == [], "missing how-tos: " + ", ".join(missing)
