"""Docs content for non-Pro, AdvantageKit and the Anomalies beta (mvp-outside-team-logs 7.1)."""

import re
from pathlib import Path

from tests.test_docs_links import anchors

GUIDE = Path(__file__).resolve().parents[1] / "docs" / "guide"
ANOMALIES = GUIDE / "reference" / "views" / "anomalies.md"
ROBOTS = GUIDE / "reference" / "config" / "robots.md"
PRO = GUIDE / "robot-side" / "pro-and-temperature.md"
AKIT = GUIDE / "robot-side" / "advantagekit.md"
LINK = re.compile(r"\]\((?P<path>[^)#\s]+)?(?:#(?P<anchor>[^)\s]+))?\)")


def _text(page: Path) -> str:
    return page.read_text(encoding="utf-8")


def test_docs_page_marked_beta() -> None:
    text = _text(ANOMALIES)
    first = next(line for line in text.splitlines()[1:] if line.strip())
    assert "beta" in first.lower()
    targets = {
        (m["path"], m["anchor"]) for m in LINK.finditer(text) if m["path"] == "../config/robots.md"
    }
    assert ("../config/robots.md", "the-battery-proxy") in targets
    assert ("../config/robots.md", "siblings-slots-compared-with-each-other") in targets
    found = anchors(ROBOTS)
    for _, anchor in targets:
        assert anchor is None or anchor in found, anchor


def test_beta_section_describes_what_shipped() -> None:
    text = _text(ANOMALIES)
    assert "not in the app yet" not in text
    for phrase in ("BETA", "at most MED", "frame rate"):
        assert phrase in text, phrase


def test_robot_file_page_shows_adding_both_to_a_drafted_file() -> None:
    text = _text(ROBOTS)
    assert "drafted" in text.lower()
    assert re.search(r"(?m)^battery_proxy = ", text)
    assert text.count("[[siblings]]") >= 2


def test_pro_page_explains_n_a_and_needs_pro() -> None:
    text = _text(PRO)
    for phrase in ("NEEDS PRO", "N/A", "not logged (needs Phoenix Pro)", "one Pro-licensed device",
                   "flashpoint doctor"):  # fmt: skip
        assert phrase in text, phrase
    assert "changes that to" not in text  # the #75 promise is now shipped


def test_advantagekit_page() -> None:
    text = _text(AKIT).lower()
    for phrase in ("/driverstation", "akit_", "very low", "enable", "not tested on real logs"):
        assert phrase in text, phrase
