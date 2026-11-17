"""CONTRIBUTING, SECURITY and the code of conduct (spec public-release, "Community files")."""

import re
from pathlib import Path

import pytest

from tests.test_docs import link_problems

REPO = Path(__file__).resolve().parents[1]
CONTRIBUTING = REPO / "CONTRIBUTING.md"
SECURITY = REPO / "SECURITY.md"
CODE_OF_CONDUCT = REPO / "CODE_OF_CONDUCT.md"
REPORT_FORM = "https://github.com/IgniteRobotics/flashpoint/security/advisories/new"
DISCUSSIONS_QA = "https://github.com/IgniteRobotics/flashpoint/discussions/categories/q-a"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


@pytest.mark.parametrize("path", [CONTRIBUTING, SECURITY, CODE_OF_CONDUCT], ids=lambda p: p.name)
def test_community_file_exists_and_its_links_resolve(path: Path) -> None:
    assert path.is_file(), f"{path.name} is missing"
    assert link_problems(path, REPO) == []


def test_contributing_says_pull_requests_are_collaborators_only() -> None:
    text = _text(CONTRIBUTING)
    assert re.search(r"pull requests.*collaborators", text, re.IGNORECASE | re.DOTALL)
    assert "fork" in text.lower()


def test_contributing_says_how_to_share_logs_privately() -> None:
    text = _text(CONTRIBUTING).lower()
    assert "privately" in text
    assert ".wpilog" in text
    assert ".hoot" in text


def test_contributing_gives_the_build_season_response_time() -> None:
    text = _text(CONTRIBUTING)
    assert re.search(r"build season[^.]*within a week", text, re.IGNORECASE)


def test_contributing_points_questions_and_bugs_at_the_right_place() -> None:
    text = _text(CONTRIBUTING)
    assert DISCUSSIONS_QA in text
    assert "https://github.com/IgniteRobotics/flashpoint/issues/new/choose" in text


def test_security_names_the_supported_versions_and_private_reporting() -> None:
    text = _text(SECURITY)
    assert re.search(r"latest minor", text, re.IGNORECASE)
    assert REPORT_FORM in text
    assert "public issue" in text.lower()


def test_code_of_conduct_is_the_contributor_covenant_with_a_private_contact() -> None:
    text = _text(CODE_OF_CONDUCT)
    assert "Contributor Covenant" in text
    assert "version 2.1" in text
    assert REPORT_FORM in text
    assert "[INSERT" not in text
