"""Maturity labels in the docs (spec project-documentation, "Feature maturity labels";
mvp-documentation task 14.1). The labels live in docs/guide/maturity.toml."""

import re
import tomllib
from pathlib import Path
from typing import Any

from flashpoint.acquire.config import ROBOT_PULL_LABEL, robot_pull_label

REPO = Path(__file__).resolve().parents[1]
GUIDE = REPO / "docs" / "guide"
FEATURES: list[dict[str, Any]] = tomllib.loads(
    (GUIDE / "maturity.toml").read_text(encoding="utf-8")
)["feature"]
PITCH_PAGES = [REPO / "README.md", GUIDE / "why.md"]
# An image's alt text comes from the media manifest; the text around the image carries the label.
IMAGE = re.compile(r"!\[[^\]]*\]\([^)]*\)")


def unlabelled_mentions(path: Path, feature: dict[str, Any]) -> list[str]:
    """Lines naming the feature without its label, outside code blocks and image alt text."""
    problems: list[str] = []
    in_fence = False
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if line.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
            continue
        lower = IMAGE.sub("", line).lower()
        named = any(mention in lower for mention in feature["mentions"])
        if not in_fence and named and feature["label"] not in lower:
            problems.append(f"{path.name}:{number}: {feature['name']} without '{feature['label']}'")
    return problems


def test_maturity_file_is_well_formed() -> None:
    names = [feature["name"] for feature in FEATURES]
    assert len(names) == len(set(names))
    for feature in FEATURES:
        assert feature["label"] and feature["mentions"] and feature["pages"], feature["name"]
        assert all(mention == mention.lower() for mention in feature["mentions"])
        assert feature["label"] == feature["label"].lower()


def test_robot_pull_label_matches_the_app() -> None:
    listed = {feature["name"]: feature["label"] for feature in FEATURES}
    if robot_pull_label() is None:
        assert "Robot pull" not in listed, "the app dropped robot pull's label; drop it here too"
    else:
        assert listed.get("Robot pull") == ROBOT_PULL_LABEL


def test_feature_pages_show_the_label_at_the_top() -> None:
    problems = []
    for feature in FEATURES:
        for page in feature["pages"]:
            top = "\n".join((GUIDE / page).read_text(encoding="utf-8").splitlines()[:10])
            if feature["label"] not in top.lower():
                problems.append(f"{page}: no '{feature['label']}' in its first 10 lines")
    assert problems == [], "\n".join(problems)


def test_support_matrix_shows_each_label() -> None:
    lines = (GUIDE / "support-matrix.md").read_text(encoding="utf-8").lower().splitlines()
    for feature in FEATURES:
        assert any(
            feature["name"].lower() in line and feature["label"] in line for line in lines
        ), f"the support matrix doesn't label {feature['name']} '{feature['label']}'"


def test_readme_and_why_label_every_mention() -> None:
    problems = [
        problem
        for path in PITCH_PAGES
        for feature in FEATURES
        for problem in unlabelled_mentions(path, feature)
    ]
    assert problems == [], "\n".join(problems)


def test_an_unlabelled_mention_is_caught(tmp_path: Path) -> None:
    page = tmp_path / "why.md"
    page.write_text("Anomalies (beta) flags motors.\n\nTriage each anomaly.\n", encoding="utf-8")
    anomalies = next(feature for feature in FEATURES if feature["name"] == "Anomalies")
    assert unlabelled_mentions(page, anomalies) == ["why.md:3: Anomalies without 'beta'"]
