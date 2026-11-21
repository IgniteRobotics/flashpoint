"""Robot pull is labelled beta until checked on a robot (spec: log-acquisition, "Robot pull
labelled beta until checked on a robot").

Written against the one maturity constant, so removing the label (task 8.5) is a change to the
constant, not to these tests.
"""

import argparse
import re
from pathlib import Path

from flashpoint.acquire.config import ROBOT_PULL_LABEL, robot_pull_label
from flashpoint.cli import _parser

REPO = Path(__file__).resolve().parents[2]
AUTO_IMPORT = REPO / "docs" / "guide" / "how-to" / "auto-import.md"


def _acquire() -> argparse.ArgumentParser:
    sub = next(a for a in _parser()._actions if isinstance(a, argparse._SubParsersAction))
    parser: argparse.ArgumentParser = sub.choices["acquire"]
    return parser


def _option_help(parser: argparse.ArgumentParser, flag: str) -> str:
    action = next(a for a in parser._actions if flag in a.option_strings)
    return str(action.help)


def test_help_text_labels_robot_pull_and_service_mode() -> None:
    label = robot_pull_label()
    parser = _acquire()
    text = " ".join(parser.format_help().split())
    if label is None:
        assert "beta" not in text.lower()
        return
    assert label == ROBOT_PULL_LABEL == "beta, tested on one robot"
    assert f"robot ({label})" in " ".join(str(parser.description).split())
    assert label in _option_help(parser, "--watch")  # running it as a service


def test_removable_volume_options_carry_no_label() -> None:
    parser = _acquire()
    assert "beta" not in _option_help(parser, "--no-usb").lower()


def test_guide_label_matches_the_app() -> None:
    """project-documentation "Beta label matches the app" (owned by mvp-documentation)."""
    label = robot_pull_label()
    text = AUTO_IMPORT.read_text(encoding="utf-8")
    labelled = re.findall(r"\*\*Beta: ([^*]+)\.\*\*", text)
    if label is None:
        assert labelled == []
        return
    head, background = text.split("## Run it in the background", 1)
    assert re.search(rf"\*\*Beta: {re.escape(label)}\.\*\*", head.split("\n## ", 1)[0])
    assert re.search(rf"\*\*Beta: {re.escape(label)}\.\*\*", background.split("\n## ", 1)[0])
    assert set(labelled) == {label}
