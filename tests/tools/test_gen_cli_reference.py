"""The generated CLI reference (spec project-documentation, "CLI reference"; design D-e)."""

import argparse
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "gen-cli-reference.py"
FIXTURE = REPO / "tests" / "fixtures" / "cli-reference" / "reference.md"


def _tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("gen_cli_reference", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _parser(
    *, extra: str | None = None, help_for_fast: str | None = "skip the slow part"
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tool")
    sub = parser.add_subparsers(dest="command")
    ingest = sub.add_parser("ingest")
    ingest.add_argument("paths", nargs="+", type=Path, help="files or folders to read")
    ingest.add_argument(
        "--profile",
        choices=("health", "all"),
        default="health",
        help="signals to export: health or all | about 3x larger",
    )
    ingest.add_argument("--fast", action="store_true", help=help_for_fast)
    if extra:
        ingest.add_argument(extra, action="store_true", help="a new option")
    doctor = sub.add_parser("doctor")
    doctor.add_argument("--lake", type=Path, help="lake root")
    return parser


def test_tables_match_the_fixture_page() -> None:
    tool = _tool()
    text = FIXTURE.read_text(encoding="utf-8")
    assert tool.problems(text, tool.tables(_parser())) == []
    assert tool.render(text, tool.tables(_parser())) == text


def test_an_option_without_help_fails_naming_it() -> None:
    with pytest.raises(ValueError, match=r"ingest: --fast has no help text"):
        _tool().tables(_parser(help_for_fast=None))


def test_an_option_added_without_regenerating_is_named() -> None:
    tool = _tool()
    problems = tool.problems(
        FIXTURE.read_text(encoding="utf-8"), tool.tables(_parser(extra="--new"))
    )
    assert problems == ["ingest: --new isn't in the reference"]


def test_a_changed_help_text_is_reported() -> None:
    tool = _tool()
    text = FIXTURE.read_text(encoding="utf-8").replace("skip the slow part", "old words")
    assert tool.problems(text, tool.tables(_parser())) == [
        "ingest: the table is out of date (help text or a default changed)"
    ]


def test_a_subcommand_without_markers_is_named() -> None:
    tool = _tool()
    text = FIXTURE.read_text(encoding="utf-8").replace("generated:cli:doctor", "nothing")
    assert tool.problems(text, tool.tables(_parser())) == ["doctor: no generated table"]
    with pytest.raises(ValueError, match="doctor"):
        tool.render(text, tool.tables(_parser()))


def test_render_replaces_only_the_generated_blocks() -> None:
    tool = _tool()
    text = FIXTURE.read_text(encoding="utf-8")
    stale = text.replace("skip the slow part", "old words")
    assert tool.render(stale, tool.tables(_parser())) == text
