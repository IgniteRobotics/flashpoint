"""Support matrix drift checks (spec "Support matrix and known limits", design D-e)."""

import importlib.util
from pathlib import Path
from types import ModuleType

REPO = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "support-matrix"


def _tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "gen_matrix", REPO / "tools" / "gen-support-matrix.py"
    )
    assert spec is not None and spec.loader is not None
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)
    return tool


TOOL = _tool()


def test_fixture_good_matrix_has_no_problems() -> None:
    assert TOOL.problems(TOOL.load(FIXTURES / "good.toml"), FIXTURES, {"q7"}) == []


def test_fixture_bad_matrix_names_table_row_and_column() -> None:
    assert TOOL.problems(TOOL.load(FIXTURES / "bad.toml"), FIXTURES, {"q7"}) == [
        "devices / Talon FX / Replay tracks: 'works' isn't one of ['supported',"
        " 'supported (needs Phoenix Pro)', 'rough', 'not tested on real logs', 'not tested',"
        " 'not supported']",
        "devices / Talon FX / Temperature: evidence 'q99' isn't a corpus group",
        "devices / Talon FX / Temperature: evidence tests/gone.py doesn't exist",
        "devices / Spark MAX / Replay tracks:"
        " 'not supported' needs an issue on the row or the cell",
        "devices / Spark MAX / Temperature: 'rough' needs a workaround and an issue",
        "devices / CANrange / extra: not a column of this table",
        "devices / CANrange / Replay tracks: 'supported' names no evidence",
        "devices / CANrange / Temperature: outside:2026-11 needs an issue",
        "devices / CANdi / Temperature: no value",
    ]


def test_fixture_rendering() -> None:
    table = TOOL.tables(TOOL.load(FIXTURES / "good.toml"))["devices"]
    assert table.splitlines()[:2] == ["| Device | Replay tracks | Temperature |", "|---|---|---|"]
    assert "| Spark MAX ([#12](https://github.com/IgniteRobotics/flashpoint/issues/12))" in table
    assert "rough: convert by hand ([#13](" in table
    assert (
        "corpus `q7`; [test](https://github.com/IgniteRobotics/flashpoint/blob/main/tests/test_widget.py)"
        in table
    )


def test_support_matrix_follows_the_rules() -> None:
    problems = TOOL.problems(TOOL.load(), REPO, TOOL.corpus_groups())
    assert problems == [], "\n".join(problems)


def test_support_matrix_page_is_current() -> None:
    text = TOOL.PAGE.read_text(encoding="utf-8")
    stale = TOOL.stale(text, TOOL.tables(TOOL.load()))
    assert stale == [], "\n".join([*stale, "run: python tools/gen-support-matrix.py"])


def test_support_matrix_has_the_spec_rows() -> None:
    by_id = {t["id"]: t for t in TOOL.load()["table"]}
    assert [r["name"] for r in by_id["devices"]["row"]] == [
        "Talon FX (Falcon 500, Kraken)", "Talon FXS", "Pigeon 2", "CANcoder", "CANrange",
        "CANdi", "CANivore", "Phoenix 5 devices", "REV Spark MAX and Spark Flex", "REV PDH",
        "CTRE PDP",
    ]  # fmt: skip
    assert [r["name"] for r in by_id["log-setups"]["row"]] == [
        "Pro hoots with wpilog", "non-Pro hoots with wpilog", "wpilog only", "hoots only",
        "AdvantageKit wpilog with hoots", "rio bus only",
    ]  # fmt: skip
    assert [r["name"] for r in by_id["platforms"]["row"]] == ["macOS", "Linux", "Windows"]
