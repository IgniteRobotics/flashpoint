"""Troubleshooting drift checks (spec "Troubleshooting and FAQ", design D-e).

Each entry's ``**Message:** `...` `` quotes Flashpoint's output exactly. Split on its
`<placeholders>`, every literal piece of 8 or more characters must still be in the source
(`src/flashpoint/**/*.py` and `*.js`), so a reworded message fails the test, naming the entry.
"""

import re
from collections.abc import Callable
from pathlib import Path

import pytest

from flashpoint.cli import main

REPO = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "troubleshooting"
PAGE = REPO / "docs" / "guide" / "troubleshooting.md"
SOURCE = REPO / "src" / "flashpoint"

MESSAGE = re.compile(r"\*\*Message:\*\*\s*`([^`]+)`")
PLACEHOLDER = re.compile(r"<[^<>]+>")
# Adjacent string literals joined over a line break: Python's implicit concatenation
# ("abc"\n    " def", also f-strings) and JavaScript's 'abc' +\n 'def'.
JOINED = re.compile(r"""["']\s*\+?\s*\n\s*f?["']""")
MIN_PIECE = 8


def source_text(root: Path) -> str:
    files = sorted([*root.rglob("*.py"), *root.rglob("*.js")])
    return "\n".join(JOINED.sub("", f.read_text(encoding="utf-8")) for f in files)


def message_problems(page: Path, source: str, root: Path) -> list[str]:
    """Quoted messages with a literal piece that's no longer in the source."""
    problems = []
    entry = "(no heading)"
    for line in page.read_text(encoding="utf-8").splitlines():
        if line.startswith(("## ", "### ", "#### ")):
            entry = line.lstrip("#").strip()
        for quoted in MESSAGE.findall(line):
            for piece in PLACEHOLDER.split(quoted):
                if len(piece) >= MIN_PIECE and piece not in source:
                    name = page.relative_to(root).as_posix()
                    problems.append(f"{name}: {entry}: {piece!r} isn't in the source")
    return problems


def test_fixture_stale_message_is_named() -> None:
    source = source_text(FIXTURES / "src")
    assert message_problems(FIXTURES / "troubleshooting.md", source, FIXTURES) == [
        "troubleshooting.md: An old message: 'the lake is on fire at ' isn't in the source",
        "troubleshooting.md: An old message: ', call someone' isn't in the source",
    ]


def test_troubleshooting_messages_match_the_code() -> None:
    problems = message_problems(PAGE, source_text(SOURCE), REPO)
    assert problems == [], "\n".join(problems)


# --- quarantined and incomplete files, from real corpus files (corpus marker) ---------------


def entry_messages(page: Path, heading: str) -> list[str]:
    """The quoted messages under one `### heading`, in order."""
    text = page.read_text(encoding="utf-8")
    section = text.split(f"\n### {heading}\n", 1)[1].split("\n#", 1)[0]
    return MESSAGE.findall(section)


def message_pattern(quoted: str) -> re.Pattern[str]:
    """A quoted message as a regex: literal text, with each `<placeholder>` matching anything."""
    parts = PLACEHOLDER.split(quoted)
    return re.compile(".+?".join(re.escape(part) for part in parts))


def _ingest_output(paths: list[Path], lake: Path, capsys: pytest.CaptureFixture[str]) -> str:
    main(["ingest", "--no-derive", "--lake", str(lake), *map(str, paths)])
    return capsys.readouterr().out


@pytest.mark.corpus
def test_quarantine_entry_from_the_empty_hoot(
    corpus_group: Callable[[str], list[Path]], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (empty,) = corpus_group("2026-gadal-q11-empty")
    out = _ingest_output([empty], tmp_path / "lake", capsys)
    quoted = entry_messages(PAGE, "A file was quarantined")[0]
    lines = [line.strip() for line in out.splitlines()]
    assert any(message_pattern(quoted).fullmatch(line) for line in lines), (quoted, out)
    assert f"QUARANTINED {empty.name}: empty-file" in lines


@pytest.mark.corpus
def test_incomplete_entry_from_the_truncated_hoots(
    corpus_group: Callable[[str], list[Path]], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    files = corpus_group("2026-gacmp-e10") + corpus_group("2026-gacmp-e10-truncated")
    out = _ingest_output(files, tmp_path / "lake", capsys)
    lines = [line.strip() for line in out.splitlines()]
    pattern = message_pattern(entry_messages(PAGE, "A file was imported as incomplete")[0])
    truncated = next(f.name for f in files if "truncated" in f.name)
    expected = [  # the copy cut to 60 %, and a hoot the event's power-off cut short
        f"WARNING     {truncated}: short-coverage",
        "WARNING     GACMP_E10_rio_2026-04-11_19-29-15.hoot: incomplete-read",
    ]
    for line in expected:
        assert line in lines, (line, out)
        assert pattern.fullmatch(line), line
