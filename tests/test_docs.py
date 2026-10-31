"""Documentation drift checks: relative links resolve, and every CLI subcommand is documented."""

import argparse
import re
from pathlib import Path

from flashpoint.cli import _parser

REPO = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "docs"
CLI_REFERENCE = REPO / "docs" / "guide" / "cli-reference.md"

LINK = re.compile(r"!?\[[^\]]*\]\(([^)\s]*)[^)]*\)")
UNCLOSED_LINK = re.compile(r"\[[^\]]*\]\([^)]*$")
INLINE_CODE = re.compile(r"`[^`]*`")
SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")


def doc_files() -> list[Path]:
    return [
        REPO / "README.md",
        REPO / "deploy" / "README.md",
        *sorted((REPO / "docs" / "guide").glob("*.md")),
    ]


def link_problems(path: Path, root: Path) -> list[str]:
    """Every broken or malformed relative link in one Markdown file, as `file:line: message`."""
    problems: list[str] = []
    in_fence = False
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if raw.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        line = INLINE_CODE.sub("", raw)
        where = f"{path.relative_to(root).as_posix()}:{number}"
        for match in LINK.finditer(line):
            target = match[1]
            if not target or target.startswith("#") or SCHEME.match(target):
                continue
            if not (path.parent / target.split("#", 1)[0]).exists():
                problems.append(f"{where}: broken link {target}")
        if UNCLOSED_LINK.search(LINK.sub("", line)):
            problems.append(f"{where}: malformed link (no closing parenthesis)")
    return problems


def subcommands() -> list[str]:
    actions = [a for a in _parser()._actions if isinstance(a, argparse._SubParsersAction)]
    return sorted(name for action in actions for name in action.choices)


def test_fixture_good_links_pass() -> None:
    assert link_problems(FIXTURES / "good.md", FIXTURES) == []
    assert link_problems(FIXTURES / "sub" / "target.md", FIXTURES) == []


def test_fixture_broken_and_malformed_links_are_reported() -> None:
    assert link_problems(FIXTURES / "broken.md", FIXTURES) == [
        "broken.md:4: broken link missing/page.md",
        "broken.md:5: malformed link (no closing parenthesis)",
    ]


def test_relative_links_resolve() -> None:
    problems = [p for path in doc_files() if path.exists() for p in link_problems(path, REPO)]
    assert problems == [], "\n".join(problems)


REPO_URL = re.compile(r"^https://github\.com/IgniteRobotics/flashpoint/(?:blob|tree)/main/([^#]*)")


def readme_link_problems(path: Path, root: Path) -> list[str]:
    """README is also the PyPI page, where relative links break: every link must be absolute,
    and a link into this repository must name a file or folder that exists."""
    problems: list[str] = []
    in_fence = False
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if raw.lstrip().startswith(("```", "~~~")):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        for match in LINK.finditer(INLINE_CODE.sub("", raw)):
            target = match[1]
            if not SCHEME.match(target):
                problems.append(f"{number}: relative link {target} (breaks on PyPI)")
            elif (repo := REPO_URL.match(target)) and not (root / repo[1]).exists():
                problems.append(f"{number}: {target} names a path that doesn't exist")
    return problems


def test_fixture_readme_links() -> None:
    assert readme_link_problems(FIXTURES / "readme.md", REPO) == [
        "3: relative link docs/guide/README.md (breaks on PyPI)",
        "4: https://github.com/IgniteRobotics/flashpoint/blob/main/docs/nope.md"
        " names a path that doesn't exist",
    ]


def test_readme_links_work_on_pypi() -> None:
    problems = readme_link_problems(REPO / "README.md", REPO)
    assert problems == [], "\n".join(problems)


def test_every_subcommand_is_in_the_cli_reference() -> None:
    text = CLI_REFERENCE.read_text(encoding="utf-8") if CLI_REFERENCE.exists() else ""
    headings = set(re.findall(r"^## `flashpoint ([a-z-]+)`", text, flags=re.MULTILINE))
    missing = [name for name in subcommands() if name not in headings]
    assert missing == [], f"undocumented in {CLI_REFERENCE.name}: {', '.join(missing)}"


# --- live-can-health: "Guide matches the contract" ----------------------------------------

LIVE_GUIDE = REPO / "docs" / "guide" / "live-can-health.md"
GUIDE_TYPES = {"integer": "int", "double": "double", "boolean": "boolean", "string": "string"}
CONTRACT_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|\s*(\w+)\s*\|")


def guide_contract() -> dict[str, str]:
    """The guide's contract table: topic pattern (relative to the root) -> contract type."""
    text = LIVE_GUIDE.read_text(encoding="utf-8")
    section = text.split("\n## The contract\n", 1)[1].split("\n## ", 1)[0]
    rows = [CONTRACT_ROW.match(line) for line in section.splitlines()]
    return {m[1]: GUIDE_TYPES.get(m[2].lower(), m[2]) for m in rows if m}


def test_guide_matches_the_contract() -> None:
    from flashpoint.live import contract

    expected = dict(contract.TOP_FIELDS)
    expected |= {f"<bus>/<Model>-<id>/{f}": t for f, t in contract.DEVICE_FIELDS.items()}
    expected |= {f"<bus>/{contract.BUS_DIR}/{f}": t for f, t in contract.BUS_FIELDS.items()}
    assert guide_contract() == expected
    text = LIVE_GUIDE.read_text(encoding="utf-8")
    assert f"`{contract.ROOT}`" in text
    assert f"Contract version: `{contract.CONTRACT_VERSION}`" in text
    for bit, name in contract.FAULT_BITS.items():
        assert f"{bit} {name}" in text
