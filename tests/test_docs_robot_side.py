"""Robot-side drift checks (spec "Robot-side 'What to log' guide and samples").

Every Java block on a robot-side page is a contiguous run of lines in a sample file, and every
sample file says which WPILib and Phoenix 6 versions it was tested with.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "robot-side"
ROBOT_SIDE = REPO / "docs" / "guide" / "robot-side"
EXAMPLES = REPO / "examples" / "robot-java"

TESTED_WITH = re.compile(r"Tested with WPILib \S+, Phoenix 6 \S+")
FENCE = re.compile(r"^\s*(```|~~~)\s*([A-Za-z]*)")


def sample_files(examples: Path) -> list[Path]:
    """The `.java` sources under `examples`, minus build output (gversion's BuildConstants)."""
    return [
        path
        for path in sorted(examples.rglob("*.java"))
        if not {"build", ".gradle", "bin"} & set(path.relative_to(examples).parts)
    ]


def java_blocks(page: Path) -> list[tuple[int, list[str]]]:
    """Each fenced `java` block on a page: (line of its opening fence, its lines)."""
    blocks: list[tuple[int, list[str]]] = []
    lang: str | None = None  # None outside a fence
    for number, line in enumerate(page.read_text(encoding="utf-8").splitlines(), start=1):
        fence = FENCE.match(line)
        if lang is None:
            if fence:
                lang = fence[2]
                if lang == "java":
                    blocks.append((number, []))
        elif fence and not fence[2]:
            lang = None
        elif lang == "java":
            blocks[-1][1].append(line.rstrip())
    return blocks


def _contains(haystack: list[str], needle: list[str]) -> bool:
    if not needle:
        return True
    return any(haystack[i : i + len(needle)] == needle for i in range(len(haystack)))


def java_block_problems(pages: Path, examples: Path, root: Path) -> list[str]:
    """Java blocks that match no region of any `.java` file under `examples`."""
    sources = [
        [line.rstrip() for line in path.read_text(encoding="utf-8").splitlines()]
        for path in sample_files(examples)
    ]
    problems = []
    for page in sorted(pages.rglob("*.md")):
        for start, block in java_blocks(page):
            if not any(_contains(source, block) for source in sources):
                where = f"{page.relative_to(root).as_posix()}:{start}"
                folder = examples.relative_to(root).as_posix()
                problems.append(f"{where}: java block matches no region of {folder}")
    return problems


def version_line_problems(examples: Path, root: Path) -> list[str]:
    """`.java` files with no "Tested with WPILib <v>, Phoenix 6 <v>" line."""
    return [
        f"{path.relative_to(root).as_posix()}: no 'Tested with WPILib <v>, Phoenix 6 <v>' line"
        for path in sample_files(examples)
        if not TESTED_WITH.search(path.read_text(encoding="utf-8"))
    ]


def test_fixture_java_blocks() -> None:
    assert java_block_problems(
        FIXTURES / "docs", FIXTURES / "examples" / "robot-java", FIXTURES
    ) == ["docs/robot-side/drift.md:11: java block matches no region of examples/robot-java"]


def test_fixture_tested_with() -> None:
    assert version_line_problems(FIXTURES / "examples" / "robot-java", FIXTURES) == [
        "examples/robot-java/src/main/java/frc/robot/NoLine.java:"
        " no 'Tested with WPILib <v>, Phoenix 6 <v>' line"
    ]


def test_robot_side_java_blocks_are_in_the_samples() -> None:
    problems = java_block_problems(ROBOT_SIDE, EXAMPLES, REPO)
    assert problems == [], "\n".join(problems)


def test_every_sample_says_what_it_was_tested_with() -> None:
    assert sample_files(EXAMPLES), "no samples under examples/robot-java"
    problems = version_line_problems(EXAMPLES, REPO)
    assert problems == [], "\n".join(problems)
