"""Screenshots on the docs pages, one per row (spec project-documentation, "README and docs home
screenshots, one per row"; docs-visual-polish task 6.1). The first dev deploy showed the docs
home's three screenshots tiled by a `grid` block: no page puts an image in a grid or a table, and
the home page captions each view's screenshot as the README does."""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
GUIDE = REPO / "docs" / "guide"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "docs-layout"
IMAGE = re.compile(r"!\[[^\]]*\]\(\s*<?([^)\s>]+)|<(?:img|video)\b", re.IGNORECASE)
WHOLE_IMAGE = re.compile(r"\s*!\[[^\]]*\]\([^)\s]+\)\s*")
GRID = re.compile(r"<div\b[^>]*\bclass=[\"'][^\"']*\bgrid\b", re.IGNORECASE)
FENCE = re.compile(r"^\s*(```|~~~)")
# Each view screenshot on the home page, the view its caption names, and the label it carries.
VIEWS: dict[str, tuple[str, str | None]] = {
    "replay.png": ("Replay", None),
    "history.png": ("History", None),
    "anomalies.png": ("Anomalies", "beta"),
}


def tiled_image_problems(text: str) -> list[str]:
    """Each line that puts an image in a `grid` block or a table row."""
    problems: list[str] = []
    depth = 0  # open <div>s since a grid opened; 0 when outside any grid
    in_fence = False
    for number, line in enumerate(text.splitlines(), start=1):
        if FENCE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if depth == 0 and GRID.search(line):
            depth = 1
        elif depth:
            depth += line.count("<div") - line.count("</div>")
        if not IMAGE.search(line):
            continue
        if depth:
            problems.append(f"{number}: image inside a grid")
        elif line.lstrip().startswith("|"):
            problems.append(f"{number}: image inside a table row")
    return problems


def caption_problems(text: str) -> list[str]:
    """Each view screenshot alone in its paragraph, after a line naming its view (and label)."""
    lines = text.splitlines()
    problems: list[str] = []
    for name, (view, label) in VIEWS.items():
        found = [n for n, line in enumerate(lines, start=1) if f"assets/media/{name}" in line]
        if not found:
            problems.append(f"{name} isn't on the page")
            continue
        number = found[0]
        line = lines[number - 1]
        before = lines[number - 2] if number >= 2 else ""
        after = lines[number] if number < len(lines) else ""
        if not WHOLE_IMAGE.fullmatch(line) or before.strip() or after.strip():
            problems.append(f"{number}: {name} isn't alone in its paragraph")
            continue
        caption = next((t for t in reversed(lines[: number - 1]) if t.strip()), "")
        if view.lower() not in caption.lower():
            problems.append(f"{number}: the line before {name} doesn't name {view}")
        if label and label not in caption.lower():
            problems.append(f"{number}: the line before {name} doesn't say '{label}'")
    return problems


def test_fixture_grid_and_table_images_are_refused() -> None:
    text = (FIXTURES / "grid.md").read_text(encoding="utf-8")
    assert tiled_image_problems(text) == [
        "5: image inside a grid",
        "7: image inside a grid",
        "13: image inside a table row",
    ]


def test_fixture_captioned_home_passes() -> None:
    text = (FIXTURES / "captioned.md").read_text(encoding="utf-8")
    assert tiled_image_problems(text) == []
    assert caption_problems(text) == []


def test_no_docs_page_tiles_its_images() -> None:
    problems = [
        f"{page.relative_to(GUIDE).as_posix()}:{problem}"
        for page in sorted(GUIDE.rglob("*.md"))
        for problem in tiled_image_problems(page.read_text(encoding="utf-8"))
    ]
    assert problems == [], "\n".join(problems)


def test_docs_home_screenshots_one_per_row() -> None:
    problems = caption_problems((GUIDE / "index.md").read_text(encoding="utf-8"))
    assert problems == [], "\n".join(problems)
