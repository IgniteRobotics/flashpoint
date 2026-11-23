"""README screenshot layout (spec project-documentation, "README screenshots, one per row";
docs-visual-polish task 1.1). Each view screenshot sits alone in its paragraph, full width, after a
line naming its view; none is squeezed into a table."""

import re
from pathlib import Path

from tests.test_docs_readme import MARKDOWN_IMAGE, SITE_MEDIA, _lines_outside_fences

REPO = Path(__file__).resolve().parents[1]
README = REPO / "README.md"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "readme"
# Each view screenshot, the view its caption names, and the label the caption carries.
VIEWS: dict[str, tuple[str, str | None]] = {
    "replay.png": ("Replay", None),
    "history.png": ("History", None),
    "anomalies.png": ("Anomalies", "beta"),
}
WHOLE_IMAGE = re.compile(r"\s*!\[[^\]]*\]\([^)\s]+\)\s*")


def table_image_problems(text: str) -> list[str]:
    """Each line of a table row that holds an image."""
    return [
        f"{number}: image inside a table row"
        for number, line in _lines_outside_fences(text)
        if line.lstrip().startswith("|") and MARKDOWN_IMAGE.search(line)
    ]


def caption_problems(text: str) -> list[str]:
    """Each view screenshot alone in its paragraph, after a line naming its view (and label)."""
    lines = text.splitlines()
    outside = _lines_outside_fences(text)
    problems: list[str] = []
    for name, (view, label) in VIEWS.items():
        found = [
            (number, line)
            for number, line in outside
            if any(m[1] == SITE_MEDIA + name for m in MARKDOWN_IMAGE.finditer(line))
        ]
        if not found:
            problems.append(f"{name} isn't in the README")
            continue
        number, line = found[0]
        before = lines[number - 2] if number >= 2 else ""
        after = lines[number] if number < len(lines) else ""
        if not WHOLE_IMAGE.fullmatch(line) or before.strip() or after.strip():
            problems.append(f"{number}: {name} isn't alone in its paragraph")
            continue
        caption = next((text for text in reversed(lines[: number - 1]) if text.strip()), "")
        if view.lower() not in caption.lower():
            problems.append(f"{number}: the line before {name} doesn't name {view}")
        if label and label not in caption.lower():
            problems.append(f"{number}: the line before {name} doesn't say '{label}'")
    return problems


def test_fixture_table_images_are_refused() -> None:
    text = (FIXTURES / "table-images.md").read_text(encoding="utf-8")
    assert table_image_problems(text) == ["5: image inside a table row"]


def test_fixture_captioned_rows_pass() -> None:
    text = (FIXTURES / "captioned.md").read_text(encoding="utf-8")
    assert table_image_problems(text) == []
    assert caption_problems(text) == []


def test_fixture_bad_captions() -> None:
    text = (FIXTURES / "bad-captions.md").read_text(encoding="utf-8")
    assert caption_problems(text) == [
        "4: replay.png isn't alone in its paragraph",
        "8: the line before history.png doesn't name History",
        "12: the line before anomalies.png doesn't say 'beta'",
    ]


def test_readme_screenshots_one_per_row() -> None:
    text = README.read_text(encoding="utf-8")
    problems = table_image_problems(text) + caption_problems(text)
    assert problems == [], "\n".join(problems)
