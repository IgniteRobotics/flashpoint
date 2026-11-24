"""One diagram notation (spec project-documentation, "One diagram notation"; docs-diagrams task
2.1). Boxes are tagged with one of five kind classes and coloured by the docs stylesheet, never
in the diagram's source; a diagram that uses kinds has an `fp-legend` block right below it naming
each one; labels are plain SVG text inside their boxes; and the colours follow the page's
light/dark toggle."""

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.docs_site import build_sites, open_page, serve

REPO = Path(__file__).resolve().parents[1]
GUIDE = REPO / "docs" / "guide"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "docs-diagrams"
KINDS = ("step", "data", "robot", "outside", "error")
OUTSIDE_DEF = "classDef outside stroke-dasharray:5 4"
VIEWPORT = {"width": 1280, "height": 800}
DIAGRAM_SVG = ".md-content div.mermaid svg"

FENCE_OPEN = re.compile(r"^\s*```mermaid\s*$")
FENCE_CLOSE = re.compile(r"^\s*```\s*$")
CLASS_DEF = re.compile(r"^\s*classDef\s+(\S+)\s*(.*?);?\s*$")
CLASS_USE = re.compile(r"^\s*class\s+\S.*?\s+([A-Za-z][\w-]*)\s*;?\s*$")
CLASS_SHORTHAND = re.compile(r":::([A-Za-z][\w-]*)")
LEGEND_OPEN = re.compile(r"^\s*<div\b[^>]*\bclass=[\"']fp-legend[\"']")
SWATCH = re.compile(r"\bfp-swatch--([A-Za-z][\w-]*)")


def _diagrams(text: str) -> Iterator[tuple[list[str], list[str]]]:
    """Each `mermaid` fence's source lines, and the lines of the `fp-legend` block right below it
    (empty when the next thing after the fence isn't one)."""
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        if not FENCE_OPEN.match(lines[i]):
            i += 1
            continue
        end = i + 1
        while end < len(lines) and not FENCE_CLOSE.match(lines[end]):
            end += 1
        source = lines[i + 1 : end]
        after = end + 1
        while after < len(lines) and not lines[after].strip():
            after += 1
        legend: list[str] = []
        if after < len(lines) and LEGEND_OPEN.match(lines[after]):
            stop = after
            while stop < len(lines) and "</div>" not in lines[stop]:
                stop += 1
            legend = lines[after : stop + 1]
        yield source, legend
        i = end + 1


def _classes(source: list[str]) -> tuple[set[str], dict[str, str]]:
    """The classes a diagram's boxes use, and each `classDef` with its style."""
    used: set[str] = set()
    defined: dict[str, str] = {}
    for line in source:
        if m := CLASS_DEF.match(line):
            defined.update(dict.fromkeys(m.group(1).split(","), m.group(2).strip()))
        elif m := CLASS_USE.match(line):
            used.add(m.group(1))
        used.update(CLASS_SHORTHAND.findall(line))
    return used, defined


def notation_problems(page: str, text: str) -> list[str]:
    """Each way a page's diagrams break the notation, naming the page, the diagram and the kind."""
    problems: list[str] = []
    for number, (source, legend) in enumerate(_diagrams(text), start=1):
        where = f"{page}: diagram {number}"
        used, defined = _classes(source)
        named = set(SWATCH.findall("\n".join(legend)))
        problems += [
            f"{where} uses the class {name!r}, not one of {', '.join(KINDS)}"
            for name in sorted((used | defined.keys()) - set(KINDS))
        ]
        problems += [
            f"{where} styles {name!r} in its source ({style!r}): colours come from"
            f" diagrams.css; the only definition allowed is `{OUTSIDE_DEF}`"
            for name, style in sorted(defined.items())
            if f"classDef {name} {style}" != OUTSIDE_DEF
        ]
        if "outside" in used and "outside" not in defined:
            problems.append(f"{where} uses 'outside' without `{OUTSIDE_DEF}`")
        problems += [
            f"{where}'s legend doesn't name the kind {kind!r}"
            for kind in sorted(used & set(KINDS) - named)
        ]
        problems += [
            f"{where}'s legend names {kind!r}, which the diagram doesn't use"
            for kind in sorted(named - used)
        ]
    return problems


# --- unit: the rule, on fixtures, then on every docs page ----------------------------------


def test_a_diagram_with_its_legend_passes() -> None:
    text = (FIXTURES / "good.md").read_text(encoding="utf-8")
    assert notation_problems("good", text) == []


def test_a_missing_legend_entry_is_named() -> None:
    text = (FIXTURES / "missing-legend.md").read_text(encoding="utf-8")
    assert notation_problems("missing-legend", text) == [
        "missing-legend: diagram 1's legend doesn't name the kind 'robot'"
    ]


def test_an_unknown_kind_and_a_colour_in_the_source_are_named() -> None:
    text = (FIXTURES / "unknown-kind.md").read_text(encoding="utf-8")
    problems = notation_problems("unknown-kind", text)
    assert any("uses the class 'thing'" in p for p in problems), problems
    assert any("styles 'data' in its source" in p for p in problems), problems
    assert any("uses 'outside' without" in p for p in problems), problems


def test_a_diagram_with_no_kinds_needs_no_legend() -> None:
    text = "```mermaid\nflowchart TB\n    A --> B\n```\n\nProse.\n"
    assert notation_problems("plain", text) == []


def test_every_docs_page_follows_the_notation() -> None:
    problems: list[str] = []
    for md in sorted(GUIDE.rglob("*.md")):
        page = md.relative_to(GUIDE).with_suffix("").as_posix()
        problems += notation_problems(page, md.read_text(encoding="utf-8"))
    assert problems == [], "\n".join(problems)


# --- browser: colours in both palettes, and labels ------------------------------------------

# The fill of the first box of each kind in one drawn diagram (null for a kind it doesn't use).
KIND_FILLS = """(svg, kinds) => Object.fromEntries(kinds.map(kind => {
    const node = svg.querySelector(`g.node.${kind}`);
    const shape = node && (node.querySelector('.label-container')
        || node.querySelector(':scope > rect, :scope > path, :scope > polygon'));
    return [kind, shape ? getComputedStyle(shape).fill : null];
}))"""
# Each node label in one drawn diagram: drawn as SVG text or HTML, and inside its box or not.
LABELS = """svg => [...svg.querySelectorAll('g.node')].flatMap(node => {
    const shape = node.querySelector('.label-container')
        || node.querySelector(':scope > rect, :scope > path, :scope > polygon');
    if (!shape) return [];
    const s = shape.getBoundingClientRect();
    const html = [...node.querySelectorAll('foreignObject')].map(f => ({
        text: f.textContent.trim(), html: true, inside: false }));
    const texts = [...node.querySelectorAll('text')].filter(t => t.textContent.trim()).map(t => {
        const r = t.getBoundingClientRect();
        const inside = r.left >= s.left - 1 && r.right <= s.right + 1
            && r.top >= s.top - 1 && r.bottom <= s.bottom + 1;
        return { text: t.textContent.trim(), html: false, inside };
    });
    return [...html, ...texts];
})"""


@pytest.fixture(scope="module")
def sites() -> tuple[Path, Path]:
    return build_sites()


@pytest.fixture(scope="module")
def site_url(sites: tuple[Path, Path]) -> Iterator[str]:
    with serve(sites[0]) as url:
        yield url


def _open_how_it_works(page: Any, site_url: str) -> None:
    page.goto(f"{site_url}/under-the-hood/how-it-works/")
    page.locator(DIAGRAM_SVG).first.wait_for(state="attached", timeout=15_000)


def _pipeline_fills(page: Any) -> dict[str, str | None]:
    fills: dict[str, str | None] = page.locator(DIAGRAM_SVG).first.evaluate(KIND_FILLS, KINDS)
    return fills


@pytest.mark.browser
def test_pipeline_kinds_differ_in_both_palettes_and_follow_the_toggle(
    browser: Any, site_url: str
) -> None:
    page, watch = open_page(browser, viewport=VIEWPORT, color_scheme="light")
    try:
        _open_how_it_works(page, site_url)
        light = _pipeline_fills(page)
        page.locator("form[data-md-component=palette] label:visible").first.click()
        page.wait_for_function(
            "() => document.body.getAttribute('data-md-color-scheme') === 'slate'"
        )
        page.wait_for_timeout(200)
        dark = _pipeline_fills(page)
    finally:
        page.context.close()
    problems: list[str] = []
    for name, fills in (("light", light), ("dark", dark)):
        shown = [fills.get(k) for k in ("step", "data", "robot")]
        if None in shown or len(set(shown)) != 3:
            problems.append(f"{name}: step, data and robot fills aren't three colours: {fills}")
    problems += [
        f"the {kind} fill didn't change with the toggle: {light.get(kind)}"
        for kind in ("step", "data", "robot")
        if light.get(kind) is not None and light.get(kind) == dark.get(kind)
    ]
    problems += watch.console_problems()
    assert problems == [], "\n".join(problems)


@pytest.mark.browser
def test_every_label_is_plain_text_inside_its_box(browser: Any, site_url: str) -> None:
    page, watch = open_page(browser, viewport=VIEWPORT)
    try:
        _open_how_it_works(page, site_url)
        svgs = page.locator(DIAGRAM_SVG)
        drawn = [
            (n, svgs.nth(n).evaluate(LABELS), svgs.nth(n).evaluate(
                "svg => svg.querySelectorAll('foreignObject').length"))
            for n in range(svgs.count())
        ]  # fmt: skip
    finally:
        page.context.close()
    problems: list[str] = []
    for n, labels, foreign in drawn:
        where = f"under-the-hood/how-it-works: diagram {n + 1}"
        if not labels:
            problems.append(f"{where} has no node labels")
        if foreign:
            problems.append(f"{where} draws {foreign} labels as HTML (foreignObject)")
        problems += [
            f"{where}: the label {label['text']!r} runs past its box"
            for label in labels
            if not label["html"] and not label["inside"]
        ]
    problems += watch.console_problems()
    assert problems == [], "\n".join(problems)
