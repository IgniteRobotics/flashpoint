"""Diagrams fit the docs column (spec project-documentation, "Diagrams fit the docs column";
docs-diagrams task 1.1). A diagram drawn under 80 % of its natural size in the 648 px column of a
1280 px window fails, naming the page, which diagram on it and the size it was drawn at. The
first dev deploy drew the left-to-right pipeline at 34 %, with labels about 5 px high."""

import math
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from tests.docs_site import build_sites, docs_media, open_page, serve

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "docs-diagrams"
LIMIT = 0.8
VIEWPORT = {"width": 1280, "height": 800}
COLUMN_PX = 648  # the docs content column in a 1280 px window
# Each drawn diagram under `root`, in page order: the SVG's width on screen and its natural width
# (its viewBox). The theme draws each one in the shadow root of a `div.mermaid`.
MEASURE = """root => [...document.querySelectorAll(root)].map(div => {
    const svg = (div.shadowRoot || div).querySelector('svg');
    return svg && { natural: svg.viewBox.baseVal.width, drawn: svg.getBoundingClientRect().width };
})"""


@dataclass(frozen=True)
class Drawn:
    page: str
    position: int  # 1 for the page's first diagram
    natural: float
    drawn: float


def scale_problems(drawn: list[Drawn], limit: float = LIMIT) -> list[str]:
    """Each diagram drawn under `limit` of its natural width, by page, position and percentage."""
    problems: list[str] = []
    for d in drawn:
        ratio = d.drawn / d.natural if d.natural > 0 else 0.0
        if ratio < limit:
            problems.append(
                f"{d.page}: diagram {d.position} is drawn at {math.floor(ratio * 100)} % of its"
                f" natural size ({d.drawn:.0f} of {d.natural:.0f} px), under"
                f" {limit * 100:.0f} %; run it top to bottom or split it"
            )
    return problems


# --- unit: the rule, on made-up numbers ---------------------------------------------------------


def test_a_diagram_at_or_over_the_limit_passes() -> None:
    drawn = [
        Drawn("a/page", 1, natural=500, drawn=400),  # exactly 0.8
        Drawn("a/page", 2, natural=432, drawn=432),
        Drawn("b/page", 1, natural=300, drawn=310),
    ]
    assert scale_problems(drawn) == []


def test_a_diagram_under_the_limit_is_named_with_its_page_position_and_size() -> None:
    drawn = [
        Drawn("under-the-hood/how-it-works", 1, natural=1909, drawn=648),
        Drawn("under-the-hood/how-it-works", 2, natural=500, drawn=399),
    ]
    problems = scale_problems(drawn)
    assert len(problems) == 2
    assert problems[0].startswith("under-the-hood/how-it-works: diagram 1 is drawn at 33 %")
    assert problems[1].startswith("under-the-hood/how-it-works: diagram 2 is drawn at 79 %")
    assert all("under 80 %" in p for p in problems)


def test_a_diagram_with_no_width_fails() -> None:
    assert scale_problems([Drawn("p", 1, natural=0, drawn=0)]) != []


# --- browser: the measurement, then every docs page -----------------------------------------


@pytest.mark.browser
def test_the_measurement_catches_the_left_to_right_pipeline(browser: Any) -> None:
    """Today's nine-box left-to-right pipeline, drawn with the vendored renderer in a 648 px
    column with the theme's settings, measures at about a third of its natural size."""
    (renderer,) = docs_media().vendor()
    source = (FIXTURES / "pipeline-lr.mmd").read_text(encoding="utf-8")
    page, watch = open_page(browser, viewport=VIEWPORT)
    try:
        page.set_content(f'<main style="width: {COLUMN_PX}px"><div class="mermaid"></div></main>')
        # By content: Playwright reads a `path` on a worker thread that outlives the test.
        page.add_script_tag(content=renderer.read_text(encoding="utf-8"))
        page.evaluate(
            """async source => {
                mermaid.initialize({ startOnLoad: false, theme: 'default' });
                const { svg } = await mermaid.render('fixture', source);
                document.querySelector('div.mermaid').innerHTML = svg;
            }""",
            source,
        )
        measured = page.evaluate(MEASURE, "div.mermaid")
    finally:
        page.context.close()
    assert len(measured) == 1 and measured[0] is not None
    ratio = measured[0]["drawn"] / measured[0]["natural"]
    assert 0.25 < ratio < 0.45, f"the left-to-right pipeline measured at {ratio:.0%}"
    problems = scale_problems([Drawn("fixture", 1, **measured[0])])
    assert len(problems) == 1 and "fixture: diagram 1" in problems[0]
    assert watch.console_problems() == []


@pytest.fixture(scope="module")
def sites() -> tuple[Path, Path]:
    return build_sites()


@pytest.fixture(scope="module")
def site_url(sites: tuple[Path, Path]) -> Iterator[str]:
    with serve(sites[0]) as url:
        yield url


def _pages_with_diagrams(site: Path) -> dict[str, int]:
    """Each built page that holds a diagram (by its URL path), and how many it holds."""
    diagram = docs_media().DIAGRAM
    found: dict[str, int] = {}
    for html in sorted(site.rglob("index.html")):
        count = len(diagram.findall(html.read_text(encoding="utf-8")))
        if count:
            found[html.parent.relative_to(site).as_posix()] = count
    return found


@pytest.mark.browser
def test_every_docs_diagram_fits_the_column(
    browser: Any, sites: tuple[Path, Path], site_url: str
) -> None:
    pages = _pages_with_diagrams(sites[0])
    assert pages, "no built docs page holds a diagram"
    drawn: list[Drawn] = []
    problems: list[str] = []
    page, watch = open_page(browser, viewport=VIEWPORT)
    try:
        for path, count in pages.items():
            page.goto(f"{site_url}/{path}/")
            page.wait_for_function(
                """n => [...document.querySelectorAll('.md-content div.mermaid')]
                    .filter(d => (d.shadowRoot || d).querySelector('svg')).length >= n""",
                arg=count,
                timeout=15_000,
            )
            measured = page.evaluate(MEASURE, ".md-content div.mermaid")
            for position, m in enumerate(measured, start=1):
                if m is None:
                    problems.append(f"{path}: diagram {position} didn't draw")
                else:
                    drawn.append(Drawn(path, position, **m))
    finally:
        page.context.close()
    problems += scale_problems(drawn) + watch.console_problems()
    assert problems == [], "\n".join(problems)
