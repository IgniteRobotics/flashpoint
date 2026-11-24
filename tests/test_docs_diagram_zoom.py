"""Diagrams open full-frame (spec project-documentation, "Diagrams open full-frame";
docs-diagrams task 3.1). Builds the site and the offline archive, then clicks and keys through the
diagrams on How Flashpoint works with every other host blocked. The opened diagram is drawn again
from its source, outside the theme's shadow root, so it's sharp at any size. It's always whole in
the window; the top-to-bottom pipeline is too tall to be larger than on the page, the file
lifecycle isn't."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

from tests.docs_site import build_sites, open_page, serve

pytestmark = pytest.mark.browser
VIEWPORT = {"width": 1280, "height": 800}
DIAGRAM = ".md-content .fp-diagram"
DIALOG = "dialog.fp-zoom"
OPENED = f"{DIALOG} svg"
PAGE = "under-the-hood/how-it-works"
# The scale a drawn SVG is shown at: its box against its viewBox (the smaller of the two axes,
# as preserveAspectRatio's default `meet` fits it), and the box itself.
SCALE = """svg => {
    const r = svg.getBoundingClientRect();
    const v = svg.viewBox.baseVal;
    return { scale: Math.min(r.width / v.width, r.height / v.height),
             x: r.x, y: r.y, width: r.width, height: r.height };
}"""
KIND_FILLS = """svg => Object.fromEntries(['step', 'data', 'robot'].map(kind => {
    const node = svg.querySelector(`g.node.${kind}`);
    const shape = node && node.querySelector('.label-container');
    return [kind, shape ? getComputedStyle(shape).fill : null];
}))"""


@pytest.fixture(scope="module")
def sites() -> tuple[Path, Path]:
    return build_sites()


@pytest.fixture(scope="module")
def site_url(sites: tuple[Path, Path]) -> Iterator[str]:
    with serve(sites[0]) as url:
        yield url


def _ready(page: Any) -> None:
    page.locator(f"{DIAGRAM} div.mermaid svg").first.wait_for(state="attached", timeout=15_000)


def _opened_problems(page: Any, on_page: float | None = None) -> list[str]:
    """The opened diagram lies whole inside the window and spans most of it; given its scale on
    the page, it's also drawn larger than that."""
    page.locator(OPENED).first.wait_for(state="visible", timeout=15_000)
    opened = page.locator(OPENED).first.evaluate(SCALE)
    problems: list[str] = []
    width, height = VIEWPORT["width"], VIEWPORT["height"]
    if opened["x"] < -1 or opened["y"] < -1:
        problems.append(f"the opened diagram starts off the window: {opened}")
    if opened["x"] + opened["width"] > width + 1 or opened["y"] + opened["height"] > height + 1:
        problems.append(f"the opened diagram runs off the window: {opened}")
    if opened["width"] < 0.9 * width and opened["height"] < 0.85 * height:
        problems.append(f"the opened diagram doesn't fill the window: {opened}")
    if on_page is not None and opened["scale"] <= on_page:
        problems.append(
            f"the opened diagram is drawn at {opened['scale']:.0%}, no larger than the"
            f" {on_page:.0%} it's drawn at on the page"
        )
    return problems


def _page_scale(page: Any, n: int = 0) -> float:
    scale: float = page.locator(f"{DIAGRAM} div.mermaid svg").nth(n).evaluate(SCALE)["scale"]
    return scale


def test_click_opens_the_pipeline_full_frame_and_esc_returns(browser: Any, site_url: str) -> None:
    page, watch = open_page(browser, viewport=VIEWPORT)
    try:
        page.goto(f"{site_url}/{PAGE}/")
        _ready(page)
        diagram = page.locator(DIAGRAM).first
        diagram.scroll_into_view_if_needed()
        page.mouse.wheel(0, 120)
        page.wait_for_timeout(200)
        scrolled = page.evaluate("() => window.scrollY")
        diagram.click()
        problems = _opened_problems(page)
        page.keyboard.press("Escape")
        page.locator(DIALOG).wait_for(state="hidden")
        after = page.evaluate("() => window.scrollY")
        refocused = page.evaluate(f"() => document.activeElement.matches({DIAGRAM!r})")
        diagram.click()
        page.locator(OPENED).first.wait_for(state="visible")
        page.locator(f"{DIALOG} button.fp-zoom__close").click()
        page.locator(DIALOG).wait_for(state="hidden")
    finally:
        page.context.close()
    assert problems == [], "\n".join(problems)
    assert after == scrolled, f"the page came back at {after}, not {scrolled}"
    assert refocused, "focus didn't go back to the diagram"
    assert watch.console_problems() == []


def test_the_file_lifecycle_opens_larger_than_on_the_page(browser: Any, site_url: str) -> None:
    page, watch = open_page(browser, viewport=VIEWPORT)
    try:
        page.goto(f"{site_url}/{PAGE}/")
        page.locator(f"{DIAGRAM} div.mermaid svg").nth(1).wait_for(state="attached")
        on_page = _page_scale(page, 1)
        page.locator(DIAGRAM).nth(1).click()
        problems = _opened_problems(page, on_page)
        caption = page.locator(f"{DIALOG} figcaption").text_content()
    finally:
        page.context.close()
    assert problems == [], "\n".join(problems)
    assert caption == "Diagram: What happens to a .hoot file"
    assert watch.console_problems() == []


def test_keyboard_opens_the_diagram_with_enter(browser: Any, site_url: str) -> None:
    page, watch = open_page(browser, viewport=VIEWPORT)
    try:
        page.goto(f"{site_url}/{PAGE}/")
        _ready(page)
        diagram = page.locator(DIAGRAM).first
        diagram.focus()
        focused = page.evaluate(f"() => document.activeElement.matches({DIAGRAM!r})")
        role = diagram.get_attribute("role")
        label = diagram.get_attribute("aria-label") or ""
        page.keyboard.press("Enter")
        page.locator(OPENED).first.wait_for(state="visible", timeout=15_000)
        page.keyboard.press("Escape")
        page.locator(DIALOG).wait_for(state="hidden")
    finally:
        page.context.close()
    assert focused, "the diagram can't take focus"
    assert role == "button"
    assert label.startswith("Open full size"), label
    assert watch.console_problems() == []


def test_the_opened_diagram_keeps_the_dark_palette(browser: Any, site_url: str) -> None:
    page, watch = open_page(browser, viewport=VIEWPORT, color_scheme="dark")
    try:
        page.goto(f"{site_url}/{PAGE}/")
        _ready(page)
        scheme = page.evaluate("() => document.body.getAttribute('data-md-color-scheme')")
        on_page = page.locator(f"{DIAGRAM} div.mermaid svg").first.evaluate(KIND_FILLS)
        page.locator(DIAGRAM).first.click()
        page.locator(OPENED).first.wait_for(state="visible", timeout=15_000)
        opened = page.locator(OPENED).first.evaluate(KIND_FILLS)
    finally:
        page.context.close()
    assert scheme == "slate"
    assert None not in on_page.values(), on_page
    assert opened == on_page
    assert watch.console_problems() == []


def test_diagram_zoom_works_offline_and_asks_nothing_outside_the_archive(
    browser: Any, sites: tuple[Path, Path]
) -> None:
    offline = sites[1].resolve()
    page, watch = open_page(browser, viewport=VIEWPORT)
    try:
        page.goto((offline / "under-the-hood" / "how-it-works.html").as_uri())
        _ready(page)
        page.wait_for_load_state("networkidle")
        before = len(watch.requests)
        page.locator(DIAGRAM).first.click()
        page.locator(OPENED).first.wait_for(state="visible", timeout=15_000)
        page.keyboard.press("Escape")
        page.locator(DIALOG).wait_for(state="hidden")
        during = watch.requests[before:]
    finally:
        page.context.close()
    outside = [
        url
        for url in during
        if not (urlsplit(url).scheme in ("data", "blob") or url.startswith(offline.as_uri()))
    ]
    assert outside == []
    assert watch.console_problems() == []
