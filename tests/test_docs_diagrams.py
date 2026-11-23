"""Diagrams render as pictures, served by the docs site itself (spec project-documentation,
"Diagrams render as pictures"; docs-visual-polish task 2.4). Builds the site and the offline
archive, then opens How Flashpoint works over loopback and from disk with every other host
blocked."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.docs_site import Watch, build_sites, open_page, serve

pytestmark = pytest.mark.browser
RENDERER = "assets/vendor/mermaid.min.js"
SOURCE = "flowchart LR"  # the pipeline diagram's first line


@pytest.fixture(scope="module")
def sites() -> tuple[Path, Path]:
    return build_sites()


@pytest.fixture(scope="module")
def site_url(sites: tuple[Path, Path]) -> Iterator[str]:
    with serve(sites[0]) as url:
        yield url


def _diagram_problems(page: Any, watch: Watch) -> list[str]:
    svg = page.locator("div.mermaid svg")
    svg.first.wait_for(state="visible", timeout=15_000)
    problems: list[str] = []
    box = svg.first.bounding_box()
    if box is None or box["width"] < 100 or box["height"] < 40:
        problems.append(f"the diagram is drawn at {box}, too small to be the pipeline")
    if "inbox" not in (svg.first.text_content() or ""):
        problems.append("the drawn diagram doesn't hold the pipeline's labels")
    if page.evaluate("t => document.body.innerText.includes(t)", SOURCE):
        problems.append(f"the diagram's source ({SOURCE!r}) is shown on the page")
    if not any(url.endswith(RENDERER) for url in watch.requests):
        problems.append(f"the page didn't load the site's own {RENDERER}")
    problems += [f"asked unpkg.com: {url}" for url in watch.blocked if "unpkg.com" in url]
    return problems + watch.console_problems()


def test_pipeline_diagram_over_loopback(browser: Any, site_url: str) -> None:
    page, watch = open_page(browser)
    try:
        page.goto(f"{site_url}/under-the-hood/how-it-works/")
        problems = _diagram_problems(page, watch)
    finally:
        page.context.close()
    assert problems == [], "\n".join(problems)


def test_pipeline_diagram_in_the_offline_archive(browser: Any, sites: tuple[Path, Path]) -> None:
    page, watch = open_page(browser)
    try:
        page.goto((sites[1] / "under-the-hood" / "how-it-works.html").as_uri())
        problems = _diagram_problems(page, watch)
    finally:
        page.context.close()
    assert problems == [], "\n".join(problems)


def test_a_page_without_a_diagram_doesnt_load_the_renderer(browser: Any, site_url: str) -> None:
    page, watch = open_page(browser)
    try:
        page.goto(f"{site_url}/")
        page.wait_for_load_state("networkidle")
    finally:
        page.context.close()
    assert [u for u in watch.requests if "mermaid" in u] == []
    assert [u for u in watch.blocked if "unpkg.com" in u] == []


def test_only_pages_with_a_diagram_reference_the_renderer(sites: tuple[Path, Path]) -> None:
    for site, page in ((sites[0], "under-the-hood/how-it-works/index.html"),
                       (sites[1], "under-the-hood/how-it-works.html")):  # fmt: skip
        referencing = sorted(
            p.relative_to(site).as_posix()
            for p in site.rglob("*.html")
            if RENDERER in p.read_text(encoding="utf-8")
        )
        assert referencing == [page]
        assert (site / RENDERER).is_file()
