"""Images open full-frame (spec project-documentation, "Images open full-frame"; docs-visual-polish
task 4.1). Builds the site and the offline archive, then clicks and keys through the zoom on the
home page and the Why page with every other host blocked."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

from tests.docs_site import build_sites, open_page, serve

pytestmark = pytest.mark.browser
VIEWPORT = {"width": 1280, "height": 800}
REPLAY = "img[src$='assets/media/replay.png']"
DIALOG = "dialog.fp-zoom"
OPENED = f"{DIALOG} img, {DIALOG} video"


@pytest.fixture(scope="module")
def sites() -> tuple[Path, Path]:
    return build_sites()


@pytest.fixture(scope="module")
def site_url(sites: tuple[Path, Path]) -> Iterator[str]:
    with serve(sites[0]) as url:
        yield url


def _fills_the_window(page: Any) -> list[str]:
    """The opened media's box lies inside the window and spans its width or its height."""
    box = page.locator(OPENED).first.bounding_box()
    if box is None:
        return ["the opened image has no box"]
    problems: list[str] = []
    width, height = VIEWPORT["width"], VIEWPORT["height"]
    if box["x"] < -1 or box["y"] < -1 or box["x"] + box["width"] > width + 1:
        problems.append(f"the opened image runs off the window: {box}")
    if box["y"] + box["height"] > height + 1:
        problems.append(f"the opened image runs off the bottom of the window: {box}")
    if box["width"] < 0.98 * width and box["height"] < 0.9 * height:
        problems.append(f"the opened image doesn't fill the window: {box}")
    fit = page.locator(OPENED).first.evaluate("el => getComputedStyle(el).objectFit")
    if fit != "contain":
        problems.append(f"the opened image is fitted with {fit!r}, not 'contain' (aspect lost)")
    return problems


def test_click_opens_the_screenshot_full_frame_and_click_closes_it(
    browser: Any, site_url: str
) -> None:
    page, watch = open_page(browser, viewport=VIEWPORT)
    try:
        page.goto(f"{site_url}/")
        shot = page.locator(REPLAY)
        shot.scroll_into_view_if_needed()
        page.mouse.wheel(0, 120)
        page.wait_for_timeout(200)
        scrolled = page.evaluate("() => window.scrollY")
        alt = shot.get_attribute("alt")
        shot.click()
        page.locator(DIALOG).wait_for(state="visible")
        problems = _fills_the_window(page)
        caption = page.locator(f"{DIALOG} figcaption").text_content()
        page.locator(OPENED).first.click()
        page.locator(DIALOG).wait_for(state="hidden")
        assert page.evaluate("() => window.scrollY") == scrolled
    finally:
        page.context.close()
    assert problems == [], "\n".join(problems)
    assert caption == alt
    assert watch.console_problems() == []


def test_backdrop_and_close_button_close_it(browser: Any, site_url: str) -> None:
    page, _ = open_page(browser, viewport=VIEWPORT)
    try:
        page.goto(f"{site_url}/")
        page.locator(REPLAY).click()
        page.locator(DIALOG).wait_for(state="visible")
        page.mouse.click(5, VIEWPORT["height"] // 2)  # the backdrop, beside a 16:10 screenshot
        page.locator(DIALOG).wait_for(state="hidden")
        page.locator(REPLAY).click()
        page.locator(f"{DIALOG} button.fp-zoom__close").click()
        page.locator(DIALOG).wait_for(state="hidden")
    finally:
        page.context.close()


def test_keyboard_opens_with_enter_and_closes_with_esc(browser: Any, site_url: str) -> None:
    page, _ = open_page(browser, viewport=VIEWPORT)
    try:
        page.goto(f"{site_url}/")
        page.locator(REPLAY).focus()
        assert page.evaluate(f"() => document.activeElement.matches({REPLAY!r})")
        assert page.locator(REPLAY).get_attribute("role") == "button"
        page.keyboard.press("Enter")
        page.locator(DIALOG).wait_for(state="visible")
        page.keyboard.press("Escape")
        page.locator(DIALOG).wait_for(state="hidden")
        refocused = page.evaluate(f"() => document.activeElement.matches({REPLAY!r})")
        page.keyboard.press(" ")
        page.locator(DIALOG).wait_for(state="visible")
    finally:
        page.context.close()
    assert refocused, "focus didn't go back to the screenshot"


def test_the_why_pages_animation_opens_and_keeps_playing(browser: Any, site_url: str) -> None:
    page, _ = open_page(browser, viewport=VIEWPORT)
    try:
        page.goto(f"{site_url}/why/")
        page.locator(".md-content video").first.click()
        page.locator(f"{DIALOG} video").wait_for(state="visible")
        playing = page.locator(f"{DIALOG} video").evaluate(
            """video => new Promise(done => {
                let updates = 0;
                video.addEventListener('timeupdate', () => { updates += 1; });
                setTimeout(() => done({ updates, paused: video.paused, loop: video.loop }), 1500);
            })"""
        )
        caption = page.locator(f"{DIALOG} figcaption").text_content()
    finally:
        page.context.close()
    assert playing["paused"] is False and playing["loop"] is True
    assert playing["updates"] >= 2, f"the opened animation isn't playing: {playing}"
    assert caption == "Picking a motor in History and following it across matches"


def test_logo_and_linked_images_dont_open(browser: Any, site_url: str) -> None:
    page, _ = open_page(browser, viewport=VIEWPORT)
    try:
        page.goto(f"{site_url}/")
        page.locator(".md-header .md-logo").first.click()
        page.wait_for_load_state()
        page.evaluate(
            """() => {
                const link = document.createElement('a');
                link.href = '#linked-image';
                link.innerHTML = '<img id="linked" alt="linked" src="assets/media/history.png">';
                document.querySelector('.md-content article').prepend(link);
            }"""
        )
        page.locator("#linked").click()
        opened = page.locator(DIALOG).is_visible()
        hash_ = page.evaluate("() => location.hash")
    finally:
        page.context.close()
    assert opened is False, "a linked image opened full-frame"
    assert hash_ == "#linked-image", "the linked image didn't follow its link"


def test_zoom_works_offline_and_asks_nothing_outside_the_archive(
    browser: Any, sites: tuple[Path, Path]
) -> None:
    offline = sites[1].resolve()
    page, watch = open_page(browser, viewport=VIEWPORT)
    try:
        page.goto((offline / "index.html").as_uri())
        page.wait_for_load_state("networkidle")
        before = len(watch.requests)
        page.locator(REPLAY).click()
        page.locator(DIALOG).wait_for(state="visible")
        problems = _fills_the_window(page)
        page.keyboard.press("Escape")
        page.locator(DIALOG).wait_for(state="hidden")
        during = watch.requests[before:]
    finally:
        page.context.close()
    assert problems == [], "\n".join(problems)
    outside = [
        url
        for url in during
        if not (urlsplit(url).scheme in ("data", "blob") or url.startswith(offline.as_uri()))
    ]
    assert outside == []
    assert [url for url in watch.requests if "zoom" in url], "the archive didn't load zoom.js"
