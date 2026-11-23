"""Paced animations (spec project-documentation, "Animations show each step"; docs-visual-polish
task 3.1). `check_animation` refuses a clip under 6 s or one that opens on the loading screen,
and the step helpers mark and pause only when recording."""

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

REPO = Path(__file__).resolve().parents[2]
LOADING_BACKGROUND = "0x0A0806"  # the app's default background, behind "Loading Flashpoint…"


def _tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("docs_media", REPO / "tools" / "docs-media.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _clip(tool: ModuleType, path: Path, source: str, seconds: float, vf: str = "") -> Path:
    """A VP8 WebM from an ffmpeg test source, 640x400 at 12 fps."""
    options = f"s=640x400:r=12:d={seconds}"
    args = ["-f", "lavfi", "-i", f"{source}:{options}" if "=" in source else f"{source}={options}"]
    args += ["-vf", vf] if vf else []
    done = tool._ffmpeg(*args, "-c:v", "libvpx", "-b:v", "300k", "-an", str(path))
    assert done.returncode == 0, done.stderr
    return path


def _gif(tool: ModuleType, path: Path) -> Path:
    path.write_bytes(tool.GIF)
    return path


# The loading notice is one short line of text on a flat background. A box the size of that
# line stands in for it: ffmpeg's text filter needs fonts that a CI runner may not have.
LOADING_LINE = "drawbox=x=40:y=40:w=170:h=14:color=0xE8DCC8:t=fill"
# The clip checks need the docs group's ffmpeg and Pillow, which only the browser job installs
# (the docs group is optional), so they carry its marker.
needs_docs_group = pytest.mark.browser


@needs_docs_group
def test_check_animation_fails_a_clip_under_6_s(tmp_path: Path) -> None:
    tool = _tool()
    webm = _clip(tool, tmp_path / "short.webm", "testsrc2", 3)
    assert tool.ANIMATION_MIN_S == 6
    with pytest.raises(tool.ShotError, match=r"^flow-short: 3\.0 s, under 6 s$"):
        tool.check_animation("flow-short", webm, _gif(tool, tmp_path / "short.gif"))


@needs_docs_group
def test_check_animation_fails_a_first_frame_like_the_loading_screen(tmp_path: Path) -> None:
    tool = _tool()
    webm = _clip(tool, tmp_path / "loading.webm", f"color=c={LOADING_BACKGROUND}", 7, LOADING_LINE)
    assert tool.first_frame_uniformity(webm) > tool.FIRST_FRAME_UNIFORM
    with pytest.raises(tool.ShotError, match=r"^flow-loading: the first frame is .*loading screen"):
        tool.check_animation("flow-loading", webm, _gif(tool, tmp_path / "loading.gif"))


@needs_docs_group
def test_check_animation_passes_a_busy_clip_of_7_s(tmp_path: Path) -> None:
    tool = _tool()
    webm = _clip(tool, tmp_path / "busy.webm", "testsrc2", 7)
    assert tool.first_frame_uniformity(webm) < tool.FIRST_FRAME_UNIFORM
    tool.check_animation("flow-busy", webm, _gif(tool, tmp_path / "busy.gif"))


# --- the step helpers mark and pause only when recording ------------------------------------


class FakeLocator:
    def __init__(self, page: "FakePage", selector: str) -> None:
        self.page = page
        self.selector = selector

    @property
    def first(self) -> "FakeLocator":
        return self

    def filter(self, **_kwargs: Any) -> "FakeLocator":
        return self

    def all(self) -> list["FakeLocator"]:
        return [self]

    def count(self) -> int:
        return 1

    def click(self) -> None:
        self.page.calls.append(("click", self.selector))

    def hover(self) -> None:
        self.page.calls.append(("hover", self.selector))

    def scroll_into_view_if_needed(self) -> None:
        self.page.calls.append(("scroll", self.selector))

    def evaluate(self, script: str, *_args: Any) -> None:
        self.page.calls.append(("mark" if "add" in script else "evaluate", self.selector))

    def wait_for(self, **_kwargs: Any) -> None:
        return None


class FakePage:
    """Records the calls a step makes; every selector it waits for is already there."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def locator(self, selector: str) -> FakeLocator:
        return FakeLocator(self, selector)

    def get_by_role(self, _role: str, name: str) -> FakeLocator:
        return FakeLocator(self, f"role={name}")

    def wait_for_selector(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def wait_for_timeout(self, ms: float) -> None:
        self.calls.append(("wait", ms))

    def evaluate(self, _script: str, *_args: Any) -> None:
        self.calls.append(("unmark", None))

    def add_style_tag(self, **_kwargs: Any) -> None:
        self.calls.append(("style", None))


def test_unpaced_steps_only_click(monkeypatch: pytest.MonkeyPatch) -> None:
    tool = _tool()
    page = FakePage()
    tool.run_steps(page, {"steps": "history-pick-motor"}, pace=False)
    assert page.calls == [("click", "#rows tr button")]


def test_paced_steps_hold_mark_click_and_pause() -> None:
    tool = _tool()
    page = FakePage()
    tool.run_steps(page, {"steps": "history-pick-motor"}, pace=True)
    target = "#rows tr button"
    assert page.calls == [
        ("style", None),
        ("wait", tool.START_HOLD_MS),
        ("scroll", target),
        ("mark", target),
        ("wait", tool.MARK_MS),
        ("click", target),
        ("unmark", None),
        ("wait", tool.STEP_PAUSE_MS),
    ]


def test_pacing_constants_meet_the_spec() -> None:
    tool = _tool()
    assert tool.START_HOLD_MS >= 1500  # the start view held at least 1.5 s
    assert tool.HOLD_MS >= 2000  # the result held at least 2 s
    assert (tool.START_HOLD_MS, tool.MARK_MS, tool.STEP_PAUSE_MS, tool.HOLD_MS) == (
        2000,
        800,
        1500,
        3000,
    )
