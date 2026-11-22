"""README content checks (spec project-documentation, "README is an overview and entry point";
mvp-documentation task 13.1). The README is also the PyPI page: its images come from the docs
site's `latest` version, and its quickstart stays one install plus three commands."""

import re
import shlex
from pathlib import Path

from tests.test_docs import MEDIA_NAMES

REPO = Path(__file__).resolve().parents[1]
README = REPO / "README.md"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "readme"
SITE = "https://igniterobotics.github.io/flashpoint/latest/"
SITE_MEDIA = SITE + "assets/media/"
# The repository description (task 13.7) is the README's pitch, word for word.
PITCH = (
    "FRC robot telemetry: import WPILib .wpilog and CTRE .hoot logs, and track every motor's"
    " health across your robot's life. Runs offline on a pit laptop."
)
MARKDOWN_IMAGE = re.compile(r"!\[[^\]]*\]\(\s*<?([^)\s>]+)")
HTML_IMAGE = re.compile(r"<img\b[^>]*?\ssrc\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")
FENCE = re.compile(r"^\s*(```|~~~)")
PHASE = re.compile(r"\bP[0-6]\b")


def _lines_outside_fences(text: str) -> list[tuple[int, str]]:
    lines: list[tuple[int, str]] = []
    in_fence = False
    for number, line in enumerate(text.splitlines(), start=1):
        if FENCE.match(line):
            in_fence = not in_fence
            continue
        if not in_fence:
            lines.append((number, line))
    return lines


def image_problems(text: str) -> list[str]:
    """Each image must be an absolute URL under the docs site's `latest` media folder, naming a
    file the media manifest produces."""
    problems: list[str] = []
    for number, line in _lines_outside_fences(text):
        for url in [m[1] for m in (*MARKDOWN_IMAGE.finditer(line), *HTML_IMAGE.finditer(line))]:
            if not SCHEME.match(url):
                problems.append(f"{number}: relative image {url} (breaks on PyPI)")
            elif not url.startswith(SITE_MEDIA):
                problems.append(f"{number}: image {url} isn't under {SITE_MEDIA}")
            elif url.removeprefix(SITE_MEDIA) not in MEDIA_NAMES:
                problems.append(f"{number}: image {url} isn't in docs/media-manifest.toml")
    return problems


def quickstart_commands(text: str) -> list[str]:
    """The commands in the first fenced block under the `## Quickstart` heading."""
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == "## Quickstart")
    commands: list[str] = []
    in_fence = False
    for line in lines[start + 1 :]:
        if FENCE.match(line):
            if in_fence:
                return commands
            in_fence = True
            continue
        if in_fence:
            command = line.split(" #", 1)[0].strip()
            if command and not command.startswith("#"):
                commands.append(command)
        elif line.startswith("## "):
            break
    raise AssertionError("the Quickstart section has no closed command block")


def quickstart_problems(commands: list[str]) -> list[str]:
    if not commands:
        return ["the quickstart block has no commands"]
    problems: list[str] = []
    first = shlex.split(commands[0])
    if "install" not in first or "flashpoint" not in first:
        problems.append(f"the first command must install flashpoint: {commands[0]}")
    rest = commands[1:]
    if len(rest) > 3:
        problems.append(f"{len(rest)} commands after the install; at most 3")
    problems += [
        f"not a flashpoint command: {c}" for c in rest if shlex.split(c)[0] != "flashpoint"
    ]
    if not rest or shlex.split(rest[-1])[:2] != ["flashpoint", "serve"]:
        problems.append("the last command must be `flashpoint serve`")
    return problems


def test_fixture_readme_images() -> None:
    text = (FIXTURES / "images.md").read_text(encoding="utf-8")
    assert image_problems(text) == [
        "5: relative image media/replay.png (breaks on PyPI)",
        "6: image https://igniterobotics.github.io/flashpoint/0.3/assets/media/history.png"
        " isn't under https://igniterobotics.github.io/flashpoint/latest/assets/media/",
        "7: image https://igniterobotics.github.io/flashpoint/latest/assets/media/nope.png"
        " isn't in docs/media-manifest.toml",
        "8: relative image assets/media/anomalies.png (breaks on PyPI)",
    ]


def test_readme_images_come_from_the_latest_docs() -> None:
    text = README.read_text(encoding="utf-8")
    problems = image_problems(text)
    assert problems == [], "\n".join(problems)
    assert MARKDOWN_IMAGE.search(text) or HTML_IMAGE.search(text), "the README has no images"


def test_fixture_quickstart_too_long() -> None:
    commands = quickstart_commands((FIXTURES / "long-quickstart.md").read_text(encoding="utf-8"))
    assert quickstart_problems(commands) == [
        "4 commands after the install; at most 3",
        "the last command must be `flashpoint serve`",
    ]


def test_readme_quickstart_is_short() -> None:
    commands = quickstart_commands(README.read_text(encoding="utf-8"))
    assert quickstart_problems(commands) == []


def test_readme_opens_with_the_pitch() -> None:
    """The first paragraph after the title is the pitch, and matches the repository description."""
    paragraphs = [p.strip() for p in README.read_text(encoding="utf-8").split("\n\n")]
    assert paragraphs[0].startswith("# ")
    assert " ".join(paragraphs[1].split()) == PITCH


def test_readme_works_with_block() -> None:
    text = README.read_text(encoding="utf-8")
    start = text.index("## Works with")
    block = text[start : text.index("\n## ", start + 1)]
    for needed in ("Phoenix 6", ".hoot", ".wpilog", "Phoenix Pro", "macOS", "Linux", "Windows"):
        assert needed in block, f"'Works with' doesn't name {needed}"
    assert SITE + "support-matrix/" in block


def test_readme_links_the_docs_entry_pages() -> None:
    text = README.read_text(encoding="utf-8")
    for page in (
        "why/",
        "quickstart/",
        "install/",
        "robot-side/",
        "support-matrix/",
        "troubleshooting/",
        "demo/",
    ):
        assert f"({SITE}{page}" in text, f"the README doesn't link {SITE}{page}"
    assert "#try-it-with-sample-logs" in text, "the README doesn't link the sample-logs quickstart"


def test_readme_has_no_phase_headings() -> None:
    headings = [
        line for _, line in _lines_outside_fences(README.read_text("utf-8")) if line.startswith("#")
    ]
    assert [h for h in headings if PHASE.search(h)] == []
