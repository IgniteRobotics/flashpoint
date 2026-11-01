"""The escaping and offline rules, checked on the shipped files (spec: match-reports).

Views build DOM only through h(); no file may use an HTML sink, inline handler, or eval, and
no app file may reference a remote host.
"""

import re
from pathlib import Path

import pytest

from flashpoint.web.server import STATIC_DIR

APP_FILES = sorted(
    p for p in STATIC_DIR.rglob("*") if p.suffix in (".js", ".html", ".css") and p.is_file()
)
SINKS = re.compile(
    r"\b(innerHTML|outerHTML|insertAdjacentHTML|document\.write|eval\s*\(|new\s+Function\b)"
)
INLINE_HANDLER = re.compile(r"""\bon[a-z]+\s*=\s*["'`]""", re.IGNORECASE)
REMOTE = re.compile(r"""(?:\b(?:https?|wss?|ftp):|(?:url\(|src=|href=)\s*["']?//)""", re.I)
LICENSE_BANNER = re.compile(r"/\*!.*?\*/", re.DOTALL)


def test_app_files_found() -> None:
    names = {p.relative_to(STATIC_DIR).as_posix() for p in APP_FILES}
    assert {"index.html", "shell.js", "shell.css", "tokens.css", "views/replay.js",
            "views/history.js", "vendor/uplot/uPlot.iife.min.js"} <= names  # fmt: skip


@pytest.mark.parametrize("path", APP_FILES, ids=lambda p: p.relative_to(STATIC_DIR).as_posix())
def test_no_html_sinks_or_eval(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert not SINKS.findall(text)
    assert not INLINE_HANDLER.findall(text)


@pytest.mark.parametrize("path", APP_FILES, ids=lambda p: p.relative_to(STATIC_DIR).as_posix())
def test_no_remote_host(path: Path) -> None:
    text = LICENSE_BANNER.sub("", path.read_text(encoding="utf-8"))
    assert not REMOTE.findall(text), f"{path.name} references a remote host"


def test_html_has_no_inline_script_or_style() -> None:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    assert "<style" not in html and " style=" not in html
    assert all("src=" in tag for tag in re.findall(r"<script[^>]*>", html))
    assert "script-src 'self'" in html


def test_licenses_vendored() -> None:
    assert (STATIC_DIR / "vendor/uplot/LICENSE").is_file()
    assert len(list((STATIC_DIR / "fonts").glob("LICENSE-*.txt"))) == 3
    assert len(list((STATIC_DIR / "fonts").glob("*.woff2"))) == 6


# Role-named colour tokens (team-branding D-a): no amber-named token, FP.tokens() key or raw
# amber RGB survives in the shipped files; glows and grid lines are tokens built from the accent.
AMBER_TOKEN = re.compile(r"(?<![\w-])--(?:amber[\w-]*|on-amber|hot)\b")
AMBER_KEY = re.compile(r"\bamber\w*\b|\b(?:t|tokens|FP\.tokens\(\))\.hot\b")
RAW_AMBER = re.compile(r"rgba?\(\s*255\s*,\s*176\s*,\s*0\b")
# Any literal colour channel outside tokens.css: palette colours (glows, tints, backdrops) must
# come from the theme's tokens. Black (shadows, scanlines, transparent strokes) has no hue.
RAW_RGB = re.compile(r"rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)")
HEX_LITERAL = re.compile(r"#[0-9A-Fa-f]{6}\b|#[0-9A-Fa-f]{3}\b")


@pytest.mark.parametrize("path", APP_FILES, ids=lambda p: p.relative_to(STATIC_DIR).as_posix())
def test_no_amber_named_tokens_or_raw_amber(path: Path) -> None:
    if "vendor" in path.parts:
        return
    text = path.read_text(encoding="utf-8")
    assert not AMBER_TOKEN.findall(text), f"{path.name} uses an amber-named token"
    assert not RAW_AMBER.findall(text), f"{path.name} hard-codes the amber RGB"
    if path.suffix == ".js":
        assert not AMBER_KEY.findall(text), f"{path.name} uses an amber-named FP.tokens() key"


@pytest.mark.parametrize("path", APP_FILES, ids=lambda p: p.relative_to(STATIC_DIR).as_posix())
def test_no_raw_palette_colours_outside_tokens(path: Path) -> None:
    """Spec: every colour (glows included) is derived from the palette, so no file but
    tokens.css names a colour by its channels, and no stylesheet names one in hex."""
    if "vendor" in path.parts or path.name == "tokens.css":
        return
    text = path.read_text(encoding="utf-8")
    raw = [m.group(0) for m in RAW_RGB.finditer(text) if m.groups() != ("0", "0", "0")]
    assert not raw, f"{path.name} hard-codes {raw}"
    if path.suffix == ".css":
        assert not HEX_LITERAL.findall(text), f"{path.name} hard-codes a hex colour"


def test_role_named_tokens_defined() -> None:
    css = (STATIC_DIR / "tokens.css").read_text(encoding="utf-8")
    for name in ("accent", "accent-hi", "accent-mid", "accent-dim", "accent-muted", "prose",
                 "warn", "on-accent", "accent-rgb", "accent-hi-rgb", "warn-rgb",
                 "bg-rgb", "grid-line"):  # fmt: skip
        assert re.search(rf"^\s*--{name}:", css, re.MULTILINE), name
