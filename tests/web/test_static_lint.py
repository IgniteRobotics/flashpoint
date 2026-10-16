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
