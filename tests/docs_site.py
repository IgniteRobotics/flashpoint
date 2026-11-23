"""The built docs site for browser tests (docs-visual-polish groups 2 and 4): the site and the
offline archive built with the real configs, a loopback server, and a page that blocks every
other host and records what it asked for.

Zensical builds only into a folder beside its config, so these build the repository's own
`site/` and `offline-site/` (both gitignored), as a local `zensical build` does.
"""

import functools
import http.server
import importlib.util
import subprocess
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import urlsplit

REPO = Path(__file__).resolve().parents[1]
SITE = REPO / "site"
OFFLINE = REPO / "offline-site"
MEDIA = REPO / "docs" / "guide" / "assets" / "media"
LOCAL_HOSTS = ("127.0.0.1", "localhost")
# Theme hook for tests only: the theme draws each diagram in a closed shadow root, which a test
# can't look into. Opened, the SVG is reachable as `div.mermaid svg`.
OPEN_SHADOW_ROOTS = (
    "const attach = Element.prototype.attachShadow;"
    "Element.prototype.attachShadow = function (o) {"
    " return attach.call(this, { ...o, mode: 'open' }); };"
)
# Files a page asks for that only the deploy provides: mike writes versions.json next to the
# versions, and the offline archive has no search index (mkdocs.offline.yml says search may not
# work from file://).
DEPLOY_ONLY = ("/versions.json", "/search.js")


def docs_media() -> ModuleType:
    spec = importlib.util.spec_from_file_location("docs_media", REPO / "tools" / "docs-media.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_sites() -> tuple[Path, Path]:
    """The vendored renderer (fetched once, by checksum), stand-in media when none have been
    captured, and both strict builds."""
    tool = docs_media()
    tool.vendor()
    if not (MEDIA / "replay.png").is_file():
        tool.placeholders(MEDIA)
    for config in ("mkdocs.yml", "mkdocs.offline.yml"):
        command = [sys.executable, "-m", "zensical", "build", "--strict", "--clean", "-f", config]
        done = subprocess.run(command, cwd=REPO, check=False, capture_output=True, text=True)
        assert done.returncode == 0, f"{config}: {done.stdout}{done.stderr}"
    return SITE, OFFLINE


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return None


@contextmanager
def serve(site: Path) -> Iterator[str]:
    """The built site on loopback; yields its base URL."""
    handler = functools.partial(_QuietHandler, directory=str(site))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


@dataclass
class Watch:
    """What one page asked for, what was blocked, and what it logged as an error."""

    requests: list[str] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)
    errors: list[tuple[str, str]] = field(default_factory=list)  # (text, url)

    def console_problems(self) -> list[str]:
        """Console and page errors, less the failed loads this test caused by blocking a host
        and the files only the deploy provides."""
        return [
            f"console error: {text} ({url})" if url else f"page error: {text}"
            for text, url in self.errors
            if not (
                text.startswith("Failed to load resource")
                and (url in self.blocked or urlsplit(url).path.endswith(DEPLOY_ONLY))
            )
        ]


def open_page(browser: Any, **context: Any) -> tuple[Any, Watch]:
    """A page that may reach only this laptop (loopback and files), with its watch."""
    ctx = browser.new_context(**context)
    page = ctx.new_page()
    page.add_init_script(OPEN_SHADOW_ROOTS)
    watch = Watch()

    def route(r: Any) -> None:
        url = urlsplit(r.request.url)
        if url.scheme in ("file", "data", "blob") or url.hostname in LOCAL_HOSTS:
            r.continue_()
        else:
            watch.blocked.append(r.request.url)
            r.abort()

    page.route("**/*", route)
    page.on("request", lambda r: watch.requests.append(r.url))
    page.on(
        "console",
        lambda m: (
            watch.errors.append((m.text, m.location.get("url", ""))) if m.type == "error" else None
        ),
    )
    page.on("pageerror", lambda e: watch.errors.append((str(e), "")))
    return page, watch
