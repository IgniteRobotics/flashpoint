"""`flashpoint serve`: one local, read-only app (stdlib HTTP, no web framework).

Routes: `/` and `/static/*` from the package, `/data/*` from `<lake>/report/data/`,
`/raw/<sha256>/<name>` for ledger hashes only, and `/api/*`. Everything else is 404.
"""

import ipaddress
import json
import re
import shutil
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from flashpoint.lake.paths import LakePaths
from flashpoint.lake.raw import raw_path
from flashpoint.report.names import safe_name
from flashpoint.report.paths import data_dir
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.api import Api

STATIC_DIR = Path(__file__).parent / "static"
DEFAULT_HOST, DEFAULT_PORT = "127.0.0.1", 8000
_SHA = re.compile(r"^[0-9a-f]{64}$")
_DATA_NAME = re.compile(r"^[A-Za-z0-9_.-]+\.js$")
CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json",
    ".woff2": "font/woff2",
    ".svg": "image/svg+xml",
    ".txt": "text/plain; charset=utf-8",
    ".md": "text/plain; charset=utf-8",
}
CSP = (
    "default-src 'self'; script-src 'self'; style-src 'self'; font-src 'self';"
    " img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none';"
    " frame-ancestors 'none'; form-action 'none'"
)


def contained(base: Path, relative: str) -> Path | None:
    """`base/relative` if it is a file inside base (no traversal, no absolute parts)."""
    parts = relative.split("/")
    if any(p in ("", ".", "..") or "\\" in p or ":" in p or "\0" in p for p in parts):
        return None
    root = base.resolve()
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        return None
    return path


class FlashpointServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        lake: LakePaths,
        queries: HistoryQueries,
        static_dir: Path = STATIC_DIR,
    ) -> None:
        super().__init__(address, Handler)
        self.lake = lake
        self.queries = queries
        self.api = Api(queries)
        self.static_dir = static_dir

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        host = host.decode() if isinstance(host, bytes) else str(host)
        shown = "127.0.0.1" if host in ("0.0.0.0", "") else host  # noqa: S104
        return f"http://{shown}:{port}/"


class Handler(BaseHTTPRequestHandler):
    server: FlashpointServer
    server_version = "flashpoint"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib signature
        pass  # quiet: the pit laptop's terminal shows the summary, not every request

    def _headers(self, status: int, content_type: str, length: int, extra: dict[str, str]) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(length))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", CSP)
        for key, value in extra.items():
            self.send_header(key, value)
        self.end_headers()

    def _send_bytes(
        self, status: int, body: bytes, content_type: str, extra: dict[str, str] | None = None
    ) -> None:
        self._headers(status, content_type, len(body), extra or {})
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode()
        self._send_bytes(status, body, "application/json", {"Cache-Control": "no-store"})

    def _not_found(self) -> None:
        self._send_bytes(HTTPStatus.NOT_FOUND, b"not found\n", "text/plain; charset=utf-8")

    def _send_file(
        self, path: Path, extra: dict[str, str] | None = None, content_type: str | None = None
    ) -> None:
        content_type = content_type or CONTENT_TYPES.get(path.suffix, "application/octet-stream")
        size = path.stat().st_size
        self._headers(HTTPStatus.OK, content_type, size, extra or {})
        if self.command != "HEAD":
            with path.open("rb") as f:
                shutil.copyfileobj(f, self.wfile)

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib naming
        self.do_GET()

    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        url = urlsplit(self.path)
        path = unquote(url.path)
        try:
            if path in ("/", "/index.html"):
                index = self.server.static_dir / "index.html"
                self._send_file(index, {"Cache-Control": "no-store"})
            elif path.startswith("/static/"):
                found = contained(self.server.static_dir, path.removeprefix("/static/"))
                self._send_file(found) if found else self._not_found()
            elif path.startswith("/data/"):
                self._data(path.removeprefix("/data/"))
            elif path.startswith("/raw/"):
                self._raw(path.removeprefix("/raw/"))
            elif url.path.startswith("/api/"):
                status, payload = self.server.api.handle(
                    url.path.removeprefix("/api/"), parse_qs(url.query, keep_blank_values=True)
                )
                self._send_json(status, payload)
            else:
                self._not_found()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _method_not_allowed(self) -> None:
        self._send_bytes(
            HTTPStatus.METHOD_NOT_ALLOWED, b"read-only\n", "text/plain", {"Allow": "GET, HEAD"}
        )

    do_POST = do_PUT = do_DELETE = do_PATCH = _method_not_allowed  # noqa: N815

    def _data(self, name: str) -> None:
        found = contained(data_dir(self.server.lake), name) if _DATA_NAME.match(name) else None
        if found is None:
            self._not_found()
        else:
            self._send_file(found, {"Cache-Control": "no-store"})

    def _raw(self, rest: str) -> None:
        sha, _, name = rest.partition("/")
        if not _SHA.match(sha) or not name or "/" in name:
            self._not_found()
            return
        kind = self.server.queries.raw_kind(sha)  # ledger hashes only
        path = raw_path(self.server.lake, sha, f".{kind}") if kind else None
        if path is None or not path.is_file():
            self._not_found()
            return
        self._send_file(
            path,
            {"Content-Disposition": f'attachment; filename="{safe_name(name)}"'},
            "application/octet-stream",
        )


def is_local(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host == "localhost"


def host_warning(host: str) -> str | None:
    if is_local(host):
        return None
    return (
        f"WARNING: serving on {host} shares the lake's data and raw logs with everyone on the"
        " pit network, read-only and with no password."
    )


def run(server: FlashpointServer) -> int:
    """Serve until interrupted; Ctrl-C exits cleanly with 0."""
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped", file=sys.stderr)
    finally:
        server.server_close()
        server.queries.close()
    return 0
