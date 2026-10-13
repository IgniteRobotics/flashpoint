import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from flashpoint.lake.paths import LakePaths
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.server import FlashpointServer
from tests.web.anomaly_lake import build_filter_lake


@dataclass
class Reply:
    status: int
    headers: dict[str, str]
    body: bytes


@dataclass
class Running:
    server: FlashpointServer
    base: str

    def get(
        self, path: str, method: str = "GET", body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> Reply:  # fmt: skip
        request = Request(self.base + path, data=body, headers=headers or {}, method=method)  # noqa: S310 - local test server
        try:
            with urlopen(request) as response:  # noqa: S310
                return Reply(response.status, dict(response.headers), response.read())
        except HTTPError as exc:
            return Reply(exc.code, dict(exc.headers), exc.read())

    def json(self, path: str) -> tuple[int, Any]:
        import json

        reply = self.get(path)
        return reply.status, json.loads(reply.body)


@pytest.fixture
def serve() -> Iterator[Any]:
    started: list[FlashpointServer] = []

    def start(
        lake: LakePaths, static_dir: Path | None = None, robots: Any = None, launcher: Any = None,
        anomalies: Any = None, spawner: Any = None,
    ) -> Running:  # fmt: skip
        kwargs: dict[str, Any] = {"static_dir": static_dir} if static_dir else {}
        if launcher is not None:
            kwargs["launcher"] = launcher
        if anomalies is not None:
            kwargs["anomalies"] = anomalies
        if spawner is not None:
            kwargs["spawner"] = spawner
        server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake, robots), **kwargs)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        started.append(server)
        return Running(server, f"http://127.0.0.1:{server.server_address[1]}")

    yield start
    for server in started:
        server.shutdown()
        server.server_close()
        server.queries.close()


@pytest.fixture(scope="module")
def cross_site(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    """Replay, History, and Anomalies served on one lake: build_filter_lake (two 2026 events,
    robots comp and practice, a 2025-only robot, anomalies on both, 2026gacol never built)."""
    lake, found = build_filter_lake(tmp_path_factory.mktemp("cross") / "lake", replay=True)
    server = FlashpointServer(("127.0.0.1", 0), lake, HistoryQueries(lake))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield {"url": f"http://127.0.0.1:{server.server_address[1]}/", "ids": found}
    server.shutdown()
    server.server_close()
    server.queries.close()
