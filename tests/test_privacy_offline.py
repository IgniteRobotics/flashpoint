"""Nothing leaves the laptop (spec project-documentation, "Privacy and network note").

With owlet already cached and every non-loopback connection refused, ingest (which derives,
detects and builds Replay data), report and serve one corpus match, and check that no step
tried to reach another host. Socket connects and name lookups are patched in this process;
owlet runs as a subprocess and isn't covered here (it converts a local file).
"""

import ipaddress
import socket
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import pytest

from flashpoint.cli import main
from flashpoint.lake.paths import LakePaths
from flashpoint.views.queries import HistoryQueries
from flashpoint.web.server import FlashpointServer

LOOPBACK_NAMES = {None, "", "localhost"}


def _is_loopback(host: Any) -> bool:
    if host in LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(str(host).split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def _is_address(host: Any) -> bool:
    try:
        ipaddress.ip_address(str(host).split("%", 1)[0])
    except ValueError:
        return False
    return True


@pytest.fixture
def outbound(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every non-loopback connect or lookup attempted, each refused with OSError."""
    attempts: list[str] = []
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_getaddrinfo = socket.getaddrinfo

    def guard(sock: socket.socket, address: Any) -> None:
        if sock.family in (socket.AF_INET, socket.AF_INET6) and not _is_loopback(address[0]):
            attempts.append(f"connect {address}")
            raise OSError(f"test: outbound connection to {address} refused")

    def connect(sock: socket.socket, address: Any) -> None:
        guard(sock, address)
        real_connect(sock, address)

    def connect_ex(sock: socket.socket, address: Any) -> int:
        guard(sock, address)
        return real_connect_ex(sock, address)

    def getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
        if not _is_loopback(host) and not _is_address(host):  # a name: a DNS query
            attempts.append(f"lookup {host}")
            raise socket.gaierror(f"test: lookup of {host} refused")
        return real_getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    return attempts


def test_guard_refuses_and_records_outbound(outbound: list[str]) -> None:
    with pytest.raises(OSError, match="outbound connection to"):
        socket.create_connection(("192.0.2.1", 80), timeout=1)  # TEST-NET-1, never routed
    with pytest.raises(OSError, match=r"lookup of example\.com refused"):
        socket.getaddrinfo("example.com", 443)
    assert outbound == ["connect ('192.0.2.1', 80)", "lookup example.com"]


@pytest.mark.corpus
def test_one_match_end_to_end_makes_no_outbound_connection(
    corpus_group: Callable[[str], list[Path]], tmp_path: Path, outbound: list[str]
) -> None:
    lake = tmp_path / "lake"
    files = corpus_group("2026-gacmp-q7")  # owlet for its hoots is cached by the corpus run

    assert main(["ingest", "--lake", str(lake), *map(str, files)]) == 0
    assert main(["report", "--lake", str(lake)]) == 0

    paths = LakePaths(lake)
    server = FlashpointServer(("127.0.0.1", 0), paths, HistoryQueries(paths))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        for path in (
            "/", "/data/matches.js", "/data/2026gacmp_qm7.js", "/api/setup/checks",
            "/api/anomalies/summary",
        ):  # fmt: skip
            with urlopen(base + path, timeout=30) as response:
                assert response.status == 200, path
    finally:
        server.shutdown()
        server.server_close()
        server.queries.close()
    assert outbound == [], outbound
