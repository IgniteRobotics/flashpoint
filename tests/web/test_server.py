import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.report.build import ReportBuilder
from flashpoint.web.server import contained, is_local
from tests.report.lake import MatchLake, quiet_rows


@pytest.fixture
def site(tmp_path: Path) -> tuple[MatchLake, Path]:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows(), wpilog_name="x');alert(1);//.wpilog")
    builder = ReportBuilder(lake.write_meta(), config.config_root())
    builder.build()
    builder.close()
    static = tmp_path / "static"
    (static / "views").mkdir(parents=True)
    (static / "index.html").write_text("<!doctype html><title>Flashpoint</title>")
    (static / "shell.css").write_text("body{}")
    (static / "views" / "replay.js").write_text("export {};")
    (tmp_path / "lake" / "secret.txt").write_text("secret")
    return lake, static


def test_routes(serve: Any, site: tuple[MatchLake, Path]) -> None:
    lake, static = site
    app = serve(lake.lake, static)
    index = app.get("/")
    assert index.status == 200 and index.headers["Content-Type"].startswith("text/html")
    assert "script-src 'self'" in index.headers["Content-Security-Policy"]
    assert app.get("/static/shell.css").headers["Content-Type"].startswith("text/css")
    assert app.get("/static/views/replay.js").headers["Content-Type"].startswith("text/javascript")
    data = app.get("/data/2026gacmp_qm7.js")
    assert data.status == 200 and data.body.startswith(b"FP.register(")
    assert app.get("/data/matches.js").body.startswith(b"FP.index(")
    status, filters = app.json("/api/filters")
    assert status == 200 and filters["lake"] == str(lake.root)
    assert app.get("/static/shell.css", "HEAD").body == b""


def test_raw_download_for_ledger_hashes_only(serve: Any, site: tuple[MatchLake, Path]) -> None:
    lake, static = site
    app = serve(lake.lake, static)
    status, match = app.json("/api/match/2026gacmp_qm7")
    assert status == 200 and match["built"] is True
    wpilog = next(s for s in match["sources"] if s["part"] == "wpilog")
    reply = app.get(f"/raw/{wpilog['sha256']}/{wpilog['download']}")
    assert reply.status == 200 and reply.body == b"wpilog q7"
    assert reply.headers["Content-Disposition"] == (
        'attachment; filename="2026gacmp_qm7__wpilog__x_alert_1_.wpilog"'
    )
    assert reply.headers["Content-Type"] == "application/octet-stream"
    unknown = "0" * 64
    assert app.get(f"/raw/{unknown}/x.wpilog").status == 404
    assert app.get(f"/raw/{wpilog['sha256']}").status == 404
    assert app.get("/raw/../meta/flashpoint.sqlite/x").status == 404


@pytest.mark.parametrize(
    "path",
    [
        "/../secret.txt",
        "/static/../index.html",
        "/static/..%2f..%2flake%2fsecret.txt",
        "/static/%2e%2e/%2e%2e/lake/secret.txt",
        "/data/../secret.txt",
        "/data/..%2fsecret.txt",
        "/data/%2e%2e%2fmeta%2fsessions.parquet",
        "/data/site-state.json",
        "/../ledger.sqlite",
        "/meta/flashpoint.sqlite",
        "/static/",
        "/static//etc/passwd",
        "/favicon.ico",
        "/report/site-state.json",
        "/static/C:%5cWindows%5cwin.ini",
    ],
)
def test_everything_else_is_not_found(serve: Any, site: tuple[MatchLake, Path], path: str) -> None:
    lake, static = site
    app = serve(lake.lake, static)
    reply = app.get(path)
    assert reply.status == 404 and b"secret" not in reply.body


def test_read_only_methods(serve: Any, site: tuple[MatchLake, Path]) -> None:
    lake, static = site
    app = serve(lake.lake, static)
    assert app.get("/api/filters", "POST").status == 405
    assert app.get("/data/matches.js", "DELETE").status == 405


def test_contained(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "f.txt").write_text("x")
    (tmp_path / "outside.txt").write_text("x")
    assert contained(tmp_path / "a", "f.txt") == (tmp_path / "a" / "f.txt").resolve()
    for bad in ("../outside.txt", "", "/etc/passwd", "a/../../outside.txt", "f.txt/", "x\\y"):
        assert contained(tmp_path / "a", bad) is None


@pytest.mark.skipif(sys.platform == "win32", reason="symlinks need privileges on Windows")
def test_symlink_out_of_static_is_refused(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "outside.txt").write_text("x")
    (tmp_path / "a" / "link.txt").symlink_to(tmp_path / "outside.txt")
    assert contained(tmp_path / "a", "link.txt") is None


def test_is_local() -> None:
    assert is_local("127.0.0.1") and is_local("::1") and is_local("localhost")
    assert not is_local("0.0.0.0") and not is_local("10.68.29.5")  # noqa: S104


_SERVE = """
import sys
from flashpoint.cli import main
sys.exit(main(sys.argv[1:]))
"""


def _rss_kib(pid: int) -> int:
    done = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
    return int(done.stdout.strip())


@pytest.mark.skipif(sys.platform == "win32", reason="ps and SIGINT semantics differ on Windows")
def test_serve_command_lazy_and_clean_interrupt(tmp_path: Path) -> None:
    import signal
    from urllib.request import urlopen

    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", quiet_rows())
    lake.write_meta()
    process = subprocess.Popen(
        [sys.executable, "-c", _SERVE, "serve", "--lake", str(lake.root), "--port", "0",
         "--no-build"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )  # fmt: skip
    try:
        assert process.stdout is not None
        line = process.stdout.readline()
        url = line.split()[-1]
        assert url.startswith("http://127.0.0.1:")
        with urlopen(url) as response:  # noqa: S310 - local test server
            assert response.status == 200
        time.sleep(0.5)
        assert _rss_kib(process.pid) < 150 * 1024  # DuckDB not opened yet
        with urlopen(url + "api/summary") as response:  # noqa: S310
            assert json.loads(response.read())["empty"] is True
        process.send_signal(signal.SIGINT)
        assert process.wait(timeout=10) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_host_warning() -> None:
    from flashpoint.web.server import host_warning

    assert host_warning("127.0.0.1") is None
    warning = host_warning("0.0.0.0")  # noqa: S104
    assert warning is not None and "pit network" in warning and "no password" in warning


def test_static_dir_ships_with_package() -> None:
    from flashpoint.web.server import STATIC_DIR

    assert (STATIC_DIR / "index.html").is_file()
