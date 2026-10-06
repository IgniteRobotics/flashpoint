"""The guarded launch request (spec: advantagescope-launch)."""

import itertools
import json
import os
import stat
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from flashpoint.web.advantagescope import (
    NETWORK,
    Install,
    Launcher,
    command,
    refusal,
)
from tests.report.lake import MatchLake

KEY = "2026johnson_qm15"
HOOT_ONLY = "2026johnson_qm53"


class Recorder:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> None:
        self.calls.append(argv)


@pytest.fixture
def lake(tmp_path: Path) -> MatchLake:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match(KEY, "q15", None, wpilog_name="FRC_20260430_151512_JOHNSON_Q15.wpilog")
    lake.add_match(HOOT_ONLY, "q53", None, with_wpilog=False)
    lake.write_meta()
    return lake


@pytest.fixture
def app_path(tmp_path: Path) -> Path:
    path = tmp_path / "bin" / "advantagescope"
    path.parent.mkdir()
    path.write_text("#!/bin/sh\n")
    return path


@pytest.fixture
def launched(serve: Any, lake: MatchLake, app_path: Path, tmp_path: Path) -> tuple[Any, Recorder]:
    recorder = Recorder()
    launcher = Launcher(Install(app_path, "config"), root=tmp_path / "stage", spawn=recorder)
    return serve(lake.lake, launcher=launcher), recorder


def _headers(app: Any, **overrides: str | None) -> dict[str, str]:
    host = app.base.removeprefix("http://")
    headers: dict[str, str | None] = {
        "Host": host,
        "Origin": f"http://{host}",
        "X-Flashpoint": "launch",
        "Content-Type": "application/json",
        **overrides,
    }
    return {k: v for k, v in headers.items() if v is not None}


def _post(app: Any, body: Any, **overrides: str | None) -> tuple[int, Any]:
    reply = app.get("/api/launch", "POST", json.dumps(body).encode(), _headers(app, **overrides))
    return reply.status, json.loads(reply.body) if reply.body.startswith(b"{") else reply.body


def _tree(path: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(path)): p.read_bytes() for p in sorted(path.rglob("*")) if p.is_file()
    }


# --- the guard -----------------------------------------------------------------------------


def _all_local(port: int) -> dict[str, str]:
    return {
        "Host": f"127.0.0.1:{port}",
        "Origin": f"http://127.0.0.1:{port}",
        "X-Flashpoint": "launch",
        "Content-Type": "application/json",
    }


def test_refusal_accepts_only_the_all_local_row() -> None:
    port = 8123
    good = _all_local(port)
    assert refusal(True, port, "127.0.0.1", good) is None
    assert refusal(True, port, "::1", {**good, "Host": f"[::1]:{port}",
                                       "Origin": f"http://[::1]:{port}"}) is None  # fmt: skip
    named = {**good, "Host": f"localhost:{port}", "Origin": f"http://localhost:{port}"}
    assert refusal(True, port, "127.0.0.1", named) is None
    assert refusal(True, port, "127.0.0.1",
                   {**good, "Content-Type": "application/json; charset=utf-8"}) is None  # fmt: skip
    variants = {
        "bound": [True, False],
        "client": ["127.0.0.1", "10.68.29.5"],
        "host": [f"127.0.0.1:{port}", f"evil.example:{port}", "127.0.0.1:9999", None],
        "origin": ["same", "http://evil.example", "null", None],
        "marker": ["launch", "other", None],
        "type": ["application/json", "text/plain", None],
    }
    accepted = []
    for row in itertools.product(*variants.values()):
        bound, client, host, origin, marker, ctype = row
        headers = {
            "Host": host,
            "Origin": f"http://{host}" if origin == "same" and host else origin,
            "X-Flashpoint": marker,
            "Content-Type": ctype,
        }
        reason = refusal(bound, port, client, {k: v for k, v in headers.items() if v is not None})
        if reason is None:
            accepted.append(row)
        else:
            assert isinstance(reason, str) and reason
    assert accepted == [
        (True, "127.0.0.1", f"127.0.0.1:{port}", "same", "launch", "application/json")
    ]
    assert refusal(False, port, "127.0.0.1", good) == NETWORK


def test_ipv4_mapped_loopback_client() -> None:
    assert refusal(True, 1, "::ffff:127.0.0.1", _all_local(1)) is None


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"Origin": "http://evil.example"}, "cross-origin"),
        ({"Origin": None}, "cross-origin"),
        ({"X-Flashpoint": None}, "X-Flashpoint"),
        ({"Host": "rebound.example:8000"}, "Host"),
        ({"Content-Type": "text/plain"}, "Content-Type"),
    ],
)
def test_refused_requests_start_nothing(
    launched: tuple[Any, Recorder], override: dict[str, str | None], reason: str
) -> None:
    app, recorder = launched
    status, body = _post(app, {"match_key": KEY}, **override)
    assert status == 403 and body["started"] is False and reason in body["reason"]
    assert recorder.calls == []


def test_other_methods_never_launch(launched: tuple[Any, Recorder]) -> None:
    app, recorder = launched
    assert app.get("/api/launch").status == 200  # availability only
    assert app.get("/api/launch", "HEAD").status == 200
    for method in ("OPTIONS", "PUT", "DELETE", "PATCH"):
        reply = app.get("/api/launch", method, b'{"match_key": "2026johnson_qm15"}', _headers(app))
        assert reply.status in (405, 501)
        assert "Access-Control-Allow-Origin" not in reply.headers
    assert recorder.calls == []


def test_post_elsewhere_is_still_read_only(launched: tuple[Any, Recorder]) -> None:
    app, recorder = launched
    assert app.get("/api/filters", "POST", b"{}", _headers(app)).status == 405
    assert app.get("/api/match/" + KEY, "POST", b"{}", _headers(app)).status == 405
    assert recorder.calls == []


# --- the request body ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "key", ["../../etc/passwd", "2026nowhere_qm1", "2026JOHNSON_QM15", "", "x' OR 1=1 --"]
)
def test_unknown_or_hostile_key(
    launched: tuple[Any, Recorder], lake: MatchLake, tmp_path: Path, key: str
) -> None:
    app, recorder = launched
    before = _tree(lake.root)
    status, body = _post(app, {"match_key": key})
    assert status == 404 and body["started"] is False
    assert recorder.calls == [] and not (tmp_path / "stage").exists()
    assert _tree(lake.root) == before


@pytest.mark.parametrize("body", [{"match_key": 15}, {}, [KEY], "2026johnson_qm15"])
def test_malformed_body(launched: tuple[Any, Recorder], body: Any) -> None:
    app, recorder = launched
    status, _ = _post(app, body)
    assert status in (400, 404) and recorder.calls == []


def test_invalid_json_and_oversized_body(launched: tuple[Any, Recorder]) -> None:
    app, recorder = launched
    assert app.get("/api/launch", "POST", b"{not json", _headers(app)).status == 400
    big = json.dumps({"match_key": KEY, "pad": "x" * 10_000}).encode()
    assert app.get("/api/launch", "POST", big, _headers(app)).status == 413
    assert recorder.calls == []


def test_launch_q15(launched: tuple[Any, Recorder], lake: MatchLake, tmp_path: Path) -> None:
    app, recorder = launched
    before = _tree(lake.lake.raw)
    status, body = _post(app, {"match_key": KEY, "path": "/bin/sh", "args": ["-c", "id"]})
    assert status == 200 and body["started"] is True
    folder = tmp_path / "stage" / KEY
    assert body["folder"] == str(folder)
    wpilog = f"{KEY}__wpilog__FRC_20260430_151512_JOHNSON_Q15.wpilog"
    assert body["wpilog"] == wpilog
    assert len(body["hoots"]) == 2 and all(h["name"].startswith(f"{KEY}__") for h in body["hoots"])
    assert recorder.calls == [command(app.server.launcher.install.path, folder / wpilog)]
    assert _tree(lake.lake.raw) == before


def test_hoot_only_match(launched: tuple[Any, Recorder], tmp_path: Path) -> None:
    app, recorder = launched
    status, body = _post(app, {"match_key": HOOT_ONLY})
    assert status == 409 and body["started"] is False and "no wpilog" in body["reason"]
    assert recorder.calls == [] and not (tmp_path / "stage" / HOOT_ONLY).exists()


def test_missing_raw_file(launched: tuple[Any, Recorder], lake: MatchLake) -> None:
    app, recorder = launched
    hoot = next(r for r in lake.tables["files"] if r["kind"] == "hoot")
    raw = lake.lake.raw / hoot["sha256"][:2] / f"{hoot['sha256']}.hoot"
    raw.chmod(0o644)
    raw.unlink()
    status, body = _post(app, {"match_key": KEY})
    assert status == 500 and body["started"] is False and hoot["sha256"] in body["reason"]
    assert recorder.calls == []


def test_spawn_failure(serve: Any, lake: MatchLake, app_path: Path, tmp_path: Path) -> None:
    def broken(argv: list[str]) -> None:
        raise PermissionError(13, "Permission denied", argv[0])

    app = serve(lake.lake, launcher=Launcher(Install(app_path, "config"), root=tmp_path / "s",
                                             spawn=broken))  # fmt: skip
    status, body = _post(app, {"match_key": KEY})
    assert status == 500 and body["started"] is False and "did not start" in body["reason"]


def test_availability(serve: Any, lake: MatchLake, app_path: Path, tmp_path: Path) -> None:
    found = serve(lake.lake, launcher=Launcher(Install(app_path, "WPILib 2026"), root=tmp_path))
    status, body = found.json("/api/launch")
    assert status == 200
    assert body == {"available": True, "reason": None, "app": str(app_path),
                    "found_by": "WPILib 2026"}  # fmt: skip
    missing = serve(lake.lake, launcher=Launcher(None, "AdvantageScope not found", root=tmp_path))
    status, body = missing.json("/api/launch")
    assert body["available"] is False and body["reason"] == "AdvantageScope not found"
    status, body = _post(missing, {"match_key": KEY})
    assert status == 503 and body["started"] is False


def test_default_server_has_no_launch(serve: Any, lake: MatchLake) -> None:
    app = serve(lake.lake)
    status, body = app.json("/api/launch")
    assert status == 200 and body["available"] is False


def test_command_per_platform(tmp_path: Path) -> None:
    app = tmp_path / "AdvantageScope (WPILib).app"
    log = tmp_path / "a b.wpilog"
    assert command(app, log, platform="darwin") == ["open", "-a", str(app), str(log)]
    exe = tmp_path / "AdvantageScope.exe"
    assert command(exe, log, platform="win32") == [str(exe), str(log)]
    assert command(exe, log, platform="darwin") == [str(exe), str(log)]


# --- real exec -----------------------------------------------------------------------------

STAND_IN = """#!{python}
import json, sys, time
with open({out!r}, "w") as f:
    json.dump(sys.argv[1:], f)
time.sleep(3)
"""


@pytest.mark.skipif(os.name == "nt", reason="a script stand-in needs a POSIX shebang")
def test_real_exec_with_a_stand_in(serve: Any, tmp_path: Path) -> None:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match(KEY, "q15", None, wpilog_name="FRC Q15 'final' run.wpilog")
    lake.write_meta()
    out = tmp_path / "argv.json"
    stand_in = tmp_path / "AdvantageScope stand-in"
    stand_in.write_text(STAND_IN.format(python=sys.executable, out=str(out)))
    stand_in.chmod(stand_in.stat().st_mode | stat.S_IXUSR)
    root = tmp_path / "Temp dir 'quoted' (x86)"  # like a Windows user's temp folder
    app = serve(lake.lake, launcher=Launcher(Install(stand_in, "config"), root=root))
    started = time.monotonic()
    status, body = _post(app, {"match_key": KEY})
    assert status == 200 and body["started"] is True
    assert time.monotonic() - started < 2  # not waited on
    assert app.json("/api/filters")[0] == 200  # the server keeps answering
    deadline = time.monotonic() + 5
    while not out.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    argv = json.loads(out.read_text())
    assert argv == [str(root / KEY / body["wpilog"])]
    assert body["wpilog"] == f"{KEY}__wpilog__FRC_Q15_final_run.wpilog"


def test_header_names_are_case_insensitive(launched: tuple[Any, Recorder]) -> None:
    app, recorder = launched
    host = app.base.removeprefix("http://")
    headers = {"host": host, "origin": f"http://{host}", "x-flashpoint": "launch",
               "content-type": "application/json"}  # fmt: skip
    reply = app.get("/api/launch", "POST", json.dumps({"match_key": KEY}).encode(), headers)
    assert reply.status == 200 and len(recorder.calls) == 1
