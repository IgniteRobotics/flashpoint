import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from flashpoint.lake.paths import LakePaths
from flashpoint.setup.checks import CheckContext, CheckResult
from flashpoint.web.advantagescope import refusal
from flashpoint.web.server import MAX_SETUP_BODY, FlashpointServer


class FixRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def action(self, name: str) -> Callable[..., dict[str, Any]]:
        def run(ctx: Any, body: Any) -> dict[str, Any]:
            self.calls.append((name, body))
            return {"ok": True, "message": f"{name} done", "changed": []}

        return run


@pytest.fixture
def lake(tmp_path: Path) -> LakePaths:
    return LakePaths(tmp_path / "lake")


@pytest.fixture
def setup_app(serve: Any, lake: LakePaths, tmp_path: Path) -> tuple[Any, FixRecorder]:
    recorder = FixRecorder()
    app = serve(lake)
    server: FlashpointServer = app.server
    server.config_dir = tmp_path / "cfg"
    server.check_context = lambda: CheckContext(
        tmp_path / "cfg", lake, tmp_path / "cache", system="Darwin", season=2026
    )
    server.fixes = {name: recorder.action(name) for name in ("init", "owlet")}
    return app, recorder


def _headers(app: Any, **overrides: str | None) -> dict[str, str]:
    host = app.base.removeprefix("http://")
    headers: dict[str, str | None] = {
        "Host": host,
        "Origin": f"http://{host}",
        "X-Flashpoint": "setup",
        "Content-Type": "application/json",
        **overrides,
    }
    return {k: v for k, v in headers.items() if v is not None}


def _fix(app: Any, action: str, body: Any = None, **overrides: str | None) -> tuple[int, Any]:
    data = json.dumps(body if body is not None else {}).encode()
    reply = app.get(f"/api/setup/fix/{action}", "POST", data, _headers(app, **overrides))
    return reply.status, json.loads(reply.body)


def test_checks_endpoint(setup_app: tuple[Any, FixRecorder]) -> None:
    app, _ = setup_app
    status, body = app.json("/api/setup/checks")
    assert status == 200 and body["writable"] is True
    by_id = {c["id"]: c for c in body["checks"]}
    assert by_id["config-root"]["status"] == "problem"
    assert by_id["config-root"]["fix_action"] == "init"
    assert by_id["config-root"]["fix_label"] == "Create configuration"


def test_fix_runs_and_rechecks(setup_app: tuple[Any, FixRecorder]) -> None:
    app, recorder = setup_app
    status, body = _fix(app, "init", {"team": 254})
    assert status == 200 and body["ok"] is True and body["message"] == "init done"
    assert recorder.calls == [("init", {"team": 254})]
    assert {c["id"] for c in body["checks"]} >= {"config-root", "team"}  # re-run after the fix


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"X-Flashpoint": None}, "X-Flashpoint"),
        ({"X-Flashpoint": "triage"}, "X-Flashpoint"),
        ({"Origin": "http://evil.example"}, "cross-origin"),
        ({"Host": "rebound.example:8000"}, "Host"),
        ({"Content-Type": "text/plain"}, "Content-Type"),
    ],
)
def test_request_without_the_guard(
    setup_app: tuple[Any, FixRecorder], override: dict[str, str | None], reason: str
) -> None:
    app, recorder = setup_app
    status, body = _fix(app, "owlet", **override)
    assert status == 403 and body["ok"] is False and reason in body["reason"]
    assert recorder.calls == []


def test_unknown_action_runs_nothing(setup_app: tuple[Any, FixRecorder]) -> None:
    app, recorder = setup_app
    status, body = _fix(app, "rm-rf")
    assert status == 404 and body["ok"] is False
    status, _ = _fix(app, "..%2Finit")
    assert status == 404 and recorder.calls == []


def test_shared_on_the_network(
    setup_app: tuple[Any, FixRecorder], monkeypatch: pytest.MonkeyPatch
) -> None:
    app, recorder = setup_app
    monkeypatch.setattr(FlashpointServer, "bound_local", property(lambda self: False))
    assert app.json("/api/setup/checks")[1]["writable"] is False
    status, body = _fix(app, "owlet")
    assert status == 403 and body["ok"] is False
    assert recorder.calls == []


def test_refused_from_another_machine() -> None:
    headers = {
        "Host": "127.0.0.1:8000",
        "Origin": "http://127.0.0.1:8000",
        "X-Flashpoint": "setup",
        "Content-Type": "application/json",
    }
    assert refusal(True, 8000, "127.0.0.1", headers, "setup") is None
    assert refusal(True, 8000, "10.68.29.5", headers, "setup") is not None
    assert refusal(True, 8000, "127.0.0.1", headers, "launch") is not None


def test_setup_body_limit(setup_app: tuple[Any, FixRecorder]) -> None:
    app, recorder = setup_app
    status, _ = _fix(app, "init", {"pad": "x" * 8000})  # over the 4 KB launch limit: fine
    assert status == 200
    status, body = _fix(app, "init", {"pad": "x" * (MAX_SETUP_BODY + 1)})
    assert status == 413 and body["ok"] is False
    assert len(recorder.calls) == 1


def test_launch_keeps_its_4_kb_limit(setup_app: tuple[Any, FixRecorder]) -> None:
    app, _ = setup_app
    data = json.dumps({"match_key": "x" * 8000}).encode()
    reply = app.get("/api/launch", "POST", data, _headers(app, **{"X-Flashpoint": "launch"}))
    assert reply.status == 413


def test_check_results_reach_the_view_unchanged(setup_app: tuple[Any, FixRecorder]) -> None:
    app, _ = setup_app
    server: FlashpointServer = app.server
    server.check_context = lambda: _Stub()  # type: ignore[assignment, return-value]
    body = app.json("/api/setup/checks")[1]
    assert body["checks"] == [CheckResult("x", "X", "warning", "m", command="c").to_dict()]


class _Stub:
    pass


@pytest.fixture(autouse=True)
def _stub_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    from flashpoint.setup import checks

    real = checks.run_checks

    def run(ctx: Any) -> list[CheckResult]:
        if isinstance(ctx, _Stub):
            return [CheckResult("x", "X", "warning", "m", command="c")]
        return real(ctx)

    monkeypatch.setattr(checks, "run_checks", run)


def test_download_owlet_from_the_view(serve: Any, lake: LakePaths, tmp_path: Path) -> None:
    """The real allow-list, a fake registry: the fix downloads, and the re-run check is ok."""
    from tests.setup.test_fixes import fake_registry

    app = serve(lake)
    server: FlashpointServer = app.server
    server.check_context = lambda: CheckContext(
        tmp_path / "cfg",
        lake,
        tmp_path / "cache",
        system="Darwin",
        season=2026,
        registry=fake_registry(tmp_path),
    )
    before = {c["id"]: c for c in app.json("/api/setup/checks")[1]["checks"]}
    assert before["owlet:C19"]["fix_action"] == "owlet"
    status, body = _fix(app, "owlet")
    assert status == 200 and body["ok"] and body["changed"] == ["owlet 26.0.0-fake (C19)"]
    after = {c["id"]: c for c in body["checks"]}
    assert after["owlet:C19"]["status"] == "ok"
