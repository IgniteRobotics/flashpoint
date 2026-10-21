import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.web.server import FlashpointServer
from tests.web.test_setup_api import _headers

ROBOT = "robots/2026-comp.toml"


@pytest.fixture
def root(tmp_path: Path) -> Path:
    target = tmp_path / "cfg"
    shutil.copytree(config.config_root(), target)
    return target


@pytest.fixture
def lake(tmp_path: Path) -> LakePaths:
    lake = LakePaths(tmp_path / "lake")
    lake.meta.mkdir(parents=True)
    (lake.meta / "sessions.parquet").write_bytes(b"stand-in")
    return lake


@pytest.fixture
def app(serve: Any, lake: LakePaths, root: Path) -> Any:
    running = serve(lake)
    server: FlashpointServer = running.server
    server.config_dir = root
    return running


def _put(app: Any, section: str, body: Any, **overrides: str | None) -> tuple[int, Any]:
    data = json.dumps(body).encode()
    reply = app.get(f"/api/config/{section}", "PUT", data, _headers(app, **overrides))
    return reply.status, json.loads(reply.body)


def _tree_hash(path: Path) -> str:
    digest = hashlib.sha256()
    for p in sorted(path.rglob("*")):
        digest.update(str(p.relative_to(path)).encode())
        if p.is_file():
            digest.update(p.read_bytes())
    return digest.hexdigest()


def test_ignite_configuration(app: Any, root: Path) -> None:
    status, body = app.json("/api/config")
    assert status == 200 and body["writable"] is True and body["config_root"] == str(root)
    sections = body["sections"]
    robot = next(r for r in sections["robots"] if r["id"] == "2026-comp")
    assert robot["source"] == ROBOT and robot["error"] is None and robot["default"] is False
    assert len(robot["data"]["slot"]) == 23
    assert [b["name"] for b in robot["data"]["bus"]] == ["rio", "DriveTrain"]
    assert len(robot["data"]["signal"]) == 52
    team = sections["team"]
    assert team["source"] == "acquire.toml" and team["values"]["team"] == 6829
    assert team["hosts"][0] == "10.68.29.2"
    thresholds = {t["name"]: t for t in sections["thresholds"]}
    assert thresholds["report.toml"]["default"] is True
    assert thresholds["report.toml"]["data"]["thresholds"]["temp_warn_c"] == 65.0
    assert sections["units"]["columns"][0] == "serial"
    assert all(len(s["sha256"]) == 64 for s in [team, robot, *thresholds.values()])


def test_broken_file(app: Any, root: Path) -> None:
    path = root / ROBOT
    path.write_text(
        path.read_text().replace('project = "Robot-2026"', 'project = "Robot-2026"\nbogus_key = 1')
    )
    body = app.json("/api/config")[1]
    robot = next(r for r in body["sections"]["robots"] if r["id"] == "2026-comp")
    assert "2026-comp.toml" in robot["error"] and "bogus_key" in robot["error"]
    assert body["sections"]["team"]["values"]["team"] == 6829
    assert any(r["id"] == "2025-comp" and r["error"] is None for r in body["sections"]["robots"])


def test_unparsable_file_still_lists(app: Any, root: Path) -> None:
    (root / "report.toml").write_text("not = [toml")
    body = app.json("/api/config")[1]
    report = next(t for t in body["sections"]["thresholds"] if t["name"] == "report.toml")
    assert report["error"] and report["data"] is None and report["default"] is False


def test_save_each_section(app: Any, root: Path) -> None:
    sections = app.json("/api/config")[1]["sections"]
    status, body = _put(
        app, "team", {"sha256": sections["team"]["sha256"], "values": {"team": 254}}
    )
    assert status == 200 and body["saved"] and "restart" in body["affects"]

    robot = next(r for r in sections["robots"] if r["id"] == "2026-comp")
    data = robot["data"]
    data["slot"] = [dict(s, role="FL drive") if s["id"] == "drive-fl" else s for s in data["slot"]]
    status, body = _put(app, "robots/2026-comp", {"sha256": robot["sha256"], "robot": data})
    assert status == 200 and body["affects"] == "flashpoint derive"
    assert 'role = "FL drive"' in (root / ROBOT).read_text()

    report = next(t for t in sections["thresholds"] if t["name"] == "report.toml")
    status, body = _put(
        app,
        "thresholds/report.toml",
        {"sha256": report["sha256"], "tables": {"thresholds": {"temp_warn_c": 60.0}}},
    )
    assert status == 200 and body["affects"] == "flashpoint report"

    rows = [{"serial": "AAAA", "label": "M-001"}]
    status, body = _put(app, "units", {"sha256": sections["units"]["sha256"], "rows": rows})
    assert status == 200 and body["saved"]


def test_duplicate_serial(app: Any, root: Path) -> None:
    units = app.json("/api/config")[1]["sections"]["units"]
    rows = [{"serial": "AAAA", "label": "M-001"}, {"serial": "AAAA", "label": "M-002"}]
    before = (root / "units" / "units-seed.csv").read_bytes()
    status, body = _put(app, "units", {"sha256": units["sha256"], "rows": rows})
    assert status == 400 and body["saved"] is False and "rows 1 and 2" in body["reason"]
    rows[1]["serial"] = "BBBB"
    rows[1]["label"] = "M-001"
    status, body = _put(app, "units", {"sha256": units["sha256"], "rows": rows})
    assert status == 400 and "label" in body["reason"] and "rows 1 and 2" in body["reason"]
    assert (root / "units" / "units-seed.csv").read_bytes() == before


def test_changed_on_disk_is_409(app: Any, root: Path) -> None:
    team = app.json("/api/config")[1]["sections"]["team"]
    (root / "acquire.toml").write_text("team = 1\n")
    status, body = _put(app, "team", {"sha256": team["sha256"], "values": {"team": 254}})
    assert status == 409 and "acquire.toml" in body["reason"]


def test_new_robot(app: Any, root: Path) -> None:
    robot = next(
        r for r in app.json("/api/config")[1]["sections"]["robots"] if r["id"] == "2026-comp"
    )
    status, body = _put(app, "robots/2026-comp", {"robot": robot["data"], "new": True})
    assert status == 409 and "2026-comp" in body["reason"]
    data = robot["data"] | {"robot": "2026-practice"}
    status, body = _put(app, "robots/2026-practice", {"robot": data, "new": True})
    assert status == 200 and (root / "robots" / "2026-practice.toml").is_file()


@pytest.mark.parametrize(
    ("section", "status"),
    [("nope", 404), ("robots/../x", 400), ("thresholds/acquire.toml", 404), ("robots", 404)],
)
def test_bad_sections(app: Any, section: str, status: int) -> None:
    assert _put(app, section, {"sha256": None})[0] == status


@pytest.mark.parametrize("body", [[], "x", {"values": "x"}, {"robot": []}, {"rows": {}}])
def test_malformed_bodies(app: Any, body: Any) -> None:
    for section in ("team", "robots/2026-comp", "units"):
        assert _put(app, section, body)[0] == 400


def test_read_only_when_shared(app: Any, root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(FlashpointServer, "bound_local", property(lambda self: False))
    body = app.json("/api/config")[1]
    assert body["writable"] is False and body["sections"]["team"]["values"]["team"] == 6829
    before = (root / "acquire.toml").read_bytes()
    status, _ = _put(
        app, "team", {"sha256": body["sections"]["team"]["sha256"], "values": {"team": 254}}
    )
    assert status == 403 and (root / "acquire.toml").read_bytes() == before


def test_put_without_the_guard(app: Any, root: Path) -> None:
    team = app.json("/api/config")[1]["sections"]["team"]
    status, body = _put(
        app, "team", {"sha256": team["sha256"], "values": {"team": 254}}, **{"X-Flashpoint": None}
    )
    assert status == 403 and body["saved"] is False
    assert app.get("/api/launch", "PUT").status == 405  # PUT stays refused elsewhere


def test_saves_never_change_the_lake(app: Any, lake: LakePaths) -> None:
    """What an edit affects: saving never touches lake data."""
    before = _tree_hash(lake.root)
    test_save_each_section(app, app.server.config_dir)
    assert _tree_hash(lake.root) == before


def test_draft_routes(serve: Any, tmp_path: Path, root: Path) -> None:
    from tests.setup.test_draft import _synthetic

    lake = _synthetic(tmp_path)
    running = serve(lake)
    running.server.config_dir = root
    status, body = running.json("/api/setup/sessions")
    assert status == 200 and body["sessions"][0]["session_id"] == "s1"
    status, body = running.json("/api/setup/draft?session=s1")
    assert status == 200 and len(body["robot"]["slot"]) == 3
    assert running.json("/api/setup/draft?session=nope")[0] == 404
    assert running.json("/api/setup/draft")[0] == 400
    assert running.json("/api/setup/draft?session=s1&x=1")[0] == 400
