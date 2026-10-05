from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest

from flashpoint.lake.paths import LakePaths
from tests.views.season import SeasonLake, build_season, match_key
from tests.views.test_queries import _lake_digest


@pytest.fixture(scope="module")
def season(tmp_path_factory: pytest.TempPathFactory) -> SeasonLake:
    return build_season(tmp_path_factory.mktemp("season") / "lake", hostile=True)


@pytest.fixture
def app(serve: Any, season: SeasonLake) -> Any:
    return serve(season.lake, robots=season.robots)


def test_filters(app: Any, season: SeasonLake) -> None:
    status, body = app.json("/api/filters")
    assert status == 200
    assert body["lake"] == str(season.lake.root) and body["empty"] is False
    assert body["values"]["event"] == ["2026gacmp", "2026gadal"]


def test_summary(app: Any) -> None:
    status, body = app.json("/api/summary?event=2026gadal")
    assert status == 200 and body["tiles"]["matches"] == 30


def test_trend(app: Any) -> None:
    status, body = app.json("/api/trend?metric=peak_temp&by=unit&subsystem=intake")
    assert status == 200
    assert set(body) == {"metric", "label", "unit", "reference", "by", "matches", "points"}
    point = body["points"][0]
    assert set(point) >= {"series", "match_key", "t", "value", "alignment", "low_alignment"}
    assert {p["series"] for p in body["points"]} >= {"ctre:ROLLER-A", "ctre:ROLLER-B"}


def test_trend_defaults(app: Any) -> None:
    status, body = app.json("/api/trend")
    assert status == 200 and body["metric"] == "peak_temp" and body["by"] == "unit"


def test_devices(app: Any) -> None:
    status, body = app.json("/api/devices?metric=supply_wh&slot=hood")
    assert status == 200 and body["metric"] == "supply_wh"
    rows = {r["unit_id"]: r for r in body["rows"]}
    assert set(rows) == {"ctre:HOOD-G", "ctre:HOOD-T"}
    assert rows["ctre:HOOD-G"]["role"] == "<img src=x onerror=alert(1)>"  # data, not markup
    assert set(rows["ctre:HOOD-G"]) >= {"serial", "first_t", "last_t", "latest", "change",
                                        "spark", "health"}  # fmt: skip


def test_unit(app: Any) -> None:
    status, body = app.json("/api/unit/" + quote("ctre:MOVER", safe=""))
    assert status == 200 and body["found"] is True and body["serial"] == "MOVER"
    assert body["lifeline"][-1]["kind"] == "first-seen"
    status, body = app.json("/api/unit/" + quote("ctre:MOVER", safe="") + "?robot=2026-comp")
    assert body["totals"]["sessions"] == 60
    status, body = app.json("/api/unit/ctre:ROLLER-Z")
    assert status == 200 and body["found"] is False and "ctre:ROLLER-A" in body["suggestions"]


def test_match_not_built(app: Any) -> None:
    status, body = app.json(f"/api/match/{match_key(3)}")
    assert status == 200 and body["built"] is False
    assert body["build_command"] == f"flashpoint report --match {match_key(3)}"
    assert body["sources"] and body["sources"][0]["part"] == "wpilog"


@pytest.mark.parametrize(
    "path",
    [
        "/api/trend?metric=peak_temp&colour=red",
        "/api/trend?metric=peak_temp&metric=stall_s",
        "/api/trend?metric=temp_max_c",
        "/api/trend?by=robot",
        "/api/trend?phase=gap",
        "/api/devices?metric=x",
        "/api/summary?sort=1",
        "/api/filters?x=1",
        "/api/unit/ctre:A?phase=match",
        "/api/match/2026gacmp_qm7?x=1",
    ],
)
def test_bad_requests(app: Any, path: str) -> None:
    status, body = app.json(path)
    assert status == 400 and "error" in body


@pytest.mark.parametrize("path", ["/api/nope", "/api/unit/", "/api/match/", "/api/filters/x"])
def test_unknown_endpoints(app: Any, path: str) -> None:
    status, body = app.json(path)
    assert status == 404 and "error" in body


def test_hostile_parameter(app: Any) -> None:
    status, body = app.json("/api/trend?event=" + quote("x' OR 1=1 --"))
    assert status == 200 and body["points"] == []


def test_empty_lake(serve: Any, tmp_path: Path) -> None:
    lake = LakePaths(tmp_path / "empty")
    app = serve(lake)
    status, body = app.json("/api/summary")
    assert status == 200 and body["empty"] is True and body["lake"] == str(lake.root)
    status, body = app.json("/api/trend")
    assert status == 200 and body["points"] == []


def test_lake_unchanged_by_every_endpoint(serve: Any, tmp_path: Path) -> None:
    season = build_season(tmp_path / "lake")
    before = _lake_digest(season.lake)
    app = serve(season.lake, robots=season.robots)
    for path in (
        "/api/filters",
        "/api/summary",
        "/api/trend?by=slot&metric=min_voltage",
        "/api/devices?metric=current_p95&phase=auto",
        "/api/unit/ctre:MOVER",
        "/api/unit/nope",
        f"/api/match/{match_key(0)}",
        "/api/trend?unit=" + quote("'; DROP TABLE x; --"),
    ):
        assert app.get(path).status == 200, path
    app.server.queries.close()
    assert _lake_digest(season.lake) == before
