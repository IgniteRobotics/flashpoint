import json
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest

from flashpoint import config
from flashpoint.lake.paths import LakePaths
from flashpoint.report.build import ReportBuilder
from flashpoint.report.paths import data_dir
from tests.report.lake import MatchLake, quiet_rows
from tests.report.silver import points
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


@pytest.fixture
def built_q7(tmp_path: Path) -> MatchLake:
    """Synthetic Q7 (match start at lake T=10 s) with a single 150 A hood sample at T+97.001 s."""
    lake = MatchLake(tmp_path / "lake")
    lake.add_match(
        "2026gacmp_qm7", "q7", quiet_rows() + points("hood", "supply_current", [(107.001, 150.0)])
    )
    builder = ReportBuilder(lake.write_meta(), config.config_root())
    try:
        builder.build()
    finally:
        builder.close()
    return lake


def _stored(lake: MatchLake) -> Any:
    text = (data_dir(lake.lake) / "2026gacmp_qm7.js").read_text(encoding="utf-8")
    return json.loads(text[len("FP.register(") : -3])


def test_envelope_window(serve: Any, built_q7: MatchLake) -> None:
    app = serve(built_q7.lake)
    status, body = app.json("/api/envelope/2026gacmp_qm7?from=95&to=99")
    assert status == 200 and body["available"] is True
    assert set(body) == {"available", "window", "series", "battery", "temps", "not_logged"}
    stored = _stored(built_q7)
    assert body["window"] == {"t0": 95.0, "width": 0.01, "n": 400}
    assert {s: set(m) for s, m in body["series"].items()} == {
        s: set(m) for s, m in stored["series"].items()
    }
    assert (
        set(body["battery"]) == {"min", "max", "mean"}
        and body["not_logged"] == stored["not_logged"]
    )
    hood = body["series"]["hood"]["supply_current"]
    assert hood["max"][200] == 150.0 and max(v for v in hood["max"] if v is not None) == 150.0
    assert body["temps"]["intake-roller"] == [[95.0, 45.0]]  # last reading carried in


def test_envelope_window_clamped_to_match(serve: Any, built_q7: MatchLake) -> None:
    app = serve(built_q7.lake)
    status, body = app.json("/api/envelope/2026gacmp_qm7?from=-50&to=3")
    assert status == 200 and body["window"]["t0"] == -2.0
    assert body["window"]["t0"] + body["window"]["n"] * body["window"]["width"] >= 3.0


@pytest.mark.parametrize(
    "query",
    [
        "from=99&to=95",
        "from=95&to=95",
        "from=500&to=600",
        "from=-90&to=-50",
        "from=nan&to=99",
        "from=95&to=inf",
        "from=x&to=99",
        "from=95",
        "from=95&to=99&step=1",
        "from=95&to=99&to=98",
    ],
)
def test_envelope_bad_window(serve: Any, built_q7: MatchLake, query: str) -> None:
    status, body = serve(built_q7.lake).json(f"/api/envelope/2026gacmp_qm7?{query}")
    assert status == 400 and "\n" not in body["error"] and set(body) == {"error"}


@pytest.mark.parametrize(
    "key", ["2026gacmp_qm8", "", "..%2F..%2Fsite-state", "..%2Fdata%2F2026gacmp_qm7", "matches"]
)
def test_envelope_unbuilt_or_hostile_key(serve: Any, built_q7: MatchLake, key: str) -> None:
    status, body = serve(built_q7.lake).json(f"/api/envelope/{key}?from=95&to=99")
    assert status == 404 and set(body) == {"error"}


def test_envelope_silver_missing(serve: Any, built_q7: MatchLake) -> None:
    shutil.rmtree(built_q7.lake.root / "silver")
    status, body = serve(built_q7.lake).json("/api/envelope/2026gacmp_qm7?from=95&to=99")
    assert status == 200 and body["available"] is False and body["reason"]
    assert "series" not in body
