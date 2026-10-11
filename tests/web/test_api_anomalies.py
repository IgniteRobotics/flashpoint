"""Anomalies API: queue, detail, badge (spec: anomaly-review), and guarded triage writes."""

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from flashpoint.anomaly import detect
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from flashpoint.views.anomalies import NO_CAUSES, AnomalyQueries
from flashpoint.web.server import FlashpointServer
from tests.web.anomaly_lake import build_anomaly_lake, ids


@pytest.fixture
def built(tmp_path: Path) -> tuple[LakePaths, Path]:
    return build_anomaly_lake(tmp_path)


class Spawned:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> None:
        self.calls.append(argv)


@pytest.fixture
def spawned() -> Spawned:
    return Spawned()


@pytest.fixture
def app(serve: Any, built: tuple[LakePaths, Path], spawned: Spawned) -> Any:
    lake, config_dir = built
    return serve(lake, anomalies=AnomalyQueries(lake, config_dir), spawner=spawned)


def _headers(app: Any, **overrides: str | None) -> dict[str, str]:
    host = app.base.removeprefix("http://")
    headers: dict[str, str | None] = {
        "Host": host, "Origin": f"http://{host}", "X-Flashpoint": "triage",
        "Content-Type": "application/json", **overrides,
    }  # fmt: skip
    return {k: v for k, v in headers.items() if v is not None}


def _triage(app: Any, anomaly_id: str, body: Any, **overrides: str | None) -> tuple[int, Any]:
    data = body if isinstance(body, bytes) else json.dumps(body).encode()
    reply = app.get(f"/api/anomalies/{anomaly_id}/triage", "POST", data,
                    _headers(app, **overrides))  # fmt: skip
    return reply.status, json.loads(reply.body) if reply.body.startswith(b"{") else reply.body


# --- reads ----------------------------------------------------------------------------------


def test_queue_defaults(app: Any, built: tuple[LakePaths, Path]) -> None:
    status, body = app.json("/api/anomalies")
    assert status == 200 and (body["tab"], body["sensitivity"]) == ("open", "normal")
    found = ids(built[0])
    assert [i["id"] for i in body["items"]] == [found["step"], found["spec"]]  # HIGH first
    assert body["counts"] == {"open": 2, "resolved": 0, "dismissed": 0}
    assert body["high"] == 1 and body["scanned_through"] == "2026syn_qm25"
    assert body["building_baseline"] == 1 and body["restart_pending"] is False
    assert body["writable"] is True
    item = body["items"][0]
    assert (item["state"], item["serial"], item["detected"]) == ("new", "HOOD", True)
    assert item["flagged_keys"] == ["2026syn_qm21", "2026syn_qm22", "2026syn_qm23",
                                    "2026syn_qm24"][: len(item["flagged_keys"])]  # fmt: skip


def test_sensitivity(app: Any) -> None:
    def opened(sens: str) -> list[str]:
        return [i["severity"] for i in app.json(f"/api/anomalies?sens={sens}")[1]["items"]]

    assert opened("normal") == ["HIGH", "MED"]
    assert opened("strict") == ["HIGH", "MED", "LOW"]
    assert opened("relaxed") == ["HIGH", "MED"]
    assert app.json("/api/anomalies?sens=loud")[0] == 400
    assert app.json("/api/anomalies?tab=all")[0] == 400
    assert app.json("/api/anomalies?colour=red")[0] == 400


def test_unit_and_match_filters(app: Any, built: tuple[LakePaths, Path]) -> None:
    found = ids(built[0])
    _, by_unit = app.json("/api/anomalies?unit=ctre:HOOD")
    assert [i["id"] for i in by_unit["items"]] == [found["step"]]
    _, by_match = app.json("/api/anomalies?match=2026syn_qm10")
    assert [i["id"] for i in by_match["items"]] == [found["spec"]]
    _, none = app.json("/api/anomalies?match=2026syn_qm1")
    assert none["items"] == [] and none["counts"]["open"] == 0


def test_detail(app: Any, built: tuple[LakePaths, Path]) -> None:
    status, body = app.json(f"/api/anomalies/{ids(built[0])['step']}")
    assert status == 200
    assert body["method"] == "STEP" and body["why"].startswith("STEP:")
    assert body["evidence"]["points"][-1]["flagged"] is True
    assert body["causes"]["items"] and body["causes"]["note"] is None
    assert body["history"] == [] and body["actions"] == ["investigating", "dismissed"]
    assert body["seen_before"] == [] and body["writable"] is True
    assert app.json("/api/anomalies/0000000000000000")[0] == 404


def test_causes_resolve_at_read_time(app: Any, built: tuple[LakePaths, Path]) -> None:
    lake, config_dir = built
    drift = ids(lake)["drift"]
    assert app.json(f"/api/anomalies/{drift}")[1]["causes"] == {"items": [], "note": NO_CAUSES}
    with (config_dir / "anomaly-causes.toml").open("a") as f:
        f.write('\n[[cause]]\nmethod = "DRIFT"\nmetric = "temp_mean_c"\ntext = "Clogged vent"\n')
    assert app.json(f"/api/anomalies/{drift}")[1]["causes"]["items"] == ["Clogged vent"]


def test_badge(app: Any, built: tuple[LakePaths, Path]) -> None:
    assert app.json("/api/anomalies/summary") == (
        200, {"open": 2, "high": 1, "restart_pending": False},
    )  # fmt: skip
    _triage(app, ids(built[0])["step"], {"to": "dismissed", "outcome": "expected heat"})
    assert app.json("/api/anomalies/summary")[1] == {
        "open": 1, "high": 0, "restart_pending": False,
    }  # fmt: skip


def test_reads_are_fast_on_a_season(tmp_path: Path) -> None:
    lake, config_dir = build_anomaly_lake(tmp_path, matches=60, slots=None)
    queries = AnomalyQueries(lake, config_dir)
    anomaly_id = ids(lake)["step"]
    calls: list[Callable[[], Any]] = [queries.queue, lambda: queries.detail(anomaly_id),
                                      queries.badge]  # fmt: skip
    for call in calls:
        call()  # the first read loads the table
        started = time.perf_counter()
        call()
        assert time.perf_counter() - started < 0.5


# --- triage ---------------------------------------------------------------------------------


def test_investigate_then_resolve(app: Any, built: tuple[LakePaths, Path]) -> None:
    anomaly_id = ids(built[0])["step"]
    assert _triage(app, anomaly_id, {"to": "investigating"})[0] == 200
    status, body = _triage(app, anomaly_id, {"to": "resolved",
                                             "outcome": "wheel bearing replaced"})  # fmt: skip
    assert status == 200 and body["saved"] is True and body["state"] == "resolved"
    _, resolved = app.json("/api/anomalies?tab=resolved")
    (item,) = resolved["items"]
    assert (item["id"], item["outcome"]) == (anomaly_id, "wheel bearing replaced")
    history = app.json(f"/api/anomalies/{anomaly_id}")[1]["history"]
    assert [(h["from_state"], h["to_state"]) for h in history] == [
        ("new", "investigating"), ("investigating", "resolved"),
    ]  # fmt: skip
    assert app.json(f"/api/anomalies/{anomaly_id}")[1]["actions"] == ["new"]  # REOPEN


@pytest.mark.parametrize(
    ("body", "status", "reason"),
    [
        ({"to": "resolved", "outcome": ""}, 400, "outcome is required"),
        ({"to": "dismissed", "outcome": "   "}, 400, "outcome is required"),
        ({"to": "dismissed", "outcome": "x" * 501}, 400, "500"),
        ({"to": "resolved", "outcome": "skipped a step"}, 409, "cannot go from new to resolved"),
        ({"to": "investigating", "restart_baseline": True}, 400, "only when dismissing"),
        ({"to": "gone"}, 400, "unknown state"),
        ({"to": "dismissed", "outcome": 5}, 400, "text"),
        ({"to": "investigating", "colour": "red"}, 400, "expected"),
        (["investigating"], 400, "expected"),
    ],
)
def test_refused_changes_write_nothing(
    app: Any, built: tuple[LakePaths, Path], body: Any, status: int, reason: str
) -> None:
    lake = built[0]
    got, reply = _triage(app, ids(lake)["step"], body)
    assert got == status and reply["saved"] is False and reason in reply["reason"]
    assert _rows(lake) == []


def _rows(lake: LakePaths) -> list[dict[str, Any]]:
    if not lake.ledger.is_file():
        return []
    ledger = Ledger(lake.ledger)
    try:
        return ledger.triage()
    finally:
        ledger.close()


def test_unknown_id(app: Any) -> None:
    assert _triage(app, "0" * 16, {"to": "investigating"})[0] == 404
    assert app.get("/api/anomalies/xyz/triage", "POST", b"{}", _headers(app)).status == 405


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"Origin": "http://evil.example"}, "cross-origin"),
        ({"Origin": None}, "cross-origin"),
        ({"X-Flashpoint": "launch"}, "X-Flashpoint"),
        ({"Host": "rebound.example:8000"}, "Host"),
        ({"Content-Type": "text/plain"}, "Content-Type"),
    ],
)
def test_guard(app: Any, built: tuple[LakePaths, Path], override: dict[str, str | None],
               reason: str) -> None:  # fmt: skip
    status, body = _triage(app, ids(built[0])["step"], {"to": "investigating"}, **override)
    assert status == 403 and reason in body["reason"]
    assert _rows(built[0]) == []


def test_bodies(app: Any, built: tuple[LakePaths, Path]) -> None:
    anomaly_id = ids(built[0])["step"]
    assert _triage(app, anomaly_id, b"{not json")[0] == 400
    big = json.dumps({"to": "investigating", "outcome": "x" * 5000}).encode()
    assert _triage(app, anomaly_id, big)[0] == 413
    assert _rows(built[0]) == []


def test_shared_server_is_read_only(
    serve: Any, built: tuple[LakePaths, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    lake, config_dir = built
    monkeypatch.setattr(FlashpointServer, "bound_local", property(lambda self: False))
    shared = serve(lake, anomalies=AnomalyQueries(lake, config_dir))
    status, body = _triage(shared, ids(lake)["step"], {"to": "investigating"})
    assert status == 403 and "network" in body["reason"]
    assert shared.json("/api/anomalies")[1]["writable"] is False
    assert _rows(lake) == []


def test_no_longer_detected(app: Any, built: tuple[LakePaths, Path]) -> None:
    lake, config_dir = built
    drift = ids(lake)["drift"]
    _triage(app, drift, {"to": "dismissed", "outcome": "vent cleaned"})
    (config_dir / "anomaly.toml").write_text(
        '[baseline]\nmetrics = ["temp_max_c", "stator_current_p95"]\n'
    )
    detect.detect(lake, config_dir)
    _, dismissed = app.json("/api/anomalies?tab=dismissed")
    (item,) = dismissed["items"]
    assert item["id"] == drift and item["detected"] is False
    assert item["outcome"] == "vent cleaned" and "No longer detected" in item["title"]
    assert _triage(app, drift, {"to": "new"})[0] == 200  # still triageable


def test_seen_before(serve: Any, tmp_path: Path) -> None:
    from tests.anomaly.gold_builder import GoldLake, steady

    drift = steady(40.0, 15) + [40.0 + 0.6 * k for k in range(1, 16)]
    gold = GoldLake(tmp_path / "lake", matches=60, slots=["drive-fl"])
    gold.set("drive-fl", "stator_current_p95", drift + drift)
    gold.swap("drive-fl", 30, "ctre:DRIVE-FL-2")
    lake = gold.write()
    from tests.web.anomaly_lake import anomaly_config

    config_dir = anomaly_config(tmp_path)
    detect.detect(lake, config_dir)
    rows = [r for r in detect.read_anomalies(lake).to_dicts() if r["method"] == "DRIFT"]
    first = next(r for r in rows if r["subject"] == "ctre:DRIVE-FL")
    second = next(r for r in rows if r["subject"] == "ctre:DRIVE-FL-2")
    app = serve(lake, anomalies=AnomalyQueries(lake, config_dir))
    _triage(app, first["id"], {"to": "investigating"})
    _triage(app, first["id"], {"to": "resolved", "outcome": "wheel bearing replaced"})
    seen = app.json(f"/api/anomalies/{second['id']}")[1]["seen_before"]
    assert [(s["id"], s["outcome"]) for s in seen] == [(first["id"], "wheel bearing replaced")]


def test_restart_baseline_spawns_detect(
    app: Any, built: tuple[LakePaths, Path], spawned: Spawned
) -> None:
    lake, config_dir = built
    step = ids(lake)["step"]
    status, body = _triage(app, step, {"to": "dismissed", "outcome": "new heat sink",
                                       "restart_baseline": True})  # fmt: skip
    assert status == 200 and body["restart_baseline"] is True
    (argv,) = spawned.calls
    assert argv[-4:] == ["flashpoint", "detect", "--lake", str(lake.root)]
    assert app.json("/api/anomalies")[1]["restart_pending"] is True
    assert app.json("/api/anomalies/summary")[1]["restart_pending"] is True
    detect.detect(lake, config_dir)  # what the spawned process does
    assert app.json("/api/anomalies")[1]["restart_pending"] is False
    rows = detect.read_anomalies(lake).to_dicts()
    hood = [r for r in rows if r["subject"] == "ctre:HOOD" and r["metric"] == "temp_max_c"
            and r["method"] in ("STEP", "DRIFT")]  # fmt: skip
    assert hood == []  # the new level is the baseline now


def test_dismissed_episode_stays_dismissed(serve: Any, tmp_path: Path) -> None:
    from tests.anomaly.gold_builder import GoldLake
    from tests.web.anomaly_lake import anomaly_config

    temps = [40.0] * 12
    temps[9] = 66.0
    gold = GoldLake(tmp_path / "lake", matches=12, slots=["drive-fl"])
    gold.set("drive-fl", "temp_max_c", temps)
    lake, config_dir = gold.write(), anomaly_config(tmp_path)
    detect.detect(lake, config_dir)
    (spec,) = [r["id"] for r in detect.read_anomalies(lake).to_dicts() if r["method"] == "SPEC"]
    app = serve(lake, anomalies=AnomalyQueries(lake, config_dir))
    _triage(app, spec, {"to": "dismissed", "outcome": "hot day"})
    assert app.json(f"/api/anomalies/{spec}")[1]["matches_affected"] == 1

    temps[10] = 66.0  # flagged again in the next match: the same episode
    gold.set("drive-fl", "temp_max_c", temps)
    gold.write()
    detect.detect(lake, config_dir)
    after = app.json(f"/api/anomalies/{spec}")[1]
    assert (after["state"], after["matches_affected"], after["detected"]) == ("dismissed", 2, True)


def test_triage_is_the_only_write(serve: Any, tmp_path: Path) -> None:
    """Every History and Anomalies query, then one change to investigating: logs and derived
    layers are byte-identical, and the only new metadata record is that triage row."""
    import hashlib
    from urllib.parse import quote

    from tests.views.season import build_season, match_key
    from tests.web.anomaly_lake import anomaly_config

    season = build_season(tmp_path / "lake")
    config_dir = anomaly_config(tmp_path)
    detect.detect(season.lake, config_dir)

    def digest() -> str:
        h = hashlib.sha256()
        for part in ("raw", "bronze", "silver", "gold"):
            for path in sorted(p for p in (season.lake.root / part).rglob("*") if p.is_file()):
                h.update(str(path.relative_to(season.lake.root)).encode() + path.read_bytes())
        return h.hexdigest()

    before = digest()
    app = serve(season.lake, robots=season.robots,
                anomalies=AnomalyQueries(season.lake, config_dir))  # fmt: skip
    _, queue = app.json("/api/anomalies?sens=strict")
    anomaly_id = queue["items"][0]["id"]
    for path in (
        "/api/filters", "/api/summary", "/api/trend?by=slot&metric=min_voltage",
        "/api/devices?metric=current_p95&phase=auto", "/api/unit/ctre:MOVER", "/api/unit/nope",
        f"/api/match/{match_key(0)}", "/api/trend?unit=" + quote("'; DROP TABLE x; --"),
        "/api/anomalies", "/api/anomalies?tab=resolved&sens=relaxed", "/api/anomalies/summary",
        f"/api/anomalies/{anomaly_id}", "/api/anomalies?unit=" + quote("x' OR 1=1 --"),
    ):  # fmt: skip
        assert app.get(path).status == 200, path
    assert _triage(app, anomaly_id, {"to": "investigating"})[0] == 200
    app.server.queries.close()
    assert digest() == before
    rows = _rows(season.lake)
    assert [(r["anomaly_id"], r["to_state"]) for r in rows] == [(anomaly_id, "investigating")]
    ledger = Ledger(season.lake.ledger)
    try:
        tables = [r["name"] for r in ledger.query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name != 'anomaly_triage'"
        )]  # fmt: skip
        assert all(ledger.query(f"SELECT count(*) AS n FROM {t}")[0]["n"] == 0 for t in tables)  # noqa: S608
    finally:
        ledger.close()
