import hashlib
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from flashpoint.lake.paths import LakePaths
from flashpoint.views.queries import Filters, HistoryQueries, QueryError
from tests.views.season import HOSTILE_ROLE, MATCHES, SeasonLake, build_season, match_key


@pytest.fixture(scope="module")
def season(tmp_path_factory: pytest.TempPathFactory) -> SeasonLake:
    return build_season(tmp_path_factory.mktemp("season") / "lake")


@pytest.fixture
def queries(season: SeasonLake) -> HistoryQueries:
    return HistoryQueries(season.lake, season.robots)


def _lake_digest(lake: LakePaths) -> str:
    digest = hashlib.sha256()
    for path in sorted(p for p in lake.root.rglob("*") if p.is_file()):
        digest.update(str(path.relative_to(lake.root)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


# --- filters ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("filters", "column", "expected"),
    [
        (Filters(season="2026"), "season", "2026"),
        (Filters(robot="2026-comp"), "robot", "2026-comp"),
        (Filters(event="2026gacmp"), "event", "2026gacmp"),
        (Filters(match_type="e"), "match_type", "e"),
        (Filters(phase="auto"), "phase", "auto"),
        (Filters(subsystem="intake"), "subsystem", "intake"),
        (Filters(slot="hood"), "slot_id", "hood"),
        (Filters(unit="ctre:HOOD-T"), "unit_id", "ctre:HOOD-T"),
    ],
)
def test_each_filter_binds_and_selects(
    queries: HistoryQueries, filters: Filters, column: str, expected: str
) -> None:
    trend = queries.trend("peak_temp", "unit", filters)
    sql = queries.last_sql
    assert f".{column} = ?" in sql and f"'{expected}'" not in sql
    assert trend["points"]
    rows = queries._rows(
        f"SELECT DISTINCT {column} AS v FROM features f WHERE f.match_key IN"  # noqa: S608
        f" (SELECT unnest(?)) AND f.unit_id IN (SELECT unnest(?))",
        [
            [p["match_key"] for p in trend["points"]],
            [p["unit_id"] for p in trend["points"]],
        ],
    )
    if column in ("season", "robot", "event", "match_type"):
        assert {r["v"] for r in rows} == {expected}
    elif column == "slot_id":
        assert {p["slot_id"] for p in trend["points"]} == {expected}
    elif column == "unit_id":
        assert {p["unit_id"] for p in trend["points"]} == {expected}


def test_event_filter_returns_that_event_only(queries: HistoryQueries) -> None:
    trend = queries.trend("peak_temp", "unit", Filters(event="2026gacmp"))
    assert "f.event = ?" in queries.last_sql
    keys = {p["match_key"] for p in trend["points"]}
    assert keys == {match_key(i) for i in range(30, MATCHES)}
    assert len(trend["points"]) == 30 * 23


def test_phase_and_subsystem_counts(queries: HistoryQueries) -> None:
    trend = queries.trend("stall_s", "slot", Filters(phase="teleop", subsystem="intake"))
    assert {p["series"] for p in trend["points"]} == {
        "2026-comp/intake-extension",
        "2026-comp/intake-extension-follower",
        "2026-comp/intake-roller",
        "2026-comp/intake-roller-follower",
    }
    assert all(p["value"] == pytest.approx(1.8) for p in trend["points"])


def test_hostile_value_is_data(season: SeasonLake, queries: HistoryQueries) -> None:
    before = _lake_digest(season.lake)
    hostile = "x' OR 1=1 --"
    assert queries.trend("peak_temp", "unit", Filters(event=hostile))["points"] == []
    assert queries.devices("peak_temp", Filters(slot=hostile)) == []
    assert queries.summary(Filters(robot=hostile))["units"] == 0
    assert queries.unit(hostile)["found"] is False
    assert _lake_digest(season.lake) == before


@pytest.mark.parametrize("metric", ["temp_max_c", "peak_temp; DROP TABLE x", "", "flags"])
def test_metric_whitelist(queries: HistoryQueries, metric: str) -> None:
    with pytest.raises(QueryError):
        queries.trend(metric, "unit", Filters())
    with pytest.raises(QueryError):
        queries.devices(metric, Filters())


def test_phase_and_grouping_checked(queries: HistoryQueries) -> None:
    with pytest.raises(QueryError):
        queries.trend("peak_temp", "unit", Filters(phase="gap"))
    with pytest.raises(QueryError):
        queries.trend("peak_temp", "robot", Filters())


def test_filter_values(queries: HistoryQueries) -> None:
    values = queries.filter_values()
    assert values["season"] == ["2026"]
    assert values["event"] == ["2026gacmp", "2026gadal"]
    assert values["match_type"] == ["e", "qm"]
    assert values["phase"] == ["match", "auto", "teleop"]
    assert "ctre:MOVER" in values["unit"] and "intake" in values["subsystem"]
    assert [m["id"] for m in values["metric"]] == [
        "peak_temp", "min_voltage", "supply_wh", "stall_s", "current_p95",
    ]  # fmt: skip


# --- trends -------------------------------------------------------------------------------


def test_per_unit_line_follows_a_swap_and_gap(queries: HistoryQueries) -> None:
    trend = queries.trend("peak_temp", "unit", Filters())
    assert [m["match_key"] for m in trend["matches"]] == [match_key(i) for i in range(MATCHES)]
    hood_g = [p["match_key"] for p in trend["points"] if p["series"] == "ctre:HOOD-G"]
    assert hood_g == [match_key(i) for i in range(MATCHES) if not 10 <= i <= 20]
    hood_t = [p["match_key"] for p in trend["points"] if p["series"] == "ctre:HOOD-T"]
    assert hood_t == [match_key(i) for i in range(10, 21)]
    assert trend["reference"] == 65.0 and trend["unit"] == "°C"


def test_per_unit_line_marks_a_slot_change(tmp_path: Path) -> None:
    """A unit in slot A for its first matches and slot B afterwards is one line."""
    import polars as pl

    season = build_season(tmp_path / "lake")
    for i in range(30, MATCHES):  # move ctre:MOVER from drive-fr to drive-br for the 2nd event
        path = season.lake.root / f"gold/match_features/season=2026/session_id=s{i:03d}"
        frame = pl.read_parquet(path / "part-0.parquet")
        frame = frame.with_columns(
            pl.when(pl.col("unit_id") == "ctre:MOVER")
            .then(pl.lit("drive-br"))
            .otherwise(pl.col("slot_id"))
            .alias("slot_id")
        )
        frame.write_parquet(path / "part-0.parquet")
    trend = HistoryQueries(season.lake, season.robots).trend("peak_temp", "unit", Filters())
    mover = [p for p in trend["points"] if p["series"] == "ctre:MOVER"]
    assert len(mover) == MATCHES
    assert [p["match_key"] for p in mover if p["slot_changed"]] == [match_key(30)]
    assert {p["slot_id"] for p in mover} == {"drive-fr", "drive-br"}


def test_per_slot_lines(queries: HistoryQueries) -> None:
    trend = queries.trend("supply_wh", "slot", Filters(robot="2026-comp"))
    series = {p["series"] for p in trend["points"]}
    assert len(series) == 23
    hood = [p for p in trend["points"] if p["series"] == "2026-comp/hood"]
    assert len(hood) == MATCHES  # one point per match key, across both hood units
    assert {p["unit_id"] for p in hood} == {"ctre:HOOD-G", "ctre:HOOD-T"}


def test_low_alignment_point_is_flagged(queries: HistoryQueries) -> None:
    trend = queries.trend("peak_temp", "unit", Filters(unit="ctre:HOOD-G"))
    flagged = [p["match_key"] for p in trend["points"] if p["low_alignment"]]
    assert flagged == [match_key(7)]
    assert next(p for p in trend["points"] if p["low_alignment"])["alignment"] == "low"


def test_min_voltage_reference(queries: HistoryQueries) -> None:
    trend = queries.trend("min_voltage", "unit", Filters(slot="drive-fl"))
    assert trend["reference"] == 7.0 and trend["unit"] == "V"
    assert min(p["value"] for p in trend["points"]) < 9.0


def test_hostile_role_is_returned_as_data(tmp_path: Path) -> None:
    season = build_season(tmp_path / "lake", hostile=True)
    rows = HistoryQueries(season.lake, season.robots).devices("peak_temp", Filters(slot="hood"))
    assert {r["role"] for r in rows} == {HOSTILE_ROLE}


# --- devices and tiles --------------------------------------------------------------------


def test_device_table(queries: HistoryQueries) -> None:
    rows = {r["unit_id"]: r for r in queries.devices("peak_temp", Filters())}
    assert len(rows) == 23 + 3  # the swap's, gap's, and legacy unit's extra units
    roller_a = rows["ctre:ROLLER-A"]
    assert roller_a["serial"] == "ROLLER-A" and roller_a["slot_id"] == "intake-roller"
    assert roller_a["matches"] == 30
    assert roller_a["first_t"] < roller_a["last_t"] < rows["ctre:ROLLER-B"]["first_t"]
    assert len(roller_a["spark"]) == 30
    assert roller_a["latest"] == roller_a["spark"][-1]
    assert roller_a["change"] == pytest.approx(roller_a["spark"][-1] - roller_a["spark"][0])
    assert rows["ctre:FLYWHEEL-RIGHT"]["health"] == "WARN"
    assert rows["ctre:INDEXER-MAIN"]["health"] == "HOT"
    assert rows["ctre:DRIVE-FL"]["health"] == "OK"
    assert rows["legacy:2026-comp:steer-fl:0"]["serial"] == "legacy:2026-comp:steer-fl:0"


def test_health_from_temperature_counts_in_tiles(queries: HistoryQueries) -> None:
    tiles = queries.summary(Filters())
    assert tiles == {
        "units": 26,
        "matches": MATCHES,
        "powered_h": pytest.approx((MATCHES * 23 + 5) * 360 / 3600),
        "units_warn_hot": 2,
    }
    gadal = queries.summary(Filters(event="2026gadal"))
    assert gadal["matches"] == 30 and gadal["units_warn_hot"] == 0
    assert gadal["powered_h"] == pytest.approx(30 * 23 * 360 / 3600)  # practice excluded


# --- units --------------------------------------------------------------------------------


def test_unit_moved_between_robots(queries: HistoryQueries) -> None:
    unit = queries.unit("ctre:MOVER")
    assert unit["found"] and unit["identity"] == "inventory" and unit["serial"] == "MOVER"
    events = list(reversed(unit["lifeline"]))  # oldest first
    assert [(e["kind"], e["robot"], e["slot_id"]) for e in events] == [
        ("first-seen", "2026-practice", "drive-fl"),
        ("moved", "2026-comp", "drive-fr"),
        ("last-seen", "2026-comp", "drive-fr"),
    ]
    assert events[1]["bus"] == "canivore" and events[1]["can_id"] == 21
    totals = unit["totals"]
    assert totals["sessions"] == MATCHES + 5 and totals["matches"] == MATCHES
    assert totals["powered_h"] == pytest.approx((MATCHES + 5) * 0.1)
    comp_only = queries.unit("ctre:MOVER", robot="2026-comp")["totals"]
    assert comp_only["sessions"] == MATCHES


def test_replaced_unit(queries: HistoryQueries) -> None:
    roller_a = queries.unit("ctre:ROLLER-A")["lifeline"]
    assert roller_a[0]["kind"] == "replaced-by" and roller_a[0]["other_unit"] == "ctre:ROLLER-B"
    assert roller_a[0]["match_key"] == match_key(30)
    roller_b = queries.unit("ctre:ROLLER-B")["lifeline"]
    assert roller_b[-1]["kind"] == "first-seen" and roller_b[-1]["match_key"] == match_key(30)


def test_legacy_units_kept_apart(queries: HistoryQueries) -> None:
    legacy = queries.unit("legacy:2026-comp:steer-fl:0")
    serial = queries.unit("ctre:STEER-FL")
    assert legacy["identity"] == "legacy" and legacy["totals"]["sessions"] == 5
    assert serial["totals"]["sessions"] == MATCHES - 5
    assert legacy["lifeline"][-1]["source"] == "legacy"
    assert legacy["lifeline"][0]["other_unit"] == "ctre:STEER-FL"


def test_totals_equal_sum_of_sessions(queries: HistoryQueries) -> None:
    unit = queries.unit("ctre:DRIVE-FL", season="2026")
    rows = queries._rows("SELECT * FROM usage WHERE unit_id = 'ctre:DRIVE-FL'", [])
    assert unit["totals"]["supply_wh"] == pytest.approx(sum(r["supply_energy_wh"] for r in rows))
    assert unit["totals"]["thermal_cycles"] == len(rows)
    assert unit["totals"]["temp_max_c"] == max(r["temp_max_c"] for r in rows)


def test_unknown_unit_suggests_close_ids(queries: HistoryQueries) -> None:
    missing = queries.unit("ctre:ROLLER-C")
    assert missing["found"] is False
    assert set(missing["suggestions"][:2]) == {"ctre:ROLLER-A", "ctre:ROLLER-B"}
    assert queries.unit("HOOD")["suggestions"][:2] == ["ctre:HOOD-G", "ctre:HOOD-T"]


def test_sessions_without_aligned_samples(season: SeasonLake, tmp_path: Path) -> None:
    import polars as pl

    lake = build_season(tmp_path / "lake").lake
    observations = pl.read_parquet(lake.meta / "slot_observations.parquet")
    extra = pl.DataFrame(
        [{"session_id": "hoot-2026-04-11_19-29-15-c027", "slot_id": "drive-fl",
          "unit_id": "ctre:DRIVE-FL", "source": "inventory", "from_ts_us": None,
          "to_ts_us": None}],
        schema=observations.schema,
    )  # fmt: skip
    pl.concat([observations, extra]).write_parquet(lake.meta / "slot_observations.parquet")
    sessions = pl.read_parquet(lake.meta / "sessions.parquet")
    hoot = pl.DataFrame(
        [{"session_id": "hoot-2026-04-11_19-29-15-c027", "wpilog_id": None, "robot": "2026-comp",
          "season": "2026", "match_key": "2026gacmp_e10", "match_source": "filename",
          "kind": "match", "warnings": None}],
        schema=sessions.schema,
    )  # fmt: skip
    pl.concat([sessions, hoot]).write_parquet(lake.meta / "sessions.parquet")
    unit = HistoryQueries(lake, season.robots).unit("ctre:DRIVE-FL")
    assert unit["sessions_without_aligned_samples"] == 1
    assert unit["lifeline"][0]["t"].startswith("2026-04-11T19:29:15")


# --- lake access --------------------------------------------------------------------------


def test_empty_lake(tmp_path: Path) -> None:
    queries = HistoryQueries(LakePaths(tmp_path / "empty"))
    assert queries.is_empty()
    assert queries.trend("peak_temp", "unit", Filters())["points"] == []
    assert queries.devices("peak_temp", Filters()) == []
    assert queries.summary(Filters())["units"] == 0
    assert queries.filter_values()["event"] == []
    assert queries.unit("ctre:X")["found"] is False


def test_connection_is_lazy(season: SeasonLake) -> None:
    queries = HistoryQueries(season.lake, season.robots)
    assert not queries.is_open
    queries.summary(Filters())
    assert queries.is_open
    queries.close()
    assert not queries.is_open


@pytest.mark.perf
def test_season_queries_under_two_seconds(season: SeasonLake) -> None:
    queries = HistoryQueries(season.lake, season.robots)
    filters = Filters(subsystem="drivetrain")
    calls: dict[str, Callable[[], object]] = {
        "filters": lambda: queries.filter_values(),
        "trend": lambda: queries.trend("peak_temp", "unit", filters),
        "trend-slot": lambda: queries.trend("supply_wh", "slot", filters),
        "devices": lambda: queries.devices("peak_temp", filters),
        "summary": lambda: queries.summary(filters),
        "unit": lambda: queries.unit("ctre:MOVER"),
    }
    for name, call in calls.items():
        started = time.perf_counter()
        call()
        elapsed = time.perf_counter() - started
        assert elapsed < 2.0, (name, elapsed)
