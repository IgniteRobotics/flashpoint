"""End-to-end ingest of the whole golden corpus into an empty lake."""

from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.ingest import Ingestor, IngestReport
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.query import connect
from flashpoint.readers import hoot

pytestmark = pytest.mark.corpus

TRUNCATED = "GACMP_E10_rio_2026-04-11_19-26-19.truncated-60pct.hoot"
EMPTY = "GADAL_Q11_6E9415C3394C485320202050101C18FF_2026-03-06_17-52-51.hoot"


def _ingest(lake: LakePaths, corpus_dir: Path) -> IngestReport:
    ingestor = Ingestor(lake, hoot.default_registry(config.cache_root()))
    try:
        return ingestor.ingest([corpus_dir])
    finally:
        ingestor.close()


@pytest.fixture(scope="module")
def ingested(
    corpus_dir: Path, tmp_path_factory: pytest.TempPathFactory
) -> tuple[LakePaths, IngestReport]:
    lake = LakePaths(tmp_path_factory.mktemp("e2e") / "lake")
    return lake, _ingest(lake, corpus_dir)


def test_expected_successes_and_quarantines(ingested: tuple[LakePaths, IngestReport]) -> None:
    _, report = ingested
    quarantined = {r.path.name: r.reason for r in report.results if r.status == "quarantined"}

    assert quarantined == {EMPTY: "empty-file"}
    assert report.count("success") == 14
    assert report.exit_code == 1


def test_truncated_hoot_flagged_short_coverage(ingested: tuple[LakePaths, IngestReport]) -> None:
    lake, report = ingested
    warned = {r.path.name: r.warnings for r in report.results if r.warnings}

    assert warned == {
        TRUNCATED: ["short-coverage"],
        # Real power-loss truncation from the event: owlet reads what it can, exits 1.
        "GACMP_E10_rio_2026-04-11_19-29-15.hoot": ["incomplete-read"],
        "GACMP_E10_6E9415C3394C485320202050101C18FF_2026-04-11_19-29-15.hoot": ["incomplete-read"],
    }
    con = connect(lake)
    integrity = dict(
        con.sql(
            "select l.filename, h.integrity from meta_hoot_logs h join meta_logs l using (log_id)"
        ).fetchall()
    )
    assert integrity[TRUNCATED] == "short-coverage"
    assert integrity["GACMP_E10_rio_2026-04-11_19-26-19.hoot"] == "ok"
    read_status = dict(
        con.sql(
            "select l.filename, h.read_status from meta_hoot_logs h join meta_logs l using (log_id)"
        ).fetchall()
    )
    assert read_status["GACMP_E10_rio_2026-04-11_19-29-15.hoot"] == "incomplete"
    assert read_status["GACMP_Q7_rio_2026-04-09_21-51-06.hoot"] == "complete"


def test_metadata_is_queryable(ingested: tuple[LakePaths, IngestReport]) -> None:
    lake, _ = ingested
    con = connect(lake)

    q7 = con.sql(
        "select fms_event, fms_match_type, fms_match_number, season from meta_logs"
        " where filename = 'FRC_20260409_215113_GACMP_Q7.wpilog'"
    ).fetchone()
    assert q7 == ("GACMP", 2, 7, "2026")
    compliancies = dict(
        con.sql(
            "select season, max(compliancy) from meta_hoot_logs h"
            " join meta_logs using (log_id) group by season"
        ).fetchall()
    )
    assert compliancies == {"2025": 13, "2026": 19}
    assert con.sql("select bool_and(pro_licensed = 1) from meta_hoot_logs").fetchone() == (True,)


def test_one_signal_query_is_fast(ingested: tuple[LakePaths, IngestReport]) -> None:
    import time

    lake, _ = ingested
    con = connect(lake)
    log_id = con.sql("select log_id from meta_logs where filename like 'GACMP_Q7_rio%'").fetchone()
    assert log_id is not None
    started = time.perf_counter()
    rows = con.sql(
        "select ts_us, v_f64 from samples"
        " where log_id = ? and signal = 'Phoenix6/TalonFX-1/SupplyCurrent'",
        params=[log_id[0]],
    ).fetchall()
    assert time.perf_counter() - started < 1.0
    assert rows


def test_second_run_is_a_noop(ingested: tuple[LakePaths, IngestReport], corpus_dir: Path) -> None:
    lake, _ = ingested
    before = {
        p: p.read_bytes()
        for p in [*lake.bronze.rglob("*.parquet"), *lake.raw.rglob("*")]
        if p.is_file()
    }

    report = _ingest(lake, corpus_dir)

    assert report.count("success") == 0
    assert report.count("skipped") == 15
    after = {
        p: p.read_bytes()
        for p in [*lake.bronze.rglob("*.parquet"), *lake.raw.rglob("*")]
        if p.is_file()
    }
    assert after == before
