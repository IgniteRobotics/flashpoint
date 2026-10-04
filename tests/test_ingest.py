from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.ingest import Ingestor, IngestReport, discover
from flashpoint.lake import bronze
from flashpoint.lake.ledger import Stage
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.query import connect
from flashpoint.readers.hoot import OwletRegistry
from tests.wpilog_builder import WpilogBuilder


@pytest.fixture
def lake(tmp_path: Path) -> LakePaths:
    return LakePaths(tmp_path / "lake")


@pytest.fixture
def registry(tmp_path: Path) -> OwletRegistry:
    manifest = tmp_path / "empty-manifest.toml"
    manifest.write_text("")
    return OwletRegistry(manifest, tmp_path / "owlet-cache")


def _log(path: Path, value: float = 1.0) -> Path:
    b = (
        WpilogBuilder()
        .start(1, "NT:/FMSInfo/EventName", "string")
        .start(2, "motor/current", "double")
    )
    b.string(1, 10, "GACMP").double(2, 20, value).double(2, 30, value + 1)
    return b.write(path)


def _run(lake: LakePaths, registry: OwletRegistry, *paths: Path) -> IngestReport:
    ingestor = Ingestor(lake, registry)
    try:
        return ingestor.ingest(paths)
    finally:
        ingestor.close()


def test_discover_finds_logs_recursively(tmp_path: Path) -> None:
    (tmp_path / "a" / "b").mkdir(parents=True)
    keep = [_log(tmp_path / "a" / "x.wpilog"), tmp_path / "a" / "b" / "y.hoot"]
    keep[1].write_bytes(b"h")
    (tmp_path / "a" / "notes.txt").write_text("no")
    (tmp_path / "a" / ".hidden.wpilog").write_text("no")
    assert discover([tmp_path]) == sorted(keep)


def test_ingests_wpilog_into_bronze_and_metadata(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    report = _run(lake, registry, _log(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog"))

    assert (report.count("success"), report.exit_code) == (1, 0)
    con = connect(lake)
    assert con.sql("select count(*) from samples where signal = 'motor/current'").fetchone() == (2,)
    assert con.sql("select fms_event, file_match_number, season from meta_logs").fetchone() == (
        "GACMP", 7, "2026",
    )  # fmt: skip


def test_reingest_is_noop_and_names_become_aliases(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    first = _log(tmp_path / "a.wpilog")
    _run(lake, registry, first)
    bronze_files = {p: p.read_bytes() for p in lake.bronze.rglob("*.parquet")}
    copy = tmp_path / "b.wpilog"
    copy.write_bytes(first.read_bytes())

    report = _run(lake, registry, first, copy)

    assert report.count("skipped") == 2
    assert {p: p.read_bytes() for p in lake.bronze.rglob("*.parquet")} == bronze_files
    con = connect(lake)
    assert con.sql("select count(*) from meta_aliases").fetchone() == (2,)


def test_bad_files_are_quarantined_without_stopping_the_batch(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    good = _log(tmp_path / "good.wpilog")
    empty = tmp_path / "empty.wpilog"
    empty.write_bytes(b"")
    bad = tmp_path / "bad.wpilog"
    bad.write_bytes(b"NOTALOG" * 4)
    short_hoot = tmp_path / "s.hoot"
    short_hoot.write_bytes(b"too short")  # distinct content: empty files share one hash

    report = _run(lake, registry, good, empty, bad, short_hoot)

    reasons = {r.path.name: r.reason for r in report.results if r.status == "quarantined"}
    assert reasons == {
        "empty.wpilog": "empty-file",
        "bad.wpilog": "invalid-header",
        "s.hoot": "invalid-header",
    }
    assert report.count("success") == 1
    assert report.exit_code == 1


def test_unknown_hoot_compliancy_is_quarantined(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    path = tmp_path / "x.hoot"
    path.write_bytes(b"bus".ljust(70, b"\x00") + bytes([19, 0]) + b"\x00" * 16)
    (result,) = _run(lake, registry, path).results
    assert (result.status, result.reason) == ("quarantined", "unsupported-compliancy:19")


def test_unexpected_error_quarantines_file(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*_args: object, **_kwargs: object) -> Path:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(bronze, "write", boom)
    (result,) = _run(lake, registry, _log(tmp_path / "a.wpilog")).results
    assert (result.status, result.reason) == ("quarantined", "internal-error:RuntimeError")


def test_rebuild_reprocesses_older_pipeline_versions_from_raw(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _log(tmp_path / "FRC_20260409_215113_GACMP_Q7.wpilog")
    _run(lake, registry, source)
    source.unlink()  # rebuild must not need the original
    monkeypatch.setattr(config, "PIPELINE_VERSION", config.PIPELINE_VERSION + 1)

    ingestor = Ingestor(lake, registry)
    try:
        report = ingestor.rebuild()
        files = ingestor.ledger.files(Stage.SUCCESS)
    finally:
        ingestor.close()

    assert report.count("success") == 1
    assert files[0]["pipeline_version"] == config.PIPELINE_VERSION
    assert connect(lake).sql("select filename from meta_logs").fetchone() == (
        "FRC_20260409_215113_GACMP_Q7.wpilog",
    )
