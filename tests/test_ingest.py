from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.ingest import Ingestor, IngestReport, discover
from flashpoint.lake import bronze
from flashpoint.lake.ledger import Ledger, Stage
from flashpoint.lake.paths import LakePaths
from flashpoint.lake.query import connect
from flashpoint.readers import hoot
from flashpoint.readers.hoot import HootError, OwletRegistry
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


def test_unexpected_error_fails_file_for_retry(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*_args: object, **_kwargs: object) -> Path:
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(bronze, "write", boom)
    (result,) = _run(lake, registry, _log(tmp_path / "a.wpilog")).results
    assert (result.status, result.reason) == ("failed", "internal-error:RuntimeError")


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


# --- machine failures are retried (#14) ---------------------------------------------------


class FailingRegistry(OwletRegistry):
    """An owlet registry that fails with a chosen error until `error` is set to None."""

    def __init__(self, tmp_path: Path, error: Exception | None) -> None:
        manifest = tmp_path / "fake-manifest.toml"
        manifest.write_text("")
        super().__init__(manifest, tmp_path / "fake-owlet-cache")
        self.error = error

    def binary_for(self, compliancy: int, download: bool = True) -> Path:
        if self.error is not None:
            raise self.error
        return Path("owlet")


def _fake_convert(
    hoot_path: Path, out_dir: Path, registry: OwletRegistry, profile: str = "health"
) -> hoot.HootConversion:
    registry.binary_for(1)
    out_dir.mkdir(parents=True, exist_ok=True)
    wpilog = _log(out_dir / "converted.wpilog")
    return hoot.HootConversion(wpilog, 1, "fake", False, profile, 1, "rio")


HOOT_NAME = "GACMP_Q7_rio_2026-04-09_21-51-13.hoot"


@pytest.fixture
def hoot_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(hoot, "convert", _fake_convert)
    path = tmp_path / "logs" / HOOT_NAME
    path.parent.mkdir()
    path.write_bytes(b"not read by the fake converter")
    return path


def _stage(lake: LakePaths, sha: str) -> tuple[Stage | None, str | None]:
    ledger = Ledger(lake.ledger)
    try:
        return ledger.stage(sha), ledger.reason(sha)
    finally:
        ledger.close()


def test_download_failure_is_failed_then_retried_to_success(
    lake: LakePaths, tmp_path: Path, hoot_file: Path
) -> None:
    registry = FailingRegistry(tmp_path, HootError("owlet-download-failed", "no network"))
    (first,) = _run(lake, registry, hoot_file).results
    assert (first.status, first.reason) == ("failed", "owlet-download-failed")
    assert _stage(lake, first.sha256) == (Stage.FAILED, "owlet-download-failed")

    registry.error = None
    (second,) = _run(lake, registry, hoot_file).results
    assert second.status == "success"
    assert _stage(lake, first.sha256) == (Stage.SUCCESS, None)


def test_environment_errors_never_quarantine(
    lake: LakePaths, tmp_path: Path, hoot_file: Path
) -> None:
    registry = FailingRegistry(tmp_path, HootError("owlet-download-failed"))
    for _ in range(5):
        (result,) = _run(lake, registry, hoot_file).results
        assert result.status == "failed"
        assert _stage(lake, result.sha256)[0] == Stage.FAILED


def test_owlet_crash_is_quarantined_on_the_third_attempt(
    lake: LakePaths, tmp_path: Path, hoot_file: Path
) -> None:
    registry = FailingRegistry(tmp_path, HootError("owlet-failed", "exit 1"))
    statuses = [_run(lake, registry, hoot_file).results[0] for _ in range(3)]
    assert [(r.status, r.reason) for r in statuses] == [
        ("failed", "owlet-failed"),
        ("failed", "owlet-failed"),
        ("quarantined", "owlet-failed"),
    ]
    assert _stage(lake, statuses[0].sha256) == (Stage.QUARANTINED, "owlet-failed")


def test_empty_file_quarantines_on_the_first_run(lake: LakePaths, tmp_path: Path) -> None:
    empty = tmp_path / HOOT_NAME
    empty.write_bytes(b"")
    (result,) = _run(lake, FailingRegistry(tmp_path, None), empty).results
    assert (result.status, result.reason) == ("quarantined", "empty-file")


def test_unexpected_error_is_failed(lake: LakePaths, tmp_path: Path, hoot_file: Path) -> None:
    (result,) = _run(lake, FailingRegistry(tmp_path, RuntimeError("boom")), hoot_file).results
    assert (result.status, result.reason) == ("failed", "internal-error:RuntimeError")


def test_failed_file_leaves_no_bronze_or_metadata(
    lake: LakePaths, tmp_path: Path, hoot_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = FailingRegistry(tmp_path, None)
    (ok,) = _run(lake, registry, hoot_file).results
    assert ok.status == "success"
    monkeypatch.setattr(config, "PIPELINE_VERSION", config.PIPELINE_VERSION + 1)
    registry.error = HootError("owlet-timeout")

    (result,) = _run(lake, registry, hoot_file).results

    assert result.status == "failed"
    assert not list(lake.bronze.rglob("*.parquet"))
    ledger = Ledger(lake.ledger)
    try:
        assert ledger.query("select * from logs where log_id = ?", (result.sha256,)) == []
    finally:
        ledger.close()


def test_rerun_reports_quarantined_earlier_and_exits_zero(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    bad = tmp_path / "bad.wpilog"
    bad.write_bytes(b"NOTALOG" * 4)
    good = _log(tmp_path / "good.wpilog")
    assert _run(lake, registry, bad, good).exit_code == 1

    report = _run(lake, registry, bad, good)

    assert {(r.path.name, r.status, r.reason) for r in report.results} == {
        ("bad.wpilog", "quarantined-earlier", "invalid-header"),
        ("good.wpilog", "skipped", None),
    }
    assert report.exit_code == 0


def test_failed_file_makes_exit_code_non_zero(
    lake: LakePaths, tmp_path: Path, hoot_file: Path
) -> None:
    report = _run(lake, FailingRegistry(tmp_path, HootError("owlet-timeout")), hoot_file)
    assert (report.count("failed"), report.exit_code) == (1, 1)


def _rebuild(lake: LakePaths, registry: OwletRegistry, quarantined: bool = False) -> IngestReport:
    ingestor = Ingestor(lake, registry)
    try:
        return ingestor.rebuild(quarantined=quarantined)
    finally:
        ingestor.close()


def test_rebuild_retries_failed_files_from_raw(
    lake: LakePaths, tmp_path: Path, hoot_file: Path
) -> None:
    registry = FailingRegistry(tmp_path, HootError("owlet-download-failed"))
    _run(lake, registry, hoot_file)
    hoot_file.unlink()
    registry.error = None

    (result,) = _rebuild(lake, registry).results

    assert result.status == "success"
    assert _stage(lake, result.sha256) == (Stage.SUCCESS, None)


def test_rebuild_quarantined_retries_only_quarantined_and_resets_attempts(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path, hoot_file: Path
) -> None:
    failing = FailingRegistry(tmp_path, HootError("owlet-failed"))
    for _ in range(3):
        _run(lake, failing, hoot_file)
    good = _log(tmp_path / "good.wpilog")
    _run(lake, registry, good)
    empty = tmp_path / "empty.wpilog"
    empty.write_bytes(b"")
    _run(lake, registry, empty)
    failing.error = None

    report = _rebuild(lake, failing, quarantined=True)

    assert {(r.path.suffix, r.status, r.reason) for r in report.results} == {
        (".hoot", "success", None),
        (".wpilog", "quarantined", "empty-file"),
    }
    ledger = Ledger(lake.ledger)
    try:
        assert all(row["attempts"] == 0 for row in ledger.files())
    finally:
        ledger.close()


def test_rebuild_quarantined_suspect_restarts_its_count(
    lake: LakePaths, tmp_path: Path, hoot_file: Path
) -> None:
    failing = FailingRegistry(tmp_path, HootError("owlet-failed"))
    for _ in range(3):
        _run(lake, failing, hoot_file)
    (result,) = _rebuild(lake, failing, quarantined=True).results
    assert result.status == "failed"  # attempt 1 of a fresh count, not a 4th strike


def test_rebuild_reports_file_without_raw_copy(
    lake: LakePaths, registry: OwletRegistry, tmp_path: Path
) -> None:
    bad = tmp_path / "bad.wpilog"
    bad.write_bytes(b"NOTALOG" * 4)
    (quarantined,) = _run(lake, registry, bad).results
    for path in lake.raw.rglob("*"):
        if path.is_file():
            path.unlink()

    (result,) = _rebuild(lake, registry, quarantined=True).results

    assert (result.status, result.reason) == ("missing-raw", "invalid-header")
    assert _stage(lake, quarantined.sha256) == (Stage.QUARANTINED, "invalid-header")
