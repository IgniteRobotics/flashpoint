"""Restore drill on the golden corpus (spec: lake-backup / Restore drill), with real rclone.

The corpus lake and its configuration are backed up to a local-directory remote
(`:local:<path>`), restored into a new lake and an empty configuration folder (spec:
config-backup) and rebuilt with the restored configuration; the new ledger must list the same
files and triage, gold must hold the same match features, the configuration files must come back
unchanged, and a resolved anomaly must come back with its id and outcome. Skipped where rclone
is not installed, except in CI, where that is a failure.
"""

import json
import os
import shutil
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flashpoint import config as flashpoint_config
from flashpoint.acquire.backup import config_files, restore, run_backup
from flashpoint.cli import main
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.gold import gold_dir
from tests.acquire.helpers import ledger_rows
from tests.brownout_config import write_brownout_config

pytestmark = pytest.mark.corpus


def _gold(lake: LakePaths) -> pl.DataFrame:
    frame = pl.read_parquet(gold_dir(lake) / "**" / "*.parquet", hive_partitioning=True)
    return frame.sort(frame.columns)


def restore_drill(
    corpus_dir: Path, tmp_path: Path, remote: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ingest the corpus, back it up to `remote`, restore into a new lake, rebuild, compare."""
    # The robot configuration with the test brownout thresholds (so Q7 has a brownout to triage),
    # plus the backup remote.
    config_dir = write_brownout_config(tmp_path / "config")
    monkeypatch.setenv(flashpoint_config.CONFIG_ENV, str(config_dir))
    toml = f"[backup]\nremote = {json.dumps(remote)}\n"  # a JSON string is a TOML string
    (config_dir / "acquire.toml").write_text(toml, encoding="utf-8")
    original = LakePaths(tmp_path / "original")
    restored = LakePaths(tmp_path / "restored")

    ingest_code = main(["ingest", str(corpus_dir), "--lake", str(original.root)])
    resolved = _resolve_one(original)
    assert main(["backup", "--lake", str(original.root)]) == 0
    # A new laptop: an empty configuration folder, so restore brings the team's files back and
    # the rebuild below runs on them.
    scratch_config = tmp_path / "scratch-config"
    monkeypatch.setenv(flashpoint_config.CONFIG_ENV, str(scratch_config))
    assert main(["restore", "--remote", remote, "--lake", str(restored.root)]) == 0
    names = config_files(config_dir)
    assert "robots/2026-comp.toml" in names and "report.toml" in names
    assert config_files(scratch_config) == names
    for name in names:
        assert (scratch_config / name).read_bytes() == (config_dir / name).read_bytes()
    assert main(["rebuild", "--lake", str(restored.root)]) == ingest_code

    files = "SELECT sha256, kind, size, stage, reason FROM files ORDER BY sha256"
    assert ledger_rows(restored.ledger, files) == ledger_rows(original.ledger, files)
    versions = ledger_rows(restored.ledger, "SELECT DISTINCT pipeline_version AS v FROM files")
    assert versions == [{"v": flashpoint_config.PIPELINE_VERSION}]
    gold = _gold(original)
    assert gold.height > 0
    assert_frame_equal(_gold(restored), gold)
    triage = "SELECT anomaly_id, at, from_state, to_state, outcome, subject FROM anomaly_triage"
    assert ledger_rows(restored.ledger, triage) == ledger_rows(original.ledger, triage) != []
    # Survives rebuild: the rebuilt lake's detection produces the same id, still resolved.
    from flashpoint.views.anomalies import AnomalyQueries

    detail = AnomalyQueries(restored, scratch_config).detail(resolved)
    assert detail is not None and detail["detected"] is True
    assert (detail["state"], detail["outcome"]) == ("resolved", "battery replaced")


def _resolve_one(lake: LakePaths) -> str:
    """Resolve the corpus's brownout anomaly (Q7, under the test thresholds) through the triage
    write path."""
    from flashpoint.anomaly import detect
    from flashpoint.views.anomalies import AnomalyQueries

    rows = detect.read_anomalies(lake).filter(pl.col("metric") == "brownouts").to_dicts()
    anomaly_id = str(rows[0]["id"])
    queries = AnomalyQueries(lake, flashpoint_config.config_root())
    queries.triage(anomaly_id, {"to": "investigating"})
    queries.triage(anomaly_id, {"to": "resolved", "outcome": "battery replaced"})
    return anomaly_id


def test_restore_drill_reproduces_the_ledger_and_gold(
    corpus_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if shutil.which("rclone") is None:
        if os.environ.get("CI"):
            pytest.fail("rclone is not installed; the CI corpus job must install it")
        pytest.skip("rclone is not installed")
    if not corpus_dir.is_dir():
        pytest.fail("corpus not fetched; run tools/fetch-corpus.py")
    restore_drill(corpus_dir, tmp_path, f":local:{tmp_path / 'remote'}", monkeypatch)


def test_restore_from_a_backup_without_configuration_with_real_rclone(tmp_path: Path) -> None:
    """rclone exits 3 for a missing `config/latest` on a local remote; restore reads that as a
    backup made before configuration was included (spec: config-backup)."""
    if shutil.which("rclone") is None:
        if os.environ.get("CI"):
            pytest.fail("rclone is not installed; the CI corpus job must install it")
        pytest.skip("rclone is not installed")
    from flashpoint.lake.ledger import Ledger

    lake = LakePaths(tmp_path / "lake")
    lake.ensure()
    Ledger(lake.ledger).close()
    (lake.raw / "ab").mkdir()
    (lake.raw / "ab" / "ab.wpilog").write_bytes(b"log")  # rclone makes no folder for an empty raw
    remote = f":local:{tmp_path / 'remote'}"
    run_backup(lake, remote)  # no configuration root: the remote has no config/
    root = tmp_path / "config"

    result = restore(LakePaths(tmp_path / "restored"), remote, config_root=root)

    assert result.config == "none"
    assert not root.exists()
