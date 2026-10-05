"""Restore drill on the golden corpus (spec: lake-backup / Restore drill), with real rclone.

The corpus lake is backed up to a local-directory remote (`:local:<path>`), restored into a new
lake and rebuilt; the new ledger must list the same files and gold must hold the same match
features. Skipped where rclone is not installed, except in CI, where that is a failure.
"""

import json
import os
import shutil
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flashpoint import config as flashpoint_config
from flashpoint.cli import main
from flashpoint.lake.paths import LakePaths
from flashpoint.semantics.gold import gold_dir
from tests.acquire.helpers import ledger_rows

pytestmark = pytest.mark.corpus


def _gold(lake: LakePaths) -> pl.DataFrame:
    frame = pl.read_parquet(gold_dir(lake) / "**" / "*.parquet", hive_partitioning=True)
    return frame.sort(frame.columns)


def restore_drill(
    corpus_dir: Path, tmp_path: Path, remote: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ingest the corpus, back it up to `remote`, restore into a new lake, rebuild, compare."""
    config_dir = tmp_path / "config"  # the robot configuration plus the backup remote
    shutil.copytree(flashpoint_config.config_root(), config_dir)
    monkeypatch.setenv(flashpoint_config.CONFIG_ENV, str(config_dir))
    toml = f"[backup]\nremote = {json.dumps(remote)}\n"  # a JSON string is a TOML string
    (config_dir / "acquire.toml").write_text(toml, encoding="utf-8")
    original = LakePaths(tmp_path / "original")
    restored = LakePaths(tmp_path / "restored")

    ingest_code = main(["ingest", str(corpus_dir), "--lake", str(original.root)])
    assert main(["backup", "--lake", str(original.root)]) == 0
    assert main(["restore", "--lake", str(restored.root)]) == 0
    assert main(["rebuild", "--lake", str(restored.root)]) == ingest_code

    files = "SELECT sha256, kind, size, stage, reason FROM files ORDER BY sha256"
    assert ledger_rows(restored.ledger, files) == ledger_rows(original.ledger, files)
    versions = ledger_rows(restored.ledger, "SELECT DISTINCT pipeline_version AS v FROM files")
    assert versions == [{"v": flashpoint_config.PIPELINE_VERSION}]
    gold = _gold(original)
    assert gold.height > 0
    assert_frame_equal(_gold(restored), gold)


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
