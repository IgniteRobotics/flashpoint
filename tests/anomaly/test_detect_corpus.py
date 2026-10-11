"""Detection on the golden corpus (spec: anomaly-detection)."""

import json
import subprocess
import sys
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.anomaly import detect
from flashpoint.report.build import ReportBuilder
from flashpoint.report.paths import data_dir

pytestmark = pytest.mark.corpus
Q7 = "2026gacmp_qm7"
SETTINGS = detect.load_settings(config.config_root())


def test_runs_after_derive(corpus_group: Callable[[str], list[Path]], tmp_path: Path) -> None:
    match_dir = tmp_path / "q7"
    match_dir.mkdir()
    for path in corpus_group("2026-gacmp-q7"):
        (match_dir / path.name).symlink_to(path)
    lake = tmp_path / "lake"
    subprocess.run(
        [sys.executable, "-m", "flashpoint", "ingest", "--lake", str(lake), str(match_dir)],
        check=True,
        capture_output=True,
    )
    summary = json.loads((lake / "gold" / "anomalies" / "summary.json").read_text())
    gold_written = max(p.stat().st_mtime for p in (lake / "gold").glob("match_features/*/*/*"))
    built = datetime.fromisoformat(summary["built_at"]).timestamp()
    assert built >= int(gold_written)
    assert (lake / "gold" / "anomalies" / "anomalies.parquet").is_file()


def test_q7_brownout_anomaly(derived_corpus_lake: Any) -> None:
    rows = detect.compute(derived_corpus_lake, SETTINGS).rows
    brownouts = [r for r in rows if r["metric"] == "brownouts"]
    assert len(brownouts) == 1
    (row,) = brownouts
    assert (row["method"], row["severity"], row["subject"], row["robot"]) == (
        "SPEC", "HIGH", "power", "2026-comp",
    )  # fmt: skip
    assert row["onset_match_key"] == Q7 and row["slot_id"] == "power"

    builder = ReportBuilder(derived_corpus_lake, config.config_root())
    try:
        builder.build(match_keys=[Q7])
    finally:
        builder.close()
    text = (data_dir(derived_corpus_lake) / f"{Q7}.js").read_text(encoding="utf-8")
    markers = json.loads(text[text.index("(") + 1 : -3])["markers"]
    faults = [m for m in markers if m["kind"] == "brownout" and m["level"] == "FAULT"]
    assert row["actual_value"] == len(faults)


def test_whole_corpus_completes_with_skip_counts(derived_corpus_lake: Any) -> None:
    computed = detect.compute(derived_corpus_lake, SETTINGS)
    summary = detect.summarize(computed, "fp")
    assert summary["matches"] >= 1 and summary["units"] > 0
    assert isinstance(summary["skipped"], dict) and isinstance(summary["missing"], dict)
    assert summary["missing"], "the corpus has rows without some metrics (e.g. no residual)"
    print(json.dumps({k: summary[k] for k in ("matches", "units", "anomalies", "counts",
                                              "skipped", "missing")}))  # fmt: skip


def test_no_raw_gives_identical_results(derived_corpus_lake: Any) -> None:
    with_raw = detect.compute(derived_corpus_lake, SETTINGS).rows
    raw = derived_corpus_lake.raw
    hidden = raw.with_name("raw-hidden")
    raw.rename(hidden)
    try:
        without_raw = detect.compute(derived_corpus_lake, SETTINGS).rows
    finally:
        hidden.rename(raw)
    assert with_raw == without_raw
