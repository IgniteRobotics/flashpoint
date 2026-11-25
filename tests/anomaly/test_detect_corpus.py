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
from tests.brownout_config import Q7_BROWNOUTS

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


def test_no_brownouts_from_motor_dips(derived_corpus_lake: Any) -> None:
    """Shipped thresholds: the gyro stays above 6.75 V, so motors' own dips raise nothing."""
    rows = detect.compute(derived_corpus_lake, SETTINGS).rows
    assert [r for r in rows if r["subject"] == "power"] == []


def test_q7_brownout_anomaly(derived_corpus_lake: Any, brownout_config: Path) -> None:
    """Test thresholds (brownout 8.5 V, sag 9.0 V), above Q7's lowest battery proxy voltage."""
    rows = detect.compute(derived_corpus_lake, detect.load_settings(brownout_config)).rows
    brownouts = [r for r in rows if r["metric"] == "brownouts"]
    assert len(brownouts) == 1
    (row,) = brownouts
    assert (row["method"], row["severity"], row["subject"], row["robot"]) == (
        "SPEC", "HIGH", "power", "2026-comp",
    )  # fmt: skip
    assert row["onset_match_key"] == Q7 and row["slot_id"] == "power"
    assert "battery proxy: gyro" in row["why"]

    builder = ReportBuilder(derived_corpus_lake, brownout_config)
    try:
        builder.build(match_keys=[Q7])
    finally:
        builder.close()
    text = (data_dir(derived_corpus_lake) / f"{Q7}.js").read_text(encoding="utf-8")
    markers = json.loads(text[text.index("(") + 1 : -3])["markers"]
    faults = [m for m in markers if m["kind"] == "brownout" and m["level"] == "FAULT"]
    assert row["actual_value"] == len(faults) == Q7_BROWNOUTS


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


def test_dropout_on_a_corpus_match(
    corpus_group: Callable[[str], list[Path]], tmp_path: Path
) -> None:
    """anomaly-detection "Dropout on a corpus match": Q7 with drive-fl (CANivore TalonFX-11)
    silent for 1 s while enabled; frame-rate scaling must not hide it or flag its siblings."""
    import duckdb
    import pyarrow.parquet as pq

    from flashpoint.ingest import Ingestor
    from flashpoint.lake.paths import LakePaths
    from flashpoint.readers import hoot
    from flashpoint.semantics import can_health
    from flashpoint.semantics.derive import Deriver

    lake = LakePaths(tmp_path / "lake")
    ingestor = Ingestor(lake, hoot.default_registry(config.cache_root()))
    try:
        ingestor.ingest(corpus_group("2026-gacmp-q7"))
    finally:
        ingestor.close()
    (canivore,) = [
        r[0]
        for r in duckdb.execute(
            f"SELECT log_id FROM read_parquet('{lake.meta / 'hoot_logs.parquet'}')"
            " WHERE bus != 'rio'"
        ).fetchall()
    ]
    health = can_health.analyse_hoot(duckdb.connect(), lake, canivore)
    start, end = max(health.counted, key=lambda w: w[1] - w[0])  # teleop
    t0 = start + 30_000_000
    assert t0 + 1_000_000 < end
    part = lake.bronze / "season=2026" / f"log_id={canivore}" / "part-0.parquet"
    table = pq.read_table(part)
    signal = table.column("signal").cast("string").to_pylist()
    ts = table.column("ts_us").to_pylist()
    keep = [
        not (s.startswith("Phoenix6/TalonFX-11/") and t0 < t < t0 + 1_000_000)
        for s, t in zip(signal, ts, strict=True)
    ]
    pq.write_table(table.filter(keep), part)
    deriver = Deriver(lake, config.config_root())
    try:
        deriver.run(force=True)
    finally:
        deriver.close()

    can_metrics = ("isolated_gaps", "missed_per_min", "longest_gap_ms")
    rows = [r for r in detect.compute(lake, SETTINGS).rows if r["metric"] in can_metrics]
    dropped = [r for r in rows if r["slot_id"] == "drive-fl"]
    assert [(r["method"], r["metric"], r["severity"]) for r in dropped] == [
        ("SPEC", "longest_gap_ms", "HIGH")
    ]
    canivore_slots = {
        "drive-fr", "drive-bl", "drive-br", "steer-fl", "steer-fr", "steer-bl", "steer-br",
        "encoder-fl", "encoder-fr", "encoder-bl", "encoder-br", "gyro",
    }  # fmt: skip
    assert [r for r in rows if r["slot_id"] in canivore_slots] == []


def test_brownout_on_the_median_fallback(derived_corpus_lake: Any, brownout_config: Path,
                                         tmp_path: Path) -> None:  # fmt: skip
    """The test thresholds, and a robot file with no battery proxy: at most MED."""
    import re
    import shutil

    no_proxy = shutil.copytree(brownout_config, tmp_path / "config")
    robot = no_proxy / "robots" / "2026-comp.toml"
    robot.write_text(
        re.sub(r"(?m)^battery_proxy = .*\n", "", robot.read_text(encoding="utf-8")),
        encoding="utf-8",
    )
    rows = detect.compute(derived_corpus_lake, detect.load_settings(no_proxy)).rows
    brownouts = [r for r in rows if r["metric"] == "brownouts"]
    assert brownouts, "the fallback proxy must still find Q7's brownouts"
    for row in brownouts:
        assert row["severity"] == "MED"
        assert re.search(r"median of \d+ devices", row["why"])
        assert "declare a battery-proxy slot" in row["why"]
        assert "wiring drops" in row["why"]
