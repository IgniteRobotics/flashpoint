"""Throughput budget for the wpilog reader (spec: wpilog-reading / Throughput)."""

import time
from collections.abc import Callable
from pathlib import Path

import pytest

from flashpoint import config
from flashpoint.readers import hoot
from flashpoint.readers.wpilog import read_wpilog
from tests.wpilog_builder import WpilogBuilder

pytestmark = [pytest.mark.corpus, pytest.mark.perf]

MIN_RECORDS_PER_SECOND = 10_000_000
LARGE_EXPORT_BUDGET_S = 4.0


def test_all_signals_export_decodes_within_budget(
    corpus_group: Callable[[str], list[Path]], tmp_path: Path
) -> None:
    registry = hoot.default_registry(config.cache_root())
    canivore = next(
        p for p in corpus_group("2026-gacmp-q7") if "_rio_" not in p.name and p.suffix == ".hoot"
    )
    export = hoot.convert(canivore, tmp_path, registry, profile="all").wpilog

    warm = WpilogBuilder().start(1, "x", "double").double(1, 1, 1.0).write(tmp_path / "warm.wpilog")
    read_wpilog(warm).to_arrow()  # compile/load the cached kernels outside the timed region

    started = time.perf_counter()
    log = read_wpilog(export)
    table = log.to_arrow()
    elapsed = time.perf_counter() - started

    rate = table.num_rows / elapsed
    print(f"\n{table.num_rows:,} records in {elapsed:.2f}s = {rate / 1e6:.1f}M rec/s")
    assert table.num_rows > 30_000_000
    assert elapsed < LARGE_EXPORT_BUDGET_S
    assert rate >= MIN_RECORDS_PER_SECOND
