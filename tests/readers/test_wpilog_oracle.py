"""Equivalence with the official WPILib reader (robotpy-wpiutil) on real corpus logs."""

import math
from collections.abc import Callable
from pathlib import Path

import pytest
from wpiutil.log import DataLogReader

from flashpoint.readers.wpilog import read_wpilog

pytestmark = pytest.mark.corpus

_SCALAR = {"double", "float", "int64", "boolean", "string", "json"}


def _oracle(path: Path) -> tuple[list[tuple[str, str]], list[tuple[str, int, object]]]:
    entries: dict[int, tuple[str, str]] = {}
    catalog: list[tuple[str, str]] = []
    rows: list[tuple[str, int, object]] = []
    for rec in DataLogReader(str(path)):
        if rec.isStart():
            start = rec.getStartData()
            entries[start.entry] = (start.name, start.type)
            catalog.append((start.name, start.type))
        elif rec.isFinish():
            entries.pop(rec.getFinishEntry(), None)
        elif rec.isControl():
            continue
        elif rec.getEntry() in entries:
            name, type_ = entries[rec.getEntry()]
            if type_ not in _SCALAR:
                value: object = None
            elif type_ == "double":
                value = rec.getDouble()
            elif type_ == "float":
                value = rec.getFloat()
            elif type_ == "int64":
                value = rec.getInteger()
            elif type_ == "boolean":
                value = rec.getBoolean()
            else:
                value = rec.getString()
            rows.append((name, rec.getTimestamp(), value))
    return catalog, rows


def _ours(path: Path) -> tuple[list[tuple[str, str]], list[tuple[str, int, object]]]:
    log = read_wpilog(path)
    catalog = [(e.name, e.type) for e in log.catalog]
    rows = []
    for r in log.samples().iter_rows(named=True):
        t = r["type"]
        if t in ("double", "float"):
            value: object = r["v_f64"]
        elif t == "int64":
            value = r["v_i64"]
        elif t == "boolean":
            value = r["v_bool"]
        elif t in ("string", "json"):
            value = r["v_str"]
        else:
            value = None
        rows.append((r["signal"], r["ts_us"], value))
    return catalog, rows


def _same(a: object, b: object) -> bool:
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    return a == b


@pytest.mark.parametrize("group", ["2026-gacmp-q7", "2025-gadal-q30"])
def test_matches_official_reader(group: str, corpus_group: Callable[[str], list[Path]]) -> None:
    path = next(p for p in corpus_group(group) if p.suffix == ".wpilog")
    oracle_catalog, oracle_rows = _oracle(path)
    our_catalog, our_rows = _ours(path)

    assert our_catalog == oracle_catalog
    assert len(our_rows) == len(oracle_rows)
    mismatches = [
        (o, u) for o, u in zip(oracle_rows, our_rows, strict=True)
        if o[:2] != u[:2] or not _same(o[2], u[2])
    ]  # fmt: skip
    assert not mismatches, mismatches[:5]
