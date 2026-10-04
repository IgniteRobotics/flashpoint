from collections.abc import Callable
from pathlib import Path

import pytest

from flashpoint.meta import extract
from flashpoint.readers.wpilog import read_wpilog

pytestmark = pytest.mark.corpus


def _wpilog(paths: list[Path]) -> Path:
    return next(p for p in paths if p.suffix == ".wpilog")


def test_q7_identified_from_fms(corpus_group: Callable[[str], list[Path]]) -> None:
    path = _wpilog(corpus_group("2026-gacmp-q7"))
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert (meta.fms_event, meta.fms_match_type, meta.fms_match_number) == ("GACMP", 2, 7)
    assert meta.project == "Robot-2026"
    assert meta.season == "2026"


@pytest.mark.parametrize("group", ["2026-gacmp-q7", "2026-gacmp-p2", "2025-gadal-q30"])
def test_filename_time_falls_inside_anchored_span(
    group: str, corpus_group: Callable[[str], list[Path]]
) -> None:
    path = _wpilog(corpus_group(group))
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert meta.anchor_source == "systemTime"
    assert meta.utc_start is not None and meta.utc_end is not None
    assert meta.file.utc is not None
    assert meta.utc_start <= meta.file.utc <= meta.utc_end


def test_nofms_log_has_no_match_number(corpus_group: Callable[[str], list[Path]]) -> None:
    path = _wpilog(corpus_group("2025-nofms"))
    meta = extract.wpilog_metadata(read_wpilog(path), path.name)

    assert meta.fms_match_number is None
    assert meta.file.match_number is None
    assert meta.season == "2025"


def test_hoot_bus_detection(corpus_group: Callable[[str], list[Path]]) -> None:
    buses = {
        extract.parse_hoot_filename(p.name).bus
        for p in corpus_group("2026-gacmp-q7")
        if p.suffix == ".hoot"
    }
    assert buses == {"rio", "6E9415C3394C485320202050101C18FF"}
