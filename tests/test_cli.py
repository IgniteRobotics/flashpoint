from pathlib import Path

import pytest

from flashpoint.cli import main
from tests.wpilog_builder import WpilogBuilder


def test_ingest_then_doctor(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    log = WpilogBuilder().start(1, "x", "double").double(1, 1, 1.0).write(tmp_path / "a.wpilog")
    lake = tmp_path / "lake"

    assert main(["ingest", "--lake", str(lake), str(log)]) == 0
    assert "1 ingested, 0 skipped, 0 quarantined" in capsys.readouterr().out

    assert main(["doctor", "--lake", str(lake)]) == 0
    out = capsys.readouterr().out
    assert "success" in out
    assert "owlet cache" in out
    assert "2026-comp" in out and "23 slots" in out


def test_ingest_missing_path_is_usage_error(tmp_path: Path) -> None:
    assert main(["ingest", "--lake", str(tmp_path / "lake"), str(tmp_path / "nope")]) == 2


def test_quarantine_exit_code(tmp_path: Path) -> None:
    bad = tmp_path / "bad.wpilog"
    bad.write_bytes(b"x" * 20)
    assert main(["ingest", "--lake", str(tmp_path / "lake"), str(bad)]) == 1
