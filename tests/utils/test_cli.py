from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOOT_FILES = list(DATA_DIR.glob("*.hoot"))


def test_cli_missing_file_exits_nonzero(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(tmp_path / "nonexistent.hoot")],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "not found" in result.stderr


def test_cli_rejects_non_hoot_file(tmp_path: Path) -> None:
    csv_file = tmp_path / "data.csv"
    csv_file.touch()
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(csv_file)],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "expected .hoot" in result.stderr


def test_cli_produces_pdf_from_real_data(tmp_path: Path) -> None:
    if not HOOT_FILES:
        import pytest
        pytest.skip("no .hoot files present in data/")
    output = tmp_path / "report.pdf"
    result = subprocess.run(
        [sys.executable, "-m", "utils", *[str(f) for f in HOOT_FILES],
         "--output", str(output)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.exists()
    assert output.stat().st_size > 0


def test_cli_per_motor_flag_accepted(tmp_path: Path) -> None:
    if not HOOT_FILES:
        import pytest
        pytest.skip("no .hoot files present in data/")
    output = tmp_path / "report_pm.pdf"
    result = subprocess.run(
        [sys.executable, "-m", "utils", *[str(f) for f in HOOT_FILES],
         "--output", str(output), "--per-motor-graphs"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.exists()


def test_cli_cache_dir_arg_is_recognized(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable, "-m", "utils",
            str(tmp_path / "nonexistent.hoot"),
            "--cache-dir", str(tmp_path / "cache"),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "not found" in result.stderr
    assert "unrecognized" not in result.stderr


def test_hoot_dir_and_positional_files_are_mutually_exclusive(tmp_path: Path) -> None:
    hoot_dir = tmp_path / "hoots"
    hoot_dir.mkdir()
    dummy = tmp_path / "dummy.hoot"
    dummy.touch()
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         str(dummy), "--hoot-dir", str(hoot_dir),
         "--site", str(tmp_path / "site")],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "mutually exclusive" in result.stderr


def test_hoot_dir_nonexistent_exits_nonzero(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         "--hoot-dir", str(tmp_path / "no-such-dir"),
         "--site", str(tmp_path / "site")],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "not a directory" in result.stderr


def test_hoot_dir_requires_site(tmp_path: Path) -> None:
    hoot_dir = tmp_path / "hoots"
    hoot_dir.mkdir()
    result = subprocess.run(
        [sys.executable, "-m", "utils", "--hoot-dir", str(hoot_dir)],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "--site" in result.stderr


def test_hoot_dir_empty_dir_exits_nonzero(tmp_path: Path) -> None:
    hoot_dir = tmp_path / "hoots"
    hoot_dir.mkdir()
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         "--hoot-dir", str(hoot_dir),
         "--site", str(tmp_path / "site")],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "no .hoot files" in result.stderr


def test_clean_removes_existing_json(tmp_path: Path) -> None:
    if not HOOT_FILES:
        import pytest
        pytest.skip("no .hoot files present in data/")
    site_dir = tmp_path / "site"
    data_dir = site_dir / "data"
    data_dir.mkdir(parents=True)
    stale = data_dir / "STALE_Q99.json"
    stale.write_text('{"match_id":"STALE_Q99"}')
    hoot_dir = tmp_path / "hoots"
    hoot_dir.mkdir()
    for f in HOOT_FILES:
        shutil.copy(f, hoot_dir / f.name)
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         "--hoot-dir", str(hoot_dir),
         "--site", str(site_dir),
         "--clean"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert not stale.exists(), "stale JSON should have been removed by --clean"


def test_hoot_dir_merge_preserves_existing_json(tmp_path: Path) -> None:
    if not HOOT_FILES:
        import pytest
        pytest.skip("no .hoot files present in data/")
    site_dir = tmp_path / "site"
    data_dir = site_dir / "data"
    data_dir.mkdir(parents=True)
    preserved = data_dir / "PREV_Q1.json"
    preserved.write_text('{"match_id":"PREV_Q1","duration":90.0,"timestamps":[],"motors":{},"totals":{}}')
    hoot_dir = tmp_path / "hoots"
    hoot_dir.mkdir()
    for f in HOOT_FILES:
        shutil.copy(f, hoot_dir / f.name)
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         "--hoot-dir", str(hoot_dir),
         "--site", str(site_dir)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert preserved.exists(), "pre-existing JSON should be preserved without --clean"


def test_clean_without_site_emits_warning(tmp_path: Path) -> None:
    if not HOOT_FILES:
        import pytest
        pytest.skip("no .hoot files present in data/")
    result = subprocess.run(
        [sys.executable, "-m", "utils",
         *[str(f) for f in HOOT_FILES],
         "--output", str(tmp_path / "report.pdf"),
         "--clean"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "no effect without --site" in result.stderr
