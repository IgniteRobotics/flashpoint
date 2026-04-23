from __future__ import annotations

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
