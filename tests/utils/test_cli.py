from __future__ import annotations

import subprocess
import sys
from pathlib import Path


DATA_DIR = Path(__file__).parent.parent.parent / "data"
RIO_CSV = DATA_DIR / "GACMP_E5_rio_filtered.csv"
CARNIVORE_CSV = DATA_DIR / "GACMP_E5_6E9415C3394C485320202050101C18FF_filtered.csv"


def test_cli_missing_file_exits_nonzero(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(tmp_path / "nonexistent.csv")],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "not found" in result.stderr


def test_cli_produces_pdf_from_real_data(tmp_path: Path) -> None:
    if not RIO_CSV.exists():
        import pytest
        pytest.skip("real data files not present")
    output = tmp_path / "report.pdf"
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(RIO_CSV), str(CARNIVORE_CSV),
         "--output", str(output)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.exists()
    assert output.stat().st_size > 0


def test_cli_per_motor_flag_accepted(tmp_path: Path) -> None:
    if not RIO_CSV.exists():
        import pytest
        pytest.skip("real data files not present")
    output = tmp_path / "report_pm.pdf"
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(RIO_CSV), str(CARNIVORE_CSV),
         "--output", str(output), "--per-motor-graphs"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.exists()


def test_cli_motor_names_flag_accepted(tmp_path: Path) -> None:
    if not RIO_CSV.exists():
        import pytest
        pytest.skip("real data files not present")
    config = tmp_path / "names.toml"
    config.write_text('[default]\n1 = "FL Drive"\n2 = "FR Drive"\n')
    output = tmp_path / "report_named.pdf"
    result = subprocess.run(
        [sys.executable, "-m", "utils", str(RIO_CSV), str(CARNIVORE_CSV),
         "--output", str(output), "--motor-names", str(config)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert output.exists()
