from __future__ import annotations

import hashlib
from pathlib import Path

from utils.hoot_loader import convert_hoot


def test_convert_hoot_returns_cache_path_on_hit(tmp_path: Path) -> None:
    hoot = tmp_path / "GACMP_Q1_rio_2025-01-01_00-00-00.hoot"
    hoot.write_bytes(b"fake hoot content")

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    h = hashlib.sha256(b"fake hoot content").hexdigest()
    cache_csv = cache_dir / f"{h}.csv"
    cache_csv.write_text(
        "Timestamp,Phoenix6/TalonFX-1/MotorVoltage\n0.0,5.0\n"
    )

    result = convert_hoot(hoot, cache_dir)

    assert result == cache_csv
