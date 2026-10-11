import json
import os
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.anomaly import power
from flashpoint.report import markers
from flashpoint.report.build import ReportBuilder
from flashpoint.report.paths import data_dir
from flashpoint.report.settings import ReportConfig, load_report_config
from flashpoint.semantics.silver import silver_dir
from tests.report.lake import MatchLake, quiet_rows
from tests.views.season import zoom_rows


@pytest.fixture
def lake(tmp_path: Path) -> MatchLake:
    lake = MatchLake(tmp_path / "lake")
    lake.add_match("2026gacmp_qm7", "q7", zoom_rows())
    lake.add_match("2026gacmp_qm8", "q8", quiet_rows(hz=10))
    lake.add_match("2026gacmp_qm9", "q9", None)  # no aligned samples
    lake.write_meta()
    return lake


def _replay_brownouts(lake: MatchLake, key: str) -> int:
    builder = ReportBuilder(lake.lake, config.config_root())
    try:
        builder.build()
    finally:
        builder.close()
    text = (data_dir(lake.lake) / f"{key}.js").read_text(encoding="utf-8")
    data: Any = json.loads(text[text.index("(") + 1 : -3])
    return sum(1 for m in data["markers"] if m["kind"] == "brownout" and m["level"] == "FAULT")


def test_counts_equal_replay_markers(lake: MatchLake) -> None:
    result = power.brownout_counts(lake.lake, load_report_config(config.config_root()))
    q7, q8 = result.counts["2026gacmp_qm7"], result.counts["2026gacmp_qm8"]
    assert q7["brownouts"] == _replay_brownouts(lake, "2026gacmp_qm7") == 1
    assert q8["brownouts"] == _replay_brownouts(lake, "2026gacmp_qm8") == 0
    assert q7["robot"] == "2026-comp" and q7["lowest_v"] == pytest.approx(6.0)
    assert "2026gacmp_qm9" not in result.counts
    assert sorted(result.recounted) == ["2026gacmp_qm7", "2026gacmp_qm8"]
    assert (lake.root / "gold" / "anomalies" / "power_counts.parquet").is_file()


def test_only_changed_matches_are_recounted(lake: MatchLake) -> None:
    report = load_report_config(config.config_root())
    power.brownout_counts(lake.lake, report)
    assert power.brownout_counts(lake.lake, report).recounted == []

    part = next((silver_dir(lake.lake) / "season=2026" / "session_id=q8").glob("*.parquet"))
    stat = part.stat()
    os.utime(part, ns=(stat.st_atime_ns, stat.st_mtime_ns + 5_000_000_000))
    assert power.brownout_counts(lake.lake, report).recounted == ["2026gacmp_qm8"]

    lower = ReportConfig(brownout_v=5.0)
    result = power.brownout_counts(lake.lake, lower)
    assert sorted(result.recounted) == ["2026gacmp_qm7", "2026gacmp_qm8"]
    assert result.counts["2026gacmp_qm7"]["brownouts"] == 0


def test_one_match_silver_at_a_time(lake: MatchLake, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[Path] = []
    original = markers.power_markers

    def spy(con: Any, silver: Path, *args: Any, **kwargs: Any) -> Any:
        seen.append(silver)
        return original(con, silver, *args, **kwargs)

    monkeypatch.setattr(markers, "power_markers", spy)
    power.brownout_counts(lake.lake, load_report_config(config.config_root()))
    assert [p.name for p in seen] == ["session_id=q7", "session_id=q8"]


def test_no_meta_no_counts(tmp_path: Path) -> None:
    result = power.brownout_counts(MatchLake(tmp_path).lake, ReportConfig())
    assert result.counts == {} and result.recounted == []
