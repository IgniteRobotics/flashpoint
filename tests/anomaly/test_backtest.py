import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from flashpoint import config
from flashpoint.anomaly import backtest, detect
from flashpoint.cli import main
from flashpoint.lake.ledger import Ledger
from flashpoint.lake.paths import LakePaths
from tests.anomaly.gold_builder import GoldLake, steady

JOHNSON = ["2026johnson_qm15", "2026johnson_qm53", "2026johnson_qm76", "2026johnson_qm105"]


def _config(tmp_path: Path, labels: str | None = None) -> Path:
    target = tmp_path / "config"
    if not target.exists():
        shutil.copytree(config.config_root(), target)
    if labels is not None:
        (target / "failure-labels.toml").write_text(labels)
    return target


def _johnson(root: Path) -> LakePaths:
    gold = GoldLake(root, matches=4, slots=["intake-roller", "intake-extension-follower",
                                            "intake-extension"])  # fmt: skip
    gold.keys = JOHNSON
    gold.set("intake-roller", "reboots", [0, 0, 0, 1])
    gold.set("intake-roller", "reboots_enabled", [0, 0, 0, 1])
    gold.set("intake-roller", "longest_gap_ms", [26.0, 0.0, 59.0, 820.0])
    gold.set("intake-extension-follower", "isolated_gaps", [7, 20, 1, 42])
    return gold.write()


def _season(root: Path, matches: int = 30) -> GoldLake:
    gold = GoldLake(root, matches=matches, slots=["hood", "drive-fl", "drive-fr", "drive-bl",
                                                  "drive-br"])  # fmt: skip
    gold.set("hood", "temp_max_c", steady(45.0, 12) + steady(56.0, matches - 12))
    temps = [40.0] * matches
    temps[8] = 66.0
    temps[24] = 70.0
    gold.set("drive-fl", "temp_max_c", temps)
    gold.set("drive-bl", "stator_current_p95",
             steady(40.0, 15) + [40.0 + 0.8 * k for k in range(1, matches - 14)])  # fmt: skip
    return gold


def test_q105_caught_and_degradation_needs_history(tmp_path: Path) -> None:
    lake = _johnson(tmp_path / "lake")
    result = backtest.backtest(lake, _config(tmp_path))
    labels = {
        (lb["slot"], lb["match"]): lb for lb in result.report["sensitivities"]["normal"]["labels"]
    }
    roller = labels[("intake-roller", "2026johnson_qm105")]
    assert (roller["result"], roller["lead_matches"], roller["method"]) == ("caught", 0, "SPEC")
    follower = labels[("intake-extension-follower", "2026johnson_qm105")]
    assert follower["result"] == "missed"
    assert follower["reason"] == "building baseline (3 of 5)"
    assert len(labels) == 2


def test_label_outside_the_lake_is_not_evaluable(tmp_path: Path) -> None:
    lake = _johnson(tmp_path / "lake")
    absent = (
        '[[label]]\nrobot = "2026-comp"\nslot = "power"\nmatch = "2026gacmp_qm7"\n'
        'kind = "failure"\nmetric = "brownouts"\nnote = "x"\n'
    )
    result = backtest.backtest(lake, _config(tmp_path, labels=absent))
    (label,) = result.report["sensitivities"]["normal"]["labels"]
    assert label["result"] == "not evaluable" and "not in the lake" in label["reason"]


def test_equals_live_detection(tmp_path: Path) -> None:
    gold = _season(tmp_path / "full")
    full = gold.write()
    result = backtest.backtest(full, _config(tmp_path, labels=""))
    keys = [gold.match_key(i) for i in range(20)]
    as_of = {a["id"] for a in result.report["appearances"] if a["appeared_at"] in keys}
    short = GoldLake(tmp_path / "short", matches=30, slots=gold.slots, series=gold.series)
    short.write(upto=20)
    settings = detect.load_settings(_config(tmp_path))
    live = {r["id"] for r in detect.compute(LakePaths(short.root), settings).rows}
    assert as_of and as_of == live


def test_triage_ignored(tmp_path: Path) -> None:
    lake = _season(tmp_path / "lake").write()
    cfg = _config(tmp_path, labels="")
    before = backtest.backtest(lake, cfg).json_text
    step = next(r for r in detect.compute(lake, detect.load_settings(cfg)).rows
                if r["method"] == "STEP")  # fmt: skip
    ledger = Ledger(lake.ledger)
    try:
        ledger.add_triage({
            "anomaly_id": step["id"], "from_state": "new", "to_state": "dismissed",
            "outcome": "expected", "restart_baseline": True, "subject": step["subject_key"],
            "metric": step["metric"], "onset_match_key": step["onset_match_key"],
        })  # fmt: skip
    finally:
        ledger.close()
    assert backtest.backtest(lake, cfg).json_text == before


def test_reproducible_json(tmp_path: Path) -> None:
    lake = _johnson(tmp_path / "lake")
    cfg = _config(tmp_path)
    first, second = backtest.backtest(lake, cfg), backtest.backtest(lake, cfg)
    assert first.json_path != second.json_path or first.json_path.exists()
    assert first.json_path.read_bytes() == second.json_path.read_bytes()
    assert first.json_text == json.dumps(json.loads(first.json_text), sort_keys=True, indent=1)
    assert first.md_path.read_text().startswith("# Backtest")
    assert first.json_path.parent == lake.root / "report" / "backtest"


def test_false_positives_per_method_and_sensitivity(tmp_path: Path) -> None:
    gold = _season(tmp_path / "lake")
    lake = gold.write()
    labels = (
        '[[label]]\nrobot = "2026-comp"\nslot = "drive-fl"\n'
        f'match = "{gold.match_key(8)}"\nkind = "failure"\nmetric = "temp_max_c"\nnote = "hot"\n'
    )
    report = backtest.backtest(lake, _config(tmp_path, labels)).report
    normal = report["sensitivities"]["normal"]
    (label,) = normal["labels"]
    assert label["result"] == "caught" and label["lead_matches"] == 0
    fp = normal["false_positives"]
    assert fp["robot_matches"] == 30
    assert fp["by_method"]["SPEC"]["count"] == 1  # the second hot match matches no label
    assert fp["by_method"]["SPEC"]["per_10_robot_matches"] == pytest.approx(1 / 3)
    assert fp["by_method"]["STEP"]["count"] >= 1
    strict = report["sensitivities"]["strict"]["false_positives"]["total"]
    relaxed = report["sensitivities"]["relaxed"]["false_positives"]["total"]
    assert strict >= fp["total"] >= relaxed
    coverage = report["coverage"]
    assert coverage["matches_judged"] == 30 and coverage["units_building_baseline"] == []


def test_label_by_date(tmp_path: Path) -> None:
    lake = _johnson(tmp_path / "lake")
    labels = (
        '[[label]]\nrobot = "2026-comp"\nslot = "intake-roller"\ndate = "2026-03-06"\n'
        'kind = "failure"\nnote = "by date"\n'
    )
    (label,) = backtest.backtest(lake, _config(tmp_path, labels)).report["sensitivities"][
        "normal"]["labels"]  # fmt: skip
    assert label["match"] == "2026johnson_qm105"  # the day's last match
    assert label["result"] == "caught"


def test_empty_lake(tmp_path: Path) -> None:
    result = backtest.backtest(LakePaths(tmp_path / "lake"), _config(tmp_path))
    assert result.message is not None and "no gold" in result.message
    assert result.report["coverage"]["matches_judged"] == 0
    assert result.json_path.is_file()


def test_cli(tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: Any) -> None:
    lake = _johnson(tmp_path / "lake")
    monkeypatch.setenv(config.CONFIG_ENV, str(_config(tmp_path)))
    assert main(["backtest", "--lake", str(lake.root)]) == 0
    out = capsys.readouterr().out
    assert "caught 1, missed 1, not evaluable 0" in out
    assert str(Path("report") / "backtest") in out
    bad = _config(tmp_path, labels=(
        '[[label]]\nrobot = "2026-comp"\nslot = "drive-xx"\nmatch = "2026johnson_qm105"\n'
        'kind = "failure"\nnote = "x"\n'
    ))  # fmt: skip
    monkeypatch.setenv(config.CONFIG_ENV, str(bad))
    assert main(["backtest", "--lake", str(lake.root)]) == 2
    assert "drive-xx" in capsys.readouterr().err
