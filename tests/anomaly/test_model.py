import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from flashpoint import config
from flashpoint.anomaly import detect, model
from flashpoint.anomaly.settings import AnomalyConfig, Model
from flashpoint.lake.paths import LakePaths
from tests.anomaly.gold_builder import GoldLake

DRIVETRAIN = ["drive-fl", "drive-fr", "drive-bl", "drive-br",
              "steer-fl", "steer-fr", "steer-bl", "steer-br"]  # fmt: skip
EVENTS, PER_EVENT = 4, 20


def _season(root: Path) -> LakePaths:
    rng = np.random.default_rng(5)
    n = EVENTS * PER_EVENT
    gold = GoldLake(root, matches=n, slots=DRIVETRAIN)
    gold.keys = [f"2026ev{i // PER_EVENT + 1}_qm{i % PER_EVENT + 1}" for i in range(n)]
    for slot in DRIVETRAIN:
        gold.set(slot, "temp_max_c", list(rng.normal(40, 1.5, n)))
        gold.set(slot, "stator_current_p95", list(rng.normal(30, 2, n)))
        gold.set(slot, "temp_rise_c_per_min", list(rng.normal(1, 0.2, n)))
    odd = 3 * PER_EVENT + 10  # event 4: one unit-match far off on several features at once
    for metric, shift in (("temp_max_c", 9.0), ("stator_current_p95", -12.0),
                          ("temp_rise_c_per_min", 2.0)):  # fmt: skip
        gold.series[("drive-fl", metric)][odd] += shift  # type: ignore[operator]
    return gold.write()


def _settings(enabled: bool) -> detect.Settings:
    base = detect.load_settings(config.config_root())
    anomaly = AnomalyConfig(model=Model(enabled=enabled))
    return detect.Settings(anomaly, base.robots, base.report, base.digest + str(enabled))


def test_off_by_default_does_not_import_sklearn(tmp_path: Path) -> None:
    lake = _season(tmp_path / "lake")
    code = (
        "import sys\n"
        "from flashpoint import config\n"
        "from flashpoint.anomaly import detect\n"
        "from pathlib import Path\n"
        "from flashpoint.lake.paths import LakePaths\n"
        f"rows = detect.compute(LakePaths(Path({str(lake.root)!r})),"
        " detect.load_settings(config.config_root())).rows\n"
        "assert not [r for r in rows if r['method'] == 'MODEL']\n"
        "print('sklearn' in sys.modules)\n"
    )
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                          check=True)  # fmt: skip
    assert done.stdout.strip() == "False"


def test_enabled_without_the_extra_warns_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lake = _season(tmp_path / "lake")
    monkeypatch.setattr(model, "_forest", lambda: None)
    without = detect.compute(lake, _settings(enabled=True))
    assert without.notes == [model.MISSING] and "ml" in model.MISSING
    assert without.rows == detect.compute(lake, _settings(enabled=False)).rows


def _model_rows(lake: LakePaths) -> list[dict[str, Any]]:
    return [r for r in detect.compute(lake, _settings(enabled=True)).rows if r["method"] == "MODEL"]


def test_flags_are_deterministic_capped_and_explained(tmp_path: Path) -> None:
    pytest.importorskip("sklearn")
    lake = _season(tmp_path / "lake")
    first, second = _model_rows(lake), _model_rows(lake)
    assert first and first == second
    assert {r["severity"] for r in first} <= {"LOW", "MED"}
    assert {r["sensitivity"] for r in first} == {"normal"}
    odd = [r for r in first if r["subject"] == "ctre:DRIVE-FL"
           and r["onset_match_key"] == "2026ev4_qm11"]  # fmt: skip
    assert odd, "the far-off unit-match is flagged"
    why = odd[0]["why"]
    for name in ("maximum temperature", "stator current P95", "temperature rise rate"):
        assert name in why


def test_training_uses_only_earlier_events(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("sklearn")
    lake = _season(tmp_path / "lake")
    fits: list[tuple[str, set[str]]] = []
    original = model._fit

    def spy(forest: Any, x: np.ndarray, contamination: float, keys: list[str], event: str) -> Any:
        fits.append((event, {k.split("_")[0] for k in keys}))
        return original(forest, x, contamination, keys, event)

    monkeypatch.setattr(model, "_fit", spy)
    _model_rows(lake)
    assert fits, "events 3 and 4 have enough training rows"
    for event, trained_on in fits:
        assert all(e < event for e in trained_on)
