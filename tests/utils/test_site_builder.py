from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from utils.models import Match, MotorData
from utils.site_builder import ensure_index, serialize_match, update_manifest, write_match


def _make_motor(n: int = 10, has_supply: bool = True, has_temp: bool = True) -> MotorData:
    t = np.linspace(0, 2, n)
    return MotorData(
        motor_voltage=np.full(n, 12.0),
        stator_current=np.full(n, 10.0),
        motor_power=np.full(n, 120.0),
        motor_energy=np.linspace(0, 0.1, n),
        supply_voltage=np.full(n, 12.5) if has_supply else None,
        supply_current=np.full(n, 8.0) if has_supply else None,
        supply_power=np.full(n, 100.0) if has_supply else None,
        supply_energy=np.linspace(0, 0.08, n) if has_supply else None,
        rotor_velocity=np.full(n, 5.0) if has_supply else None,
        device_temp=np.full(n, 50.0) if has_temp else None,
    )


def _make_match() -> Match:
    motors = {
        "TalonFX-1": _make_motor(),
        "TalonFX-2": _make_motor(has_supply=False, has_temp=False),
    }
    totals = _make_motor()
    return Match(
        match_id="2024-FIN-Q1",
        timestamps=np.linspace(0, 2, 10),
        motors=motors,
        totals=totals,
    )


def test_serialize_match_top_level_fields():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    assert result["match_id"] == "2024-FIN-Q1"
    assert abs(result["duration"] - 2.0) < 0.01
    assert len(result["timestamps"]) == 10
    assert isinstance(result["timestamps"][0], float)


def test_serialize_match_motor_arrays_present():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    m1 = result["motors"]["TalonFX-1"]
    assert len(m1["motor_power"]) == 10
    assert len(m1["supply_current"]) == 10
    assert len(m1["device_temp"]) == 10


def test_serialize_match_optional_arrays_omitted_when_none():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    m2 = result["motors"]["TalonFX-2"]
    assert "supply_current" not in m2
    assert "supply_power" not in m2
    assert "device_temp" not in m2


def test_serialize_match_motor_name_used_when_provided():
    match = _make_match()
    names = {"TalonFX-1": "FR Drive", "TalonFX-2": "FL Drive"}
    result = serialize_match(match, motor_names=names)
    assert result["motors"]["TalonFX-1"]["name"] == "FR Drive"


def test_serialize_match_motor_id_used_as_name_when_no_map():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    assert result["motors"]["TalonFX-1"]["name"] == "TalonFX-1"


def test_serialize_match_stats_computed():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    stats = result["motors"]["TalonFX-1"]["stats"]
    assert abs(stats["peak_motor_w"] - 120.0) < 0.01
    assert abs(stats["avg_motor_v"] - 12.0) < 0.01
    assert abs(stats["avg_temp"] - 50.0) < 0.01


def test_serialize_match_stats_temp_none_when_no_temp():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    stats = result["motors"]["TalonFX-2"]["stats"]
    assert stats["avg_temp"] is None
    assert stats["max_temp"] is None


def test_serialize_match_stats_supply_none_when_no_supply():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    stats = result["motors"]["TalonFX-2"]["stats"]
    assert stats["peak_supply_w"] is None
    assert stats["max_supply_a"] is None


def test_serialize_match_totals_present():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    assert len(result["totals"]["motor_power"]) == 10
    assert len(result["totals"]["supply_power"]) == 10


def test_serialize_match_is_json_serializable():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    # Must not raise
    json.dumps(result)


def test_write_match_creates_json_file(tmp_path: Path):
    match = _make_match()
    data = serialize_match(match, motor_names=None)
    write_match(data, tmp_path)
    expected = tmp_path / "data" / "2024-FIN-Q1.json"
    assert expected.exists()
    loaded = json.loads(expected.read_text())
    assert loaded["match_id"] == "2024-FIN-Q1"


def test_write_match_overwrites_existing(tmp_path: Path):
    match = _make_match()
    data = serialize_match(match, motor_names=None)
    write_match(data, tmp_path)
    write_match(data, tmp_path)  # second write — must not raise
    assert (tmp_path / "data" / "2024-FIN-Q1.json").exists()


def test_update_manifest_lists_all_data_files(tmp_path: Path):
    (tmp_path / "data").mkdir()
    for name in ("2024-FIN-Q1", "2024-FIN-Q2"):
        (tmp_path / "data" / f"{name}.json").write_text(
            json.dumps({"match_id": name, "duration": 148.0, "n_motors": 8})
        )
    update_manifest(tmp_path)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    ids = {m["id"] for m in manifest}
    assert ids == {"2024-FIN-Q1", "2024-FIN-Q2"}


def test_update_manifest_entry_fields(tmp_path: Path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "2024-FIN-Q1.json").write_text(
        json.dumps({"match_id": "2024-FIN-Q1", "duration": 148.2, "motors": {"a": {}, "b": {}}})
    )
    update_manifest(tmp_path)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    entry = manifest[0]
    assert entry["id"] == "2024-FIN-Q1"
    assert abs(entry["duration"] - 148.2) < 0.01
    assert entry["n_motors"] == 2


def test_ensure_index_writes_html(tmp_path: Path, monkeypatch):
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    monkeypatch.setattr("utils.site_builder._PLOTLY_PATH", None)
    ensure_index(tmp_path)
    assert (tmp_path / "index.html").exists()
    assert "TEMPLATE" in (tmp_path / "index.html").read_text()


def test_ensure_index_does_not_overwrite(tmp_path: Path, monkeypatch):
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    monkeypatch.setattr("utils.site_builder._PLOTLY_PATH", None)
    (tmp_path / "index.html").write_text("EXISTING")
    ensure_index(tmp_path)
    assert (tmp_path / "index.html").read_text() == "EXISTING"


def test_ensure_index_copies_plotly(tmp_path: Path, monkeypatch):
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    plotly_path = tmp_path / "plotly.src.js"
    plotly_path.write_text("var Plotly={};")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    monkeypatch.setattr("utils.site_builder._PLOTLY_PATH", plotly_path)
    ensure_index(tmp_path)
    assert (tmp_path / "plotly.min.js").exists()
    assert (tmp_path / "plotly.min.js").read_text() == "var Plotly={};"


def test_ensure_index_overwrites_plotly(tmp_path: Path, monkeypatch):
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    plotly_path = tmp_path / "plotly.src.js"
    plotly_path.write_text("var Plotly={};")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    monkeypatch.setattr("utils.site_builder._PLOTLY_PATH", plotly_path)
    (tmp_path / "plotly.min.js").write_text("OLD")
    ensure_index(tmp_path)
    assert (tmp_path / "plotly.min.js").read_text() == "var Plotly={};"


def test_serialize_match_downsamples_high_frequency_data():
    n = 1000
    timestamps = np.linspace(0, 10, n)  # 10s at 100 Hz → step=2
    motor = MotorData(
        motor_voltage=np.full(n, 12.0),
        stator_current=np.full(n, 10.0),
        motor_power=np.full(n, 120.0),
        motor_energy=np.linspace(0, 1.0, n),
        supply_voltage=None,
        supply_current=None,
        supply_power=None,
        supply_energy=None,
        rotor_velocity=None,
        device_temp=None,
    )
    match = Match(
        match_id="test-downsample",
        timestamps=timestamps,
        motors={"TalonFX-1": motor},
        totals=motor,
    )
    result = serialize_match(match, motor_names=None)
    # 100 Hz input → step=2 → ~500 output samples
    assert len(result["timestamps"]) < n
    assert len(result["timestamps"]) == len(result["motors"]["TalonFX-1"]["motor_power"])


def test_serialize_match_duration_with_nonzero_start():
    match = _make_match()
    match = Match(
        match_id=match.match_id,
        timestamps=np.linspace(5.0, 7.0, 10),
        motors=match.motors,
        totals=match.totals,
    )
    result = serialize_match(match, motor_names=None)
    assert abs(result["duration"] - 2.0) < 0.01
