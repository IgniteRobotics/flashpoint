from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np

from .models import Match, MotorData


def _motor_stats(data: MotorData) -> dict:
    has_supply = data.supply_power is not None
    has_temp = data.device_temp is not None
    return {
        "peak_motor_w":  float(data.motor_power.max()),
        "p95_motor_w":   float(np.percentile(data.motor_power, 95)),
        "avg_motor_v":   float(data.motor_voltage.mean()),
        "max_motor_v":   float(data.motor_voltage.max()),
        "max_stator_a":  float(data.stator_current.max()),
        "avg_stator_a":  float(data.stator_current.mean()),
        "max_supply_v":  float(data.supply_voltage.max())   if has_supply else None,
        "avg_supply_v":  float(data.supply_voltage.mean())  if has_supply else None,
        "max_supply_a":  float(data.supply_current.max())   if has_supply else None,
        "avg_supply_a":  float(data.supply_current.mean())  if has_supply else None,
        "peak_supply_w": float(data.supply_power.max())     if has_supply else None,
        "p95_supply_w":  float(np.percentile(data.supply_power, 95)) if has_supply else None,
        "avg_temp":      float(data.device_temp.mean()) if has_temp else None,
        "max_temp":      float(data.device_temp.max())  if has_temp else None,
    }


_TARGET_HZ = 50


def _downsample(arr: np.ndarray, step: int) -> list:
    return arr[::step].tolist()


def _serialize_motor(data: MotorData, name: str, step: int) -> dict:
    d: dict = {
        "name": name,
        "motor_voltage":  _downsample(data.motor_voltage, step),
        "stator_current": _downsample(data.stator_current, step),
        "motor_power":    _downsample(data.motor_power, step),
        "motor_energy":   _downsample(data.motor_energy, step),
        "stats": _motor_stats(data),
    }
    for attr in ("supply_voltage", "supply_current", "supply_power", "supply_energy",
                 "rotor_velocity", "device_temp"):
        val = getattr(data, attr)
        if val is not None:
            d[attr] = _downsample(val, step)
    return d


def serialize_match(
    match: Match,
    motor_names: dict[str, str] | None,
) -> dict:
    """Convert a Match to a plain JSON-serializable dict, downsampled to ~50 Hz."""
    n = len(match.timestamps)
    duration = float(match.timestamps[-1] - match.timestamps[0])
    actual_hz = n / duration if duration > 0 else _TARGET_HZ
    step = max(1, round(actual_hz / _TARGET_HZ))

    motors = {
        mid: _serialize_motor(data, (motor_names or {}).get(mid, mid), step)
        for mid, data in match.motors.items()
    }

    totals: dict = {
        "motor_power":  _downsample(match.totals.motor_power, step),
        "motor_energy": _downsample(match.totals.motor_energy, step),
    }
    for attr in ("supply_power", "supply_current", "supply_energy"):
        val = getattr(match.totals, attr)
        if val is not None:
            totals[attr] = _downsample(val, step)

    return {
        "match_id":   match.match_id,
        "duration":   duration,
        "timestamps": _downsample(match.timestamps, step),
        "motors":     motors,
        "totals":     totals,
    }


_TEMPLATE_PATH: Path | None = Path(__file__).parent / "templates" / "index.html"
_LOGO_PATH: Path | None = Path(__file__).parent.parent / "media" / "logo.png"
_PLOTLY_PATH: Path | None = Path(__file__).parent / "static" / "plotly-2.35.2.min.js"


def write_match(match_dict: dict, site_dir: Path) -> None:
    data_dir = site_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out = data_dir / f"{match_dict['match_id']}.json"
    out.write_text(json.dumps(match_dict))


def update_manifest(site_dir: Path) -> None:
    data_dir = site_dir / "data"
    entries = []
    for f in sorted(data_dir.glob("*.json")):
        d = json.loads(f.read_text())
        if "match_id" not in d:
            continue
        entries.append({
            "id":       d["match_id"],
            "duration": d.get("duration", 0.0),
            "n_motors": len(d.get("motors", {})),
        })
    (site_dir / "manifest.json").write_text(json.dumps(entries))


def ensure_index(site_dir: Path) -> None:
    site_dir.mkdir(parents=True, exist_ok=True)
    if _PLOTLY_PATH and _PLOTLY_PATH.exists():
        shutil.copy(_PLOTLY_PATH, site_dir / "plotly.min.js")
    index = site_dir / "index.html"
    if index.exists():
        return
    if _TEMPLATE_PATH and _TEMPLATE_PATH.exists():
        shutil.copy(_TEMPLATE_PATH, index)
    if _LOGO_PATH and _LOGO_PATH.exists():
        shutil.copy(_LOGO_PATH, site_dir / "logo.png")
