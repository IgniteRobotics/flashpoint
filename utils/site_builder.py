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


def _serialize_motor(data: MotorData, name: str) -> dict:
    d: dict = {
        "name": name,
        "motor_voltage":  data.motor_voltage.tolist(),
        "stator_current": data.stator_current.tolist(),
        "motor_power":    data.motor_power.tolist(),
        "motor_energy":   data.motor_energy.tolist(),
        "stats": _motor_stats(data),
    }
    for attr in ("supply_voltage", "supply_current", "supply_power", "supply_energy",
                 "rotor_velocity", "device_temp"):
        val = getattr(data, attr)
        if val is not None:
            d[attr] = val.tolist()
    return d


def serialize_match(
    match: Match,
    motor_names: dict[str, str] | None,
) -> dict:
    """Convert a Match to a plain JSON-serializable dict."""
    motors = {
        mid: _serialize_motor(data, (motor_names or {}).get(mid, mid))
        for mid, data in match.motors.items()
    }

    totals: dict = {
        "motor_power":  match.totals.motor_power.tolist(),
        "motor_energy": match.totals.motor_energy.tolist(),
    }
    for attr in ("supply_power", "supply_current", "supply_energy"):
        val = getattr(match.totals, attr)
        if val is not None:
            totals[attr] = val.tolist()

    return {
        "match_id":   match.match_id,
        "duration":   float(match.timestamps[-1]),
        "timestamps": match.timestamps.tolist(),
        "motors":     motors,
        "totals":     totals,
    }


_TEMPLATE_PATH: Path | None = Path(__file__).parent / "templates" / "index.html"
_LOGO_PATH: Path | None = Path(__file__).parent.parent / "media" / "logo.png"


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
    index = site_dir / "index.html"
    if index.exists():
        return
    if _TEMPLATE_PATH and _TEMPLATE_PATH.exists():
        shutil.copy(_TEMPLATE_PATH, index)
    if _LOGO_PATH and _LOGO_PATH.exists():
        shutil.copy(_LOGO_PATH, site_dir / "logo.png")
