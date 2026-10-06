"""Views settings from `config/report.toml`: thresholds (rule markers, health, reference lines)
and the optional AdvantageScope path."""

import tomllib
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any


class ReportConfigError(Exception):
    pass


@dataclass(frozen=True)
class ReportConfig:
    temp_warn_c: float = 65.0
    temp_fault_c: float = 75.0
    brownout_v: float = 6.75  # battery proxy below this, held brownout_hold_s: FAULT
    brownout_hold_s: float = 0.02
    sag_v: float = 8.0  # battery proxy below this: WARN
    voltage_danger_v: float = 7.0  # History's min-voltage reference line
    sample_gap_s: float = 1.0  # a slot silent this long during the match: WARN


_TABLES = {"thresholds", "advantagescope"}


def _load(config_dir: Path) -> dict[str, Any]:
    path = config_dir / "report.toml"
    if not path.is_file():
        return {}
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise ReportConfigError(f"{path.name}: {exc}") from exc


def load_report_config(config_dir: Path) -> ReportConfig:
    data = _load(config_dir)
    path = config_dir / "report.toml"
    values = data.get("thresholds", {})
    known = {f.name for f in fields(ReportConfig)}
    unknown = sorted(set(values) - known) + sorted(set(data) - _TABLES)
    if unknown:
        raise ReportConfigError(f"{path.name}: unknown keys {unknown}")
    try:
        return ReportConfig(**{k: float(v) for k, v in values.items()})
    except (TypeError, ValueError) as exc:
        raise ReportConfigError(f"{path.name}: {exc}") from exc


def advantagescope_path(config_dir: Path) -> Path | None:
    """`[advantagescope] path`, with `~` expanded; None when unset or empty."""
    table = _load(config_dir).get("advantagescope", {})
    unknown = sorted(set(table) - {"path"})
    if unknown:
        raise ReportConfigError(f"report.toml: unknown keys {unknown} in [advantagescope]")
    value = table.get("path", "")
    if not isinstance(value, str):
        raise ReportConfigError("report.toml: [advantagescope] path must be a string")
    return Path(value).expanduser() if value else None
