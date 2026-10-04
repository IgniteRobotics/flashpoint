"""Per-log provenance extracted from inside logs: FMS, build info, clock anchor, CAN inventory."""

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import numpy as np
import polars as pl

from flashpoint.readers.wpilog import CatalogEntry, WpilogLog

INVENTORY_ENTRY = "/Flashpoint/CANInventory"
INVENTORY_SCHEMA = 1
_INVENTORY_DEVICE_KEYS = ("model", "bus", "id", "serial")
_FMS_FIELDS = (
    "EventName",
    "MatchNumber",
    "MatchType",
    "ReplayNumber",
    "IsRedAlliance",
    "StationNumber",
)
_MIN_PLAUSIBLE_UTC_US = int(datetime(2020, 1, 1, tzinfo=UTC).timestamp() * 1_000_000)

_WPILOG_NAME = re.compile(
    r"^FRC_(?P<date>\d{8})_(?P<time>\d{6})(?:_(?P<event>[A-Za-z0-9]+)_(?P<mtype>[PQE])(?P<mnum>\d+))?"
    r"\.wpilog$"
)
_HOOT_NAME = re.compile(
    r"^(?:(?P<event>[A-Za-z0-9]+)_(?P<match>[PQE]\d+)_)?(?P<bus>rio|[0-9A-Fa-f]{32})_"
    r"(?P<stamp>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})(?:\..*)?\.hoot$"
)


@dataclass(frozen=True)
class WpilogName:
    utc: datetime | None = None
    event: str | None = None
    match_type: str | None = None
    match_number: int | None = None


@dataclass(frozen=True)
class HootName:
    bus: str | None = None
    session_stamp: str | None = None
    event: str | None = None
    match: str | None = None


@dataclass(frozen=True)
class InventoryRecord:
    ts_us: int
    payload: str
    valid: bool
    error: str | None


@dataclass
class WpilogMetadata:
    fms_event: str | None = None
    fms_match_type: int | None = None
    fms_match_number: int | None = None
    fms_replay: int | None = None
    fms_red_alliance: bool | None = None
    fms_station: int | None = None
    build: dict[str, str] = field(default_factory=dict)
    anchor_source: str = "none"
    utc_offset_us: int | None = None
    utc_start: datetime | None = None
    utc_end: datetime | None = None
    season: str = "unknown"
    file: WpilogName = field(default_factory=WpilogName)
    inventory: list[InventoryRecord] = field(default_factory=list)

    @property
    def project(self) -> str | None:
        return self.build.get("Project Name")

    @property
    def git_branch(self) -> str | None:
        return self.build.get("Git Branch")

    @property
    def git_dirty(self) -> str | None:
        return self.build.get("GitDirty")

    @property
    def inventory_status(self) -> str:
        return "present" if self.inventory else "absent"


def parse_wpilog_filename(name: str) -> WpilogName:
    m = _WPILOG_NAME.match(name)
    if not m:
        return WpilogName()
    utc = datetime.strptime(m["date"] + m["time"], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    number = int(m["mnum"]) if m["mnum"] else None
    return WpilogName(utc, m["event"], m["mtype"], number)


def parse_hoot_filename(name: str) -> HootName:
    m = _HOOT_NAME.match(name)
    if not m:
        return HootName()
    return HootName(m["bus"], m["stamp"], m["event"], m["match"])


def is_metadata_entry(entry: CatalogEntry) -> bool:
    """Catalog entries metadata extraction reads (a tiny fraction of any log)."""
    name = entry.name
    return (
        (name.rsplit("/", 2)[-2:-1] == ["FMSInfo"] and name.endswith(_FMS_FIELDS))
        or (name.startswith("MetaData") and entry.type == "string")
        or (name == "systemTime" and entry.type == "int64")
        or name == INVENTORY_ENTRY
    )


def _last_non_empty(frame: pl.DataFrame, column: str) -> Any:
    values = frame.get_column(column).drop_nulls()
    if values.dtype == pl.String:
        values = values.filter(values != "")
    return values[-1] if len(values) else None


def _signals(frame: pl.DataFrame, predicate: pl.Expr) -> pl.DataFrame:
    return frame.filter(predicate).sort("ts_us")


def _fms(frame: pl.DataFrame, meta: WpilogMetadata) -> None:
    fms = _signals(frame, pl.col("name").str.contains(r"FMSInfo/[A-Za-z]+$"))
    if fms.is_empty():
        return
    fms = fms.with_columns(pl.col("name").str.split("/").list.last().alias("field"))
    by_field = {key: group for (key,), group in fms.group_by("field", maintain_order=True)}

    def value(name: str, column: str) -> Any:
        group = by_field.get(name)
        return None if group is None else _last_non_empty(group, column)

    meta.fms_event = value("EventName", "v_str")
    meta.fms_red_alliance = value("IsRedAlliance", "v_bool")
    for attr, name in (
        ("fms_match_number", "MatchNumber"),
        ("fms_match_type", "MatchType"),
        ("fms_replay", "ReplayNumber"),
        ("fms_station", "StationNumber"),
    ):
        number = value(name, "v_i64")
        # Zero means "not set" for FMS integers (no FMS attached, or a non-match log).
        setattr(meta, attr, int(number) if number else None)


def _build_info(frame: pl.DataFrame, meta: WpilogMetadata) -> None:
    rows = _signals(frame, pl.col("name").str.starts_with("MetaData"))
    for text in rows.get_column("v_str").drop_nulls().to_list():
        key, sep, val = text.partition(": ")  # split on the first ": " only
        if sep:
            meta.build[key.strip()] = val.strip()


def _anchor(frame: pl.DataFrame, span: tuple[int, int] | None, meta: WpilogMetadata) -> None:
    sync = _signals(frame, pl.col("name") == "systemTime").filter(
        pl.col("v_i64") > _MIN_PLAUSIBLE_UTC_US
    )
    if not sync.is_empty():
        offsets = (sync.get_column("v_i64") - sync.get_column("ts_us")).to_numpy()
        meta.utc_offset_us = int(np.median(offsets))
        meta.anchor_source = "systemTime"
    elif meta.file.utc is not None:
        meta.anchor_source = "filename"
        meta.utc_start = meta.file.utc
    if meta.utc_offset_us is not None and span is not None:
        meta.utc_start = datetime.fromtimestamp((span[0] + meta.utc_offset_us) / 1e6, UTC)
        meta.utc_end = datetime.fromtimestamp((span[1] + meta.utc_offset_us) / 1e6, UTC)
    when = meta.utc_start or meta.file.utc
    meta.season = str(when.year) if when else "unknown"


def validate_inventory(payload: str) -> tuple[bool, str | None]:
    try:
        data = json.loads(payload)
    except ValueError:
        return False, "malformed: not JSON"
    if not isinstance(data, dict) or data.get("schema") != INVENTORY_SCHEMA:
        return False, "malformed: schema is not 1"
    if "error" in data:
        return False, str(data["error"])
    devices = data.get("devices")
    if not isinstance(devices, list):
        return False, "malformed: devices is not a list"
    for device in devices:
        if not isinstance(device, dict) or any(k not in device for k in _INVENTORY_DEVICE_KEYS):
            return False, "malformed: device missing model/bus/id/serial"
    return True, None


def _inventory(frame: pl.DataFrame, meta: WpilogMetadata) -> None:
    rows = _signals(frame, pl.col("name") == INVENTORY_ENTRY)
    for ts_us, payload in rows.select("ts_us", "v_str").iter_rows():
        if payload is None:
            continue
        valid, error = validate_inventory(payload)
        meta.inventory.append(InventoryRecord(int(ts_us), payload, valid, error))


def metadata_from_frame(
    frame: pl.DataFrame, span: tuple[int, int] | None, filename: str
) -> WpilogMetadata:
    """Build metadata from samples of `is_metadata_entry` signals (any order) and the log's span."""
    frame = frame.with_columns(pl.col("signal").cast(pl.String).alias("name"))
    meta = WpilogMetadata(file=parse_wpilog_filename(filename))
    _fms(frame, meta)
    _build_info(frame, meta)
    _anchor(frame, span, meta)
    _inventory(frame, meta)
    return meta


def wpilog_metadata(log: WpilogLog, filename: str) -> WpilogMetadata:
    return metadata_from_frame(log.subset(is_metadata_entry).samples(), log.ts_range(), filename)
