"""Per-log provenance extracted from inside logs: FMS, build info, clock anchor, CAN inventory."""

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from flashpoint.readers.wpilog import CatalogEntry, WpilogLog, WpilogReader

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
# Bump when extraction reads something new: derive re-reads older wpilogs that lack a match number
# or a UTC end (log-metadata "Wall-clock anchor"). v2: AdvantageKit names and anchor.
META_VERSION = 2
MATCH_TYPE_NAMES = {"practice": 1, "qualification": 2, "elimination": 3}

# AdvantageKit names (mvp-outside-team-logs design §1), checked against AdvantageKit v26.0.2's
# source: LoggedDriverStation, LoggedSystemStats and WPILOGWriter write these. MatchType and
# AllianceStation are integers (AllianceStation: 1-3 red, 4-6 blue, 0 unknown). Not yet seen in
# a real outside log.
AKIT_DS_PREFIX = "/DriverStation/"
AKIT_DS_FIELDS = ("EventName", "MatchNumber", "MatchType", "ReplayNumber", "AllianceStation")
AKIT_EPOCH_ENTRY = "/SystemStats/EpochTimeMicros"
# akit_<yy-MM-dd_HH-mm-ss | random hex>[_<event, lower-cased>][_<p|q|e><n>].wpilog. The time is
# the JVM's local time, which is UTC on a roboRIO. The event is optional and lazy, so a bare
# `_q7` reads as the match.
_AKIT_WPILOG_NAME = re.compile(
    r"^akit_(?:(?P<stamp>\d{2}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})|[0-9a-f]+)"
    r"(?:_(?P<event>[A-Za-z0-9]+?))??(?:_(?P<mtype>[PQEpqe])(?P<mnum>\d+))?\.wpilog$"
)

_WPILOG_NAME = re.compile(
    r"^FRC_(?P<date>\d{8})_(?P<time>\d{6})(?:_(?P<event>[A-Za-z0-9]+)_(?P<mtype>[PQE])(?P<mnum>\d+))?"
    r"\.wpilog$"
)
_HOOT_NAME = re.compile(
    r"^(?:(?P<event>[A-Za-z0-9]+)_(?P<match>[PQE]\d+)_)?(?P<bus>rio|[0-9A-Fa-f]{32})_"
    r"(?P<stamp>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})(?:\..*)?\.hoot$"
)


EMPTY_METADATA_FRAME = pl.DataFrame(
    schema={
        "signal": pl.String,
        "type": pl.String,
        "ts_us": pl.Int64,
        "v_f64": pl.Float64,
        "v_i64": pl.Int64,
        "v_bool": pl.Boolean,
        "v_str": pl.String,
        "v_bytes": pl.Binary,
    }
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
    fms_origin: str | None = None  # fms-info | driver-station
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
        return _parse_akit_filename(name)
    utc = datetime.strptime(m["date"] + m["time"], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    number = int(m["mnum"]) if m["mnum"] else None
    return WpilogName(utc, m["event"], m["mtype"], number)


def _parse_akit_filename(name: str) -> WpilogName:
    m = _AKIT_WPILOG_NAME.match(name)
    if not m:
        return WpilogName()
    utc = (
        datetime.strptime(m["stamp"], "%y-%m-%d_%H-%M-%S").replace(tzinfo=UTC)
        if m["stamp"]
        else None
    )
    event = m["event"].upper() if m["event"] else None
    mtype = m["mtype"].upper() if m["mtype"] else None
    return WpilogName(utc, event, mtype, int(m["mnum"]) if m["mnum"] else None)


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
        or (name.startswith(AKIT_DS_PREFIX) and name[len(AKIT_DS_PREFIX) :] in AKIT_DS_FIELDS)
        or (name.startswith("MetaData") and entry.type == "string")
        or (name in ("systemTime", AKIT_EPOCH_ENTRY) and entry.type == "int64")
        or name == INVENTORY_ENTRY
    )


def _last_non_empty(frame: pl.DataFrame, column: str) -> Any:
    values = frame.get_column(column).drop_nulls()
    if values.dtype == pl.String:
        values = values.filter(values != "")
    return values[-1] if len(values) else None


def _signals(frame: pl.DataFrame, predicate: pl.Expr) -> pl.DataFrame:
    return frame.filter(predicate).sort("ts_us")


def _match_type(number: Any, name: Any) -> int | None:
    """A known match type, from its number (1-3) or its name; anything else is absent."""
    if number in MATCH_TYPE_NAMES.values():
        return int(number)
    if isinstance(name, str):
        return MATCH_TYPE_NAMES.get(name.strip().lower())
    return None


def _fields(frame: pl.DataFrame) -> dict[str, pl.DataFrame]:
    frame = frame.with_columns(pl.col("name").str.split("/").list.last().alias("field"))
    return {key: group for (key,), group in frame.group_by("field", maintain_order=True)}


def _value(by_field: dict[str, pl.DataFrame], name: str, column: str) -> Any:
    group = by_field.get(name)
    return None if group is None else _last_non_empty(group, column)


def _number(by_field: dict[str, pl.DataFrame], name: str) -> int | None:
    # Zero means "not set" for FMS integers (no FMS attached, or a non-match log).
    number = _value(by_field, name, "v_i64")
    return int(number) if number else None


def _fms_info(fms: pl.DataFrame, meta: WpilogMetadata) -> None:
    by_field = _fields(fms)
    meta.fms_event = _value(by_field, "EventName", "v_str")
    meta.fms_red_alliance = _value(by_field, "IsRedAlliance", "v_bool")
    meta.fms_match_number = _number(by_field, "MatchNumber")
    meta.fms_match_type = _match_type(_number(by_field, "MatchType"), None)
    meta.fms_replay = _number(by_field, "ReplayNumber")
    meta.fms_station = _number(by_field, "StationNumber")
    meta.fms_origin = "fms-info"


def _driver_station(ds: pl.DataFrame, meta: WpilogMetadata) -> None:
    """AdvantageKit's /DriverStation set. A value of the wrong type reads null, so absent."""
    by_field = _fields(ds)
    meta.fms_event = _value(by_field, "EventName", "v_str")
    meta.fms_match_number = _number(by_field, "MatchNumber")
    meta.fms_match_type = _match_type(
        _number(by_field, "MatchType"), _value(by_field, "MatchType", "v_str")
    )
    meta.fms_replay = _number(by_field, "ReplayNumber")
    station = _number(by_field, "AllianceStation")
    if station is not None and 1 <= station <= 6:
        meta.fms_red_alliance = station <= 3
        meta.fms_station = (station - 1) % 3 + 1
    meta.fms_origin = "driver-station"


_FMS_ATTRS = (
    "fms_event", "fms_match_type", "fms_match_number", "fms_replay", "fms_red_alliance",
    "fms_station", "fms_origin",
)  # fmt: skip


def _fms(frame: pl.DataFrame, meta: WpilogMetadata) -> None:
    """FMSInfo when it gives a match number, else /DriverStation as a whole set (never mixed)."""
    fms = _signals(frame, pl.col("name").str.contains(r"FMSInfo/[A-Za-z]+$"))
    ds = _signals(frame, pl.col("name").str.starts_with(AKIT_DS_PREFIX))
    if not fms.is_empty():
        _fms_info(fms, meta)
        if meta.fms_match_number is not None or ds.is_empty():
            return
        for name in _FMS_ATTRS:
            setattr(meta, name, None)
    if not ds.is_empty():
        _driver_station(ds, meta)


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
    epoch = _signals(frame, pl.col("name") == AKIT_EPOCH_ENTRY).filter(
        pl.col("v_i64") > _MIN_PLAUSIBLE_UTC_US
    )
    anchored = next(
        ((source, rows) for source, rows in (("systemTime", sync), ("advantagekit-epoch", epoch))
         if not rows.is_empty()),
        None,
    )  # fmt: skip
    if anchored is not None:
        source, rows = anchored
        offsets = (rows.get_column("v_i64") - rows.get_column("ts_us")).to_numpy()
        meta.utc_offset_us = int(np.median(offsets))
        meta.anchor_source = source
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


def read_metadata(path: Path, filename: str) -> WpilogMetadata:
    """Metadata from a stored wpilog, streamed window by window (the re-read of older lakes)."""
    reader = WpilogReader(path)
    frames: list[pl.DataFrame] = []
    span: tuple[int, int] | None = None
    for chunk in reader:
        chunk_span = chunk.ts_range()
        if chunk_span is not None:
            span = (
                (min(span[0], chunk_span[0]), max(span[1], chunk_span[1])) if span else chunk_span
            )
        wanted = chunk.subset(is_metadata_entry)
        if len(wanted):
            frames.append(
                wanted.samples().with_columns(
                    pl.col("signal").cast(pl.String), pl.col("type").cast(pl.String)
                )
            )
    frame = pl.concat(frames) if frames else EMPTY_METADATA_FRAME
    return metadata_from_frame(frame, span, filename)


def logs_columns(meta: WpilogMetadata) -> dict[str, Any]:
    """The ledger `logs` columns that come from a wpilog's metadata."""
    return {
        "fms_event": meta.fms_event, "fms_match_type": meta.fms_match_type,
        "fms_match_number": meta.fms_match_number, "fms_replay": meta.fms_replay,
        "fms_red_alliance": meta.fms_red_alliance, "fms_station": meta.fms_station,
        "fms_origin": meta.fms_origin,
        "file_event": meta.file.event, "file_match_type": meta.file.match_type,
        "file_match_number": meta.file.match_number, "project": meta.project,
        "build_date": meta.build.get("Build Date"), "commit_hash": meta.build.get("Commit Hash"),
        "git_branch": meta.git_branch, "git_dirty": meta.git_dirty,
        "utc_start": meta.utc_start.isoformat() if meta.utc_start else None,
        "utc_end": meta.utc_end.isoformat() if meta.utc_end else None,
        "anchor_source": meta.anchor_source, "utc_offset_us": meta.utc_offset_us,
        "meta_version": META_VERSION,
    }  # fmt: skip
