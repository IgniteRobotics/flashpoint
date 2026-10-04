"""WPILib DataLog (.wpilog) v1.0 reader.

Record framing is scanned by compiled kernels over a memory-mapped file. That is about
40x faster than a per-record Python loop over the official reader, which the test
suite uses as an oracle. Control records (Start/Finish/SetMetadata) are rare and are
decoded in Python.

Spec: https://github.com/wpilibsuite/allwpilib/blob/main/datalog/doc/datalog.adoc
"""

import struct
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path

import numpy as np
import numpy.typing as npt
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from numba import njit

HEADER_MAGIC = b"WPILOG"
SUPPORTED_MAJOR = 0x01
HEADER_FIXED_SIZE = 12

CONTROL_START = 0
CONTROL_FINISH = 1
CONTROL_SET_METADATA = 2

KIND_F64 = 0
KIND_F32 = 1
KIND_I64 = 2
KIND_BOOL = 3
KIND_STR = 4
KIND_BYTES = 5

_TYPE_KIND = {
    "double": KIND_F64,
    "float": KIND_F32,
    "int64": KIND_I64,
    "boolean": KIND_BOOL,
    "string": KIND_STR,
    "json": KIND_STR,
}
_KIND_SIZE = {KIND_F64: 8, KIND_F32: 4, KIND_I64: 8, KIND_BOOL: 1}
_SCHEMA_PREFIX = "/.schema/"

U8Array = npt.NDArray[np.uint8]
I32Array = npt.NDArray[np.int32]
I64Array = npt.NDArray[np.int64]
U32Array = npt.NDArray[np.uint32]
BoolArray = npt.NDArray[np.bool_]


class WpilogError(Exception):
    """Unreadable log. `reason` is a stable, machine-readable quarantine code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


@dataclass
class CatalogEntry:
    """One Start record. Entry ids may be reused after Finish, so the catalog index is the key."""

    index: int
    entry_id: int
    name: str
    type: str
    metadata: str
    start_ts_us: int


# --- compiled kernels -------------------------------------------------------------------


@njit(cache=True)
def _read_le(buf: U8Array, pos: int, length: int) -> int:
    value = 0
    for i in range(length):
        value |= int(buf[pos + i]) << (8 * i)
    return value


@njit(cache=True)
def _scan(
    buf: U8Array, start: int, ent: U32Array, ts: I64Array, off: I64Array, sz: U32Array
) -> tuple[int, int]:
    """Walk record framing in one pass. Returns (records, end of the last complete record)."""
    n = len(buf)
    pos = start
    k = 0
    while pos < n:
        h = int(buf[pos])
        el = (h & 0x3) + 1
        sl = ((h >> 2) & 0x3) + 1
        tl = ((h >> 4) & 0x7) + 1
        hl = 1 + el + sl + tl
        if pos + hl > n:
            break
        size = _read_le(buf, pos + 1 + el, sl)
        if pos + hl + size > n:
            break
        ent[k] = _read_le(buf, pos + 1, el)
        ts[k] = _read_le(buf, pos + 1 + el + sl, tl)
        off[k] = pos + hl
        sz[k] = size
        k += 1
        pos += hl + size
    return k, pos


@njit(cache=True)
def _assign_compact(
    ent: U32Array,
    ts: I64Array,
    off: I64Array,
    sz: U32Array,
    n: int,
    ctrl_pos: I64Array,
    ctrl_kind: I32Array,
    ctrl_entry: I64Array,
    ctrl_cat: I32Array,
    map_size: int,
) -> tuple[I32Array, I64Array, I64Array, U32Array, int, int]:
    """Map data records to catalog entries and compact them, in file order.

    Returns (catalog index, timestamp, offset, size, kept count, orphan count).
    """
    active = np.full(map_size, -1, np.int32)
    cat_out = np.empty(n, np.int32)
    ts_out = np.empty(n, np.int64)
    off_out = np.empty(n, np.int64)
    sz_out = np.empty(n, np.uint32)
    j = 0
    kept = 0
    orphans = 0
    n_ctrl = len(ctrl_pos)
    for i in range(n):
        if j < n_ctrl and ctrl_pos[j] == i:
            e = ctrl_entry[j]
            if 0 <= e < map_size:
                if ctrl_kind[j] == CONTROL_START:
                    active[e] = ctrl_cat[j]
                elif ctrl_kind[j] == CONTROL_FINISH:
                    active[e] = -1
            j += 1
            continue
        e = int(ent[i])
        c = active[e] if e < map_size else -1
        if c < 0:
            orphans += 1
            continue
        cat_out[kept] = c
        ts_out[kept] = ts[i]
        off_out[kept] = off[i]
        sz_out[kept] = sz[i]
        kept += 1
    return cat_out, ts_out, off_out, sz_out, kept, orphans


@njit(cache=True)
def _read_u64(buf: U8Array, p: int, width: int) -> np.uint64:
    v = np.uint64(0)
    for b in range(width):
        v |= np.uint64(buf[p + b]) << np.uint64(8 * b)
    return v


@njit(cache=True)
def _gather_fixed(
    buf: U8Array, cat: I32Array, off: I64Array, sz: U32Array, cat_kind: I32Array
) -> tuple[
    npt.NDArray[np.float64], BoolArray, I64Array, BoolArray, BoolArray, BoolArray, BoolArray,
    BoolArray,
]:  # fmt: skip
    """One pass over all samples: typed values plus validity per value column."""
    n = len(cat)
    f64 = np.zeros(n, np.float64)
    f64_ok = np.zeros(n, np.bool_)
    i64 = np.zeros(n, np.int64)
    i64_ok = np.zeros(n, np.bool_)
    bools = np.zeros(n, np.bool_)
    bool_ok = np.zeros(n, np.bool_)
    str_ok = np.zeros(n, np.bool_)
    bytes_ok = np.zeros(n, np.bool_)
    scratch = np.zeros(1, np.uint64)
    scratch32 = np.zeros(1, np.uint32)
    for i in range(n):
        kind = cat_kind[cat[i]]
        size = sz[i]
        p = off[i]
        if kind == KIND_F64 and size == 8:
            scratch[0] = _read_u64(buf, p, 8)
            f64[i] = scratch.view(np.float64)[0]
            f64_ok[i] = True
        elif kind == KIND_F32 and size == 4:
            scratch32[0] = np.uint32(_read_u64(buf, p, 4))
            f64[i] = np.float64(scratch32.view(np.float32)[0])
            f64_ok[i] = True
        elif kind == KIND_I64 and size == 8:
            scratch[0] = _read_u64(buf, p, 8)
            i64[i] = scratch.view(np.int64)[0]
            i64_ok[i] = True
        elif kind == KIND_BOOL and size == 1:
            bools[i] = buf[p] != 0
            bool_ok[i] = True
        elif kind == KIND_STR:
            str_ok[i] = True
        else:
            bytes_ok[i] = True
    return f64, f64_ok, i64, i64_ok, bools, bool_ok, str_ok, bytes_ok


@njit(cache=True)
def _gather_var(
    buf: U8Array, off: I64Array, sz: U32Array, mask: BoolArray
) -> tuple[U8Array, I64Array]:
    offsets = np.zeros(len(off) + 1, np.int64)
    total = 0
    for i in range(len(off)):
        if mask[i]:
            total += sz[i]
        offsets[i + 1] = total
    data = np.empty(total, np.uint8)
    for i in range(len(off)):
        if mask[i]:
            start = offsets[i]
            p = off[i]
            for b in range(sz[i]):
                data[start + b] = buf[p + b]
    return data, offsets


# --- decoded log -------------------------------------------------------------------------


def _validity(mask: BoolArray) -> pa.Buffer:
    return pa.py_buffer(np.packbits(mask, bitorder="little").tobytes())


def _primitive(
    values: npt.NDArray[np.generic], mask: BoolArray, arrow_type: pa.DataType
) -> pa.Array:
    data = (
        np.packbits(values.astype(np.bool_), bitorder="little")
        if arrow_type == pa.bool_()
        else np.ascontiguousarray(values)
    )
    return pa.Array.from_buffers(
        arrow_type,
        len(mask),
        [_validity(mask), pa.py_buffer(data)],
        null_count=len(mask) - int(np.count_nonzero(mask)),
    )


def _var_array(buf: U8Array, off: I64Array, sz: U32Array, mask: BoolArray) -> pa.Array:
    data, offsets = _gather_var(buf, off, sz, mask)
    return pa.Array.from_buffers(
        pa.large_binary(),
        len(off),
        [_validity(mask), pa.py_buffer(offsets), pa.py_buffer(data)],
        null_count=len(mask) - int(np.count_nonzero(mask)),
    )


def _dictionary(codes: I32Array, values: list[str]) -> pa.DictionaryArray:
    """Dictionary-encode per-sample codes into catalog values (deduplicating if needed)."""
    if len(set(values)) == len(values):
        indices, dictionary = codes, values
    else:
        unique = list(dict.fromkeys(values))
        position = {v: i for i, v in enumerate(unique)}
        remap = np.array([position[v] for v in values], dtype=np.int32)
        indices, dictionary = remap[codes], unique
    return pa.DictionaryArray.from_arrays(
        pa.array(indices, pa.int32()), pa.array(dictionary, pa.string())
    )


@dataclass
class WpilogLog:
    path: Path
    version: int
    extra_header: str
    catalog: list[CatalogEntry]
    truncated_bytes: int
    orphan_records: int
    _buf: U8Array = field(repr=False)
    _cat: I32Array = field(repr=False)
    _ts: I64Array = field(repr=False)
    _off: I64Array = field(repr=False)
    _sz: U32Array = field(repr=False)

    def __len__(self) -> int:
        return len(self._cat)

    def ts_range(self) -> tuple[int, int] | None:
        """(min, max) sample timestamp in microseconds, or None for an empty log."""
        if not len(self._ts):
            return None
        return int(self._ts.min()), int(self._ts.max())

    def subset(self, keep: Callable[[CatalogEntry], bool]) -> "WpilogLog":
        """A view restricted to catalog entries matching `keep` (cheap; no payload copies)."""
        wanted = np.array([keep(e) for e in self.catalog] or [False], dtype=np.bool_)
        mask = wanted[self._cat] if len(self._cat) else np.zeros(0, np.bool_)
        return replace(
            self,
            _cat=self._cat[mask],
            _ts=self._ts[mask],
            _off=self._off[mask],
            _sz=self._sz[mask],
        )

    def to_arrow(self) -> pa.Table:
        """Long-format samples in file order.

        Columns: signal, type, ts_us, v_f64, v_i64, v_bool, v_str, v_bytes.
        """
        cat_kind = np.array(
            [_TYPE_KIND.get(e.type, KIND_BYTES) for e in self.catalog] or [KIND_BYTES], np.int32
        )
        f64, f64_ok, i64, i64_ok, bools, bool_ok, str_ok, bytes_ok = _gather_fixed(
            self._buf, self._cat, self._off, self._sz, cat_kind
        )
        v_str = _var_array(self._buf, self._off, self._sz, str_ok)
        try:
            v_str = pc.cast(v_str, pa.large_string())
        except pa.ArrowInvalid:  # invalid UTF-8: keep those payloads as bytes instead
            bytes_ok |= str_ok
            v_str = pa.nulls(len(self._cat), pa.large_string())
        return pa.table(
            {
                "signal": _dictionary(self._cat, [e.name for e in self.catalog]),
                "type": _dictionary(self._cat, [e.type for e in self.catalog]),
                "ts_us": pa.array(self._ts, pa.int64()),
                "v_f64": _primitive(f64, f64_ok, pa.float64()),
                "v_i64": _primitive(i64, i64_ok, pa.int64()),
                "v_bool": _primitive(bools, bool_ok, pa.bool_()),
                "v_str": v_str,
                "v_bytes": _var_array(self._buf, self._off, self._sz, bytes_ok),
            }
        )

    def samples(self) -> pl.DataFrame:
        frame = pl.from_arrow(self.to_arrow())
        assert isinstance(frame, pl.DataFrame)
        return frame

    def schemas(self) -> dict[str, str]:
        """Struct/proto schemas published under `/.schema/<type>` (last value wins)."""
        schema_rows = [e.index for e in self.catalog if e.name.startswith(_SCHEMA_PREFIX)]
        found: dict[str, str] = {}
        for idx in schema_rows:
            name = self.catalog[idx].name.removeprefix(_SCHEMA_PREFIX)
            for i in np.flatnonzero(self._cat == idx):
                start = int(self._off[i])
                found[name] = bytes(self._buf[start : start + int(self._sz[i])]).decode(
                    errors="replace"
                )
        return found


# --- reading -----------------------------------------------------------------------------


def _inner_string(payload: bytes, pos: int) -> tuple[str, int]:
    (length,) = struct.unpack_from("<I", payload, pos)
    end = pos + 4 + length
    if end > len(payload):
        raise ValueError("string overruns record")
    return payload[pos + 4 : end].decode(errors="replace"), end


def _decode_controls(
    buf: U8Array, ent: U32Array, ts: I64Array, off: I64Array, sz: U32Array
) -> tuple[list[CatalogEntry], I64Array, I32Array, I64Array, I32Array]:
    positions = np.flatnonzero(ent == 0).astype(np.int64)
    kinds = np.full(len(positions), -1, np.int32)
    entries = np.full(len(positions), -1, np.int64)
    cats = np.full(len(positions), -1, np.int32)
    catalog: list[CatalogEntry] = []
    latest: dict[int, int] = {}
    for j, i in enumerate(positions):
        start = int(off[i])
        payload = bytes(buf[start : start + int(sz[i])])
        if len(payload) < 5:
            continue
        kind = payload[0]
        (entry_id,) = struct.unpack_from("<I", payload, 1)
        try:
            if kind == CONTROL_START and len(payload) >= 17:
                name, p = _inner_string(payload, 5)
                type_, p = _inner_string(payload, p)
                metadata, _ = _inner_string(payload, p)
                cats[j] = len(catalog)
                latest[entry_id] = len(catalog)
                catalog.append(
                    CatalogEntry(len(catalog), entry_id, name, type_, metadata, int(ts[i]))
                )
            elif kind == CONTROL_SET_METADATA and len(payload) >= 9:
                metadata, _ = _inner_string(payload, 5)
                if entry_id in latest:
                    catalog[latest[entry_id]].metadata = metadata
            elif kind != CONTROL_FINISH or len(payload) != 5:
                continue
        except (ValueError, struct.error):
            continue
        kinds[j] = kind
        entries[j] = entry_id
    return catalog, positions, kinds, entries, cats


def read_wpilog(path: Path) -> WpilogLog:
    """Decode a .wpilog file. Raises WpilogError for an invalid header or unsupported version."""
    size = path.stat().st_size
    if size < HEADER_FIXED_SIZE:
        raise WpilogError("invalid-header", "file shorter than header")
    buf: U8Array = np.memmap(path, dtype=np.uint8, mode="r")
    if bytes(buf[:6]) != HEADER_MAGIC:
        raise WpilogError("invalid-header", "missing WPILOG magic")
    version = int(buf[6]) | (int(buf[7]) << 8)
    if version >> 8 != SUPPORTED_MAJOR:
        raise WpilogError("unsupported-version", f"0x{version:04x}")
    extra_len = int.from_bytes(bytes(buf[8:12]), "little")
    data_start = HEADER_FIXED_SIZE + extra_len
    if data_start > size:
        raise WpilogError("invalid-header", "extra header overruns file")
    extra_header = bytes(buf[HEADER_FIXED_SIZE:data_start]).decode(errors="replace")

    # Upper bound on record count (min record = 4 header bytes). np.empty is lazily paged,
    # so only the slots actually written consume memory.
    capacity = (size - data_start) // 4 + 1
    ent = np.empty(capacity, np.uint32)
    ts = np.empty(capacity, np.int64)
    off = np.empty(capacity, np.int64)
    sz = np.empty(capacity, np.uint32)
    count, end = _scan(buf, data_start, ent, ts, off, sz)

    catalog, ctrl_pos, ctrl_kind, ctrl_entry, ctrl_cat = _decode_controls(
        buf, ent[:count], ts[:count], off[:count], sz[:count]
    )
    map_size = int(ctrl_entry.max()) + 1 if len(ctrl_entry) and ctrl_entry.max() >= 0 else 1
    cat, ts_k, off_k, sz_k, kept, orphans = _assign_compact(
        ent, ts, off, sz, count, ctrl_pos, ctrl_kind, ctrl_entry, ctrl_cat, map_size
    )
    del ent, ts, off, sz
    return WpilogLog(
        path=path,
        version=version,
        extra_header=extra_header,
        catalog=catalog,
        truncated_bytes=size - end,
        orphan_records=orphans,
        _buf=buf,
        _cat=cat[:kept].copy(),
        _ts=ts_k[:kept].copy(),
        _off=off_k[:kept].copy(),
        _sz=sz_k[:kept].copy(),
    )
