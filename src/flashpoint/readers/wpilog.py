"""WPILib DataLog (.wpilog) v1.0 reader.

Record framing is scanned by compiled kernels over a memory-mapped file. That is about
40x faster than a per-record Python loop over the official reader, which the test
suite uses as an oracle. Control records (Start/Finish/SetMetadata) are rare and are
decoded in Python.

Spec: https://github.com/wpilibsuite/allwpilib/blob/main/datalog/doc/datalog.adoc
"""

import struct
from dataclasses import dataclass, field
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
    buf: U8Array,
    start: int,
    count_only: bool,
    ent: U32Array,
    ts: I64Array,
    off: I64Array,
    sz: U32Array,
) -> tuple[int, int]:
    """Walk record framing. Returns (records, end position of the last complete record)."""
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
        if not count_only:
            ent[k] = _read_le(buf, pos + 1, el)
            ts[k] = _read_le(buf, pos + 1 + el + sl, tl)
            off[k] = pos + hl
            sz[k] = size
        k += 1
        pos += hl + size
    return k, pos


@njit(cache=True)
def _assign(
    ent: U32Array,
    ctrl_pos: I64Array,
    ctrl_kind: I32Array,
    ctrl_entry: I64Array,
    ctrl_cat: I32Array,
    map_size: int,
) -> I32Array:
    """Catalog index for each record (-1 for control records and orphans)."""
    active = np.full(map_size, -1, np.int32)
    out = np.full(len(ent), -1, np.int32)
    j = 0
    n_ctrl = len(ctrl_pos)
    for i in range(len(ent)):
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
        if e != 0 and e < map_size:
            out[i] = active[e]
    return out


@njit(cache=True)
def _gather_fixed(
    buf: U8Array, off: I64Array, mask: BoolArray, width: int
) -> npt.NDArray[np.uint64]:
    out = np.zeros(len(off), np.uint64)
    for i in range(len(off)):
        if mask[i]:
            v = np.uint64(0)
            p = off[i]
            for b in range(width):
                v |= np.uint64(buf[p + b]) << np.uint64(8 * b)
            out[i] = v
    return out


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


def _fixed_array(raw: npt.NDArray[np.uint64], mask: BoolArray, kind: int) -> pa.Array:
    if kind == KIND_F64:
        values: npt.NDArray[np.generic] = raw.view(np.float64)
        arrow_type = pa.float64()
    elif kind == KIND_F32:
        values = raw.astype(np.uint32).view(np.float32).astype(np.float64)
        arrow_type = pa.float64()
    elif kind == KIND_I64:
        values = raw.view(np.int64)
        arrow_type = pa.int64()
    else:
        return pa.array(raw.astype(np.bool_), type=pa.bool_(), mask=~mask)
    return pa.Array.from_buffers(
        arrow_type,
        len(raw),
        [_validity(mask), pa.py_buffer(np.ascontiguousarray(values))],
        null_count=int((~mask).sum()),
    )


def _var_array(buf: U8Array, off: I64Array, sz: U32Array, mask: BoolArray) -> pa.Array:
    data, offsets = _gather_var(buf, off, sz, mask)
    return pa.Array.from_buffers(
        pa.large_binary(),
        len(off),
        [_validity(mask), pa.py_buffer(offsets), pa.py_buffer(data)],
        null_count=int((~mask).sum()),
    )


def _dictionary(codes: I32Array, values: list[str]) -> pa.DictionaryArray:
    unique = sorted(set(values))
    position = {v: i for i, v in enumerate(unique)}
    remap = np.array([position[v] for v in values], dtype=np.int32)
    indices = remap[codes] if len(codes) else codes
    return pa.DictionaryArray.from_arrays(
        pa.array(indices, pa.int32()), pa.array(unique, pa.string())
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

    def to_arrow(self) -> pa.Table:
        """Long-format samples in file order.

        Columns: signal, type, ts_us, v_f64, v_i64, v_bool, v_str, v_bytes.
        """
        cat, buf, off, sz = self._cat, self._buf, self._off, self._sz
        kinds = np.array(
            [_TYPE_KIND.get(e.type, KIND_BYTES) for e in self.catalog] or [KIND_BYTES], np.int32
        )
        row_kind = kinds[cat] if len(cat) else np.empty(0, np.int32)
        expected = np.full(len(row_kind), -1, np.int64)
        for kind, width in _KIND_SIZE.items():
            expected[row_kind == kind] = width
        fixed_ok = (expected >= 0) & (sz == expected)

        def fixed(kinds_: tuple[int, ...], width: int) -> tuple[npt.NDArray[np.uint64], BoolArray]:
            mask = fixed_ok & np.isin(row_kind, kinds_)
            return _gather_fixed(buf, off, mask, width), mask

        f64_raw, f64_mask = fixed((KIND_F64,), 8)
        f32_raw, f32_mask = fixed((KIND_F32,), 4)
        f32_vals = f32_raw.astype(np.uint32).view(np.float32).astype(np.float64)
        f64_vals = np.where(f32_mask, f32_vals, f64_raw.view(np.float64))
        v_f64 = _fixed_array(f64_vals.view(np.uint64), f64_mask | f32_mask, KIND_F64)
        i64_raw, i64_mask = fixed((KIND_I64,), 8)
        bool_raw, bool_mask = fixed((KIND_BOOL,), 1)

        str_mask = row_kind == KIND_STR
        bytes_mask = ~(fixed_ok | str_mask)
        v_str = _var_array(buf, off, sz, str_mask)
        try:
            v_str = pc.cast(v_str, pa.large_string())
        except pa.ArrowInvalid:  # invalid UTF-8: keep the payload as bytes instead
            bytes_mask |= str_mask
            v_str = pa.nulls(len(cat), pa.large_string())

        return pa.table(
            {
                "signal": _dictionary(cat, [e.name for e in self.catalog]),
                "type": _dictionary(cat, [e.type for e in self.catalog]),
                "ts_us": pa.array(self._ts, pa.int64()),
                "v_f64": v_f64,
                "v_i64": _fixed_array(i64_raw, i64_mask, KIND_I64),
                "v_bool": _fixed_array(bool_raw, bool_mask, KIND_BOOL),
                "v_str": v_str,
                "v_bytes": _var_array(buf, off, sz, bytes_mask),
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

    empty_u32 = np.empty(0, np.uint32)
    empty_i64 = np.empty(0, np.int64)
    count, _ = _scan(buf, data_start, True, empty_u32, empty_i64, empty_i64, empty_u32)
    ent = np.empty(count, np.uint32)
    ts = np.empty(count, np.int64)
    off = np.empty(count, np.int64)
    sz = np.empty(count, np.uint32)
    _, end = _scan(buf, data_start, False, ent, ts, off, sz)

    catalog, ctrl_pos, ctrl_kind, ctrl_entry, ctrl_cat = _decode_controls(buf, ent, ts, off, sz)
    map_size = int(ctrl_entry.max()) + 1 if len(ctrl_entry) and ctrl_entry.max() >= 0 else 1
    cat_all = _assign(ent, ctrl_pos, ctrl_kind, ctrl_entry, ctrl_cat, map_size)

    data = ent != 0
    keep = data & (cat_all >= 0)
    return WpilogLog(
        path=path,
        version=version,
        extra_header=extra_header,
        catalog=catalog,
        truncated_bytes=size - end,
        orphan_records=int((data & (cat_all < 0)).sum()),
        _buf=buf,
        _cat=cat_all[keep],
        _ts=ts[keep],
        _off=off[keep],
        _sz=sz[keep],
    )
