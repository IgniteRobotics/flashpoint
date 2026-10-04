"""Build synthetic WPILib DataLog 1.0 files for tests (spec: allwpilib datalog.adoc)."""

import struct
from pathlib import Path


def _varint(value: int) -> bytes:
    length = max(1, (value.bit_length() + 7) // 8)
    return value.to_bytes(length, "little")


def _string(value: str) -> bytes:
    raw = value.encode()
    return struct.pack("<I", len(raw)) + raw


class WpilogBuilder:
    def __init__(self, extra_header: str = "", version: int = 0x0100) -> None:
        extra = extra_header.encode()
        self._buf = bytearray(b"WPILOG" + struct.pack("<HI", version, len(extra)) + extra)

    def record(self, entry: int, timestamp: int, payload: bytes) -> "WpilogBuilder":
        e, s, t = _varint(entry), _varint(len(payload)), _varint(timestamp)
        header = (len(e) - 1) | ((len(s) - 1) << 2) | ((len(t) - 1) << 4)
        self._buf += bytes([header]) + e + s + t + payload
        return self

    def start(
        self, entry: int, name: str, type_: str, metadata: str = "", ts: int = 0
    ) -> "WpilogBuilder":
        payload = (
            b"\x00" + struct.pack("<I", entry) + _string(name) + _string(type_) + _string(metadata)
        )
        return self.record(0, ts, payload)

    def finish(self, entry: int, ts: int = 0) -> "WpilogBuilder":
        return self.record(0, ts, b"\x01" + struct.pack("<I", entry))

    def set_metadata(self, entry: int, metadata: str, ts: int = 0) -> "WpilogBuilder":
        return self.record(0, ts, b"\x02" + struct.pack("<I", entry) + _string(metadata))

    def double(self, entry: int, ts: int, value: float) -> "WpilogBuilder":
        return self.record(entry, ts, struct.pack("<d", value))

    def float(self, entry: int, ts: int, value: float) -> "WpilogBuilder":
        return self.record(entry, ts, struct.pack("<f", value))

    def int64(self, entry: int, ts: int, value: int) -> "WpilogBuilder":
        return self.record(entry, ts, struct.pack("<q", value))

    def boolean(self, entry: int, ts: int, value: bool) -> "WpilogBuilder":
        return self.record(entry, ts, b"\x01" if value else b"\x00")

    def string(self, entry: int, ts: int, value: str) -> "WpilogBuilder":
        return self.record(entry, ts, value.encode())

    def raw_bytes(self) -> bytes:
        return bytes(self._buf)

    def write(self, path: Path, truncate_tail: int = 0) -> Path:
        data = self.raw_bytes()
        path.write_bytes(data[: len(data) - truncate_tail] if truncate_tail else data)
        return path
