# 0003. wpilog reader: robotpy-wpiutil `wpiutil.log.DataLogReader`

- Status: Proposed
- Date: 2026-10-04
- Refs: 04 §1, D3; pitfall #30

## Context
`datalog.py` is a vendored, pure-Python copy of WPILib's example reader. It builds a Python object for every record (#30) and drops struct-typed entries.

## Decision
Read wpilog with RobotPy's compiled `wpiutil.log.DataLogReader`. Decode struct and protobuf entries using the schemas embedded in the log. Stream records into Arrow record batches.

## Consequences
- Official, maintained, and fast.
- Adds a binary wheel dependency, so check platform wheels exist for every target (macOS arm64, Linux x86-64 and aarch64, Windows).
- The RobotPy version is pinned per season.

## Alternatives considered
- **Vendored `datalog.py`:** no build dependency, but slow.
- **A custom reader from the Kaitai spec:** unnecessary maintenance.

## Update (P1 spike and implementation, 2026-10-04): revised
**Decision changed:** read wpilog with a **compiled (numba), spec-based streaming scanner**. `robotpy-wpiutil` stays as a **dev-only test oracle**.

| Measure (corpus Q7, Apple Silicon) | Official reader, Python loop | Flashpoint reader |
|---|---|---|
| CANivore hoot all-signals export, 36.9 M records | 36 s | **1.8 s (20 M rec/s)** |
| Q7 system wpilog, 643 K records | 0.7 s | under 0.1 s |

- The C++ reader isn't the bottleneck. Crossing into Python once per record caps throughput at about 1 M records/s.
- The scanner reads 32 MB windows, maps entries (handling id reuse after Finish), and gathers typed values straight into Arrow buffers.
- It matches the official reader **exactly** (catalog, record count, every scalar value) on the 2026 Q7 and 2025 Q30 corpus logs; this is enforced in CI.
- Struct, array, and protobuf payloads are kept as raw bytes, with their schemas retrievable. Decoding them into fields is deferred.
- The Status line stays Proposed, pending team review.
