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
