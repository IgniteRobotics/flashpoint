## Why

Today's ingest round-trips binary logs through text CSV, uses a pure-Python parser, joins sparse signals on exact timestamps, and isn't idempotent. The numbers it produces are wrong and slow (#14, #17, #18, #24, #30–#33, #36). Every later phase depends on a correct, reproducible, fast raw store. Roadmap phase **P1**; design in 05 §2–§4.

## What Changes

- Content-addressed **raw store**: originals are kept byte-for-byte, keyed by SHA-256, and never modified
- An **import ledger** with per-stage status (received → bronze → silver → gold → success), a `pipeline_version` field, and quarantine with a reason
- A **wpilog reader** that writes typed, long-format bronze samples, including struct-typed entries
- **hoot conversion** through a versioned owlet registry: pick the version per log, checksum the binaries, detect Pro, and fail loudly on version mismatch
- **Log metadata**: FMSInfo, git build metadata, a `systemTime` wall-clock anchor, and the raw CANInventory entry
- CLI: `flashpoint ingest PATH...` and `flashpoint doctor`
- A performance budget enforced in CI: one qual match (wpilog plus 2 hoots) in under 30 s and under 1 GB RAM

## Capabilities

### New Capabilities
- `log-ingest-ledger`: content-hash dedup, stage tracking, idempotent re-runs, quarantine, rebuild by pipeline version
- `wpilog-reading`: decode every wpilog record type into typed long-format samples
- `hoot-conversion`: version-aware hoot → wpilog conversion, Pro detection, error handling
- `telemetry-lake`: partitioned raw and bronze storage layout, plus the query contract
- `log-metadata`: per-log provenance (FMS, git, time anchor, CAN inventory payload)

### Modified Capabilities
None (greenfield).

## Non-goals

- Mapping signals to components, match framing, or physics (P2)
- Pulling logs from the robot (P3)
- REV `.revlog` and DS `.dslog` readers (later)

## Impact

New `src/flashpoint/{readers,lake,cli}`. Dependencies: robotpy-wpiutil, polars, pyarrow, duckdb. An owlet fetch manifest. Replaces `csv_converter.py`, `datalog.py`, and `ingest_*.py`; they are removed by `retire-legacy-code` stage 2a after this change is archived.
