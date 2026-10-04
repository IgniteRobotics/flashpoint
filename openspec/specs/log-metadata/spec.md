# log-metadata Specification

## Purpose
Extract per-log provenance from inside each log (match info, code build, wall-clock anchor, device inventory), so later phases can identify matches, sessions, and physical units without trusting filenames.

## Requirements

### Requirement: Log metadata record
For each wpilog, the system SHALL record:
- the FMS fields (event name, match number, match type, replay number, alliance, station), each with its last non-empty value;
- the build metadata (project, build date, commit hash, branch, dirty flag) found in the log;
- the filename-derived event and match, when present.

Missing fields SHALL be recorded as absent, not as empty strings or zeros.

#### Scenario: Qualification match
- **WHEN** corpus `2026-gacmp-q7`'s wpilog is ingested
- **THEN** its metadata records event `GACMP`, match type qualification, and match number 7, from the in-log FMS data

#### Scenario: Non-FMS log
- **WHEN** corpus `2025-nofms` is ingested
- **THEN** no FMS match number is recorded, and ingest still succeeds

### Requirement: Wall-clock anchor
The system SHALL record a mapping from log timestamps to UTC wall-clock time, using the log's `systemTime` entries. If there is no `systemTime`, it SHALL fall back to the filename timestamp, and record which source was used.

#### Scenario: Anchor from systemTime
- **WHEN** a 2026 corpus wpilog is ingested
- **THEN** the UTC time in its filename falls between the log's anchored UTC start and end, and the anchor source is `systemTime`

### Requirement: Hoot log metadata
For each hoot, the system SHALL record the bus (rio or CANivore id from the filename), the compliancy, the converter version used, the Pro flag, the signal profile, and the first and last sample timestamps.

#### Scenario: Hoot bus detection
- **WHEN** corpus `2026-gacmp-q7`'s hoots are ingested
- **THEN** one is recorded with bus `rio`, and the other with the CANivore id `6E9415C3394C485320202050101C18FF`

### Requirement: CAN inventory capture
If a wpilog contains `/Flashpoint/CANInventory` entries, every payload SHALL be stored verbatim with its log timestamp, and SHALL be validated against inventory schema v1. Invalid or error payloads SHALL be kept, and flagged.

#### Scenario: Log without inventory
- **WHEN** a corpus log predating the inventory logger is ingested
- **THEN** ingest succeeds, and the log is marked `inventory = absent`
