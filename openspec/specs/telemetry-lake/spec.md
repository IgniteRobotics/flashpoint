# telemetry-lake Specification

## Purpose
Define where Flashpoint stores raw logs and decoded samples, and how they can be queried, so that every later layer reads one stable, partitioned, offline store.

## Requirements

### Requirement: Immutable raw store
Every ingested file SHALL be copied byte-for-byte into raw storage, keyed by its content hash. Raw files SHALL never be modified or deleted by ingest or rebuild.

#### Scenario: Raw copy verified
- **WHEN** a file is ingested
- **THEN** its raw copy exists, and its hash equals the ledger hash

### Requirement: Bronze samples layout
Decoded samples SHALL be stored in a columnar, compressed, partitioned dataset with one row per sample. The columns SHALL include the log id, signal name, signal type, timestamp in microseconds, and one typed value column per value kind. Partitions SHALL be by season and by log, so that queries for one season, event, or log read only the relevant files.

#### Scenario: Query one signal
- **WHEN** a user queries one signal from one ingested log
- **THEN** only that log's partition is read, and the result returns in under 1 second

### Requirement: Atomic visibility
A log's bronze output SHALL become visible to queries all at once. Readers SHALL never observe a partially written log.

#### Scenario: Crash during write
- **WHEN** ingest is killed while writing a log's bronze output
- **THEN** queries see either no rows for that log or all of them, never a subset

### Requirement: Lake location and portability
The lake root SHALL be configurable (an environment variable or CLI flag, with a documented default). A lake directory copied to another machine SHALL be queryable there without re-ingesting.

#### Scenario: Copy to another laptop
- **WHEN** the lake directory is copied to a different machine with Flashpoint installed
- **THEN** the same queries return the same results

### Requirement: Ingest budget
Ingesting one full qualification match (one wpilog plus its CANivore and rio hoots, `health` profile) SHALL complete in under 30 seconds with peak memory under 1 GB on the reference pit laptop, excluding converter download and first-run compile warm-up.

#### Scenario: Q7 budget
- **WHEN** corpus group `2026-gacmp-q7` is ingested into an empty lake
- **THEN** wall time is under 30 s and peak RSS is under 1 GB
