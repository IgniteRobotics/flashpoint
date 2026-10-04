# log-ingest-ledger Specification

## Purpose
Track every log file Flashpoint has seen, by content, so that ingest is idempotent, resumable, auditable, and rebuildable when the processing pipeline changes.

## Requirements

### Requirement: Content-addressed identity
Each input file SHALL be identified by the SHA-256 of its bytes. The filename and path SHALL NOT affect identity.

#### Scenario: Same file under two names
- **WHEN** the same log is ingested once as `a.wpilog` and again as `b.wpilog`
- **THEN** the ledger holds one entry for that hash, and records both names as aliases

#### Scenario: Re-ingest is a no-op
- **WHEN** `flashpoint ingest` runs twice over the corpus group `2026-gacmp-q7`
- **THEN** the second run reports 0 new files, and the lake's contents are byte-identical to after the first run

### Requirement: Stage tracking
The ledger SHALL record, per file, its stage (`received`, `bronze`, `success`, or `quarantined`), the pipeline version that produced it, and timestamps. A file is `success` only after every derived output for that file is durably written.

#### Scenario: Interrupted ingest resumes
- **WHEN** ingest is killed after a file reaches `received` but before `bronze`
- **THEN** the next run reprocesses that file, and it leaves no partial derived output visible to queries

### Requirement: Quarantine instead of abort
A file that cannot be processed SHALL be marked `quarantined` with a machine-readable reason. Processing of other files in the same run SHALL continue. The command SHALL exit non-zero if any file was quarantined.

#### Scenario: Empty hoot in a batch
- **WHEN** a batch includes corpus `2026-gadal-q11-empty` (0 bytes) and `2026-gacmp-q7`
- **THEN** Q11 is quarantined with reason `empty-file`, Q7 reaches `success`, and the exit code is non-zero

#### Scenario: Not a log file
- **WHEN** a `.wpilog` file does not start with the `WPILOG` header
- **THEN** it is quarantined with reason `invalid-header`

### Requirement: Rebuild on pipeline version change
When the pipeline version changes, files ingested under an older version SHALL be reprocessed from the retained raw bytes when the user requests a rebuild. Logs are never re-pulled from the robot.

#### Scenario: Rebuild after upgrade
- **WHEN** the pipeline version is bumped and `flashpoint rebuild` runs
- **THEN** every `success` file from the older version is reprocessed from raw storage, and ends at `success` under the new version
