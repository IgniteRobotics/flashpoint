## Purpose

Copy the lake's irreplaceable parts (raw log files and metadata) to a configured remote store, and restore a working lake from that copy, replacing the legacy Drive script.

## ADDED Requirements

### Requirement: Backup scope and safety
Backup SHALL copy the lake's raw files and a consistent snapshot of its metadata to the configured remote. Bronze, silver, and gold SHALL NOT be backed up, since they rebuild from raw. Raw files SHALL only ever be added to the remote, never deleted or overwritten there. The metadata database SHALL be snapshotted in a consistent state even while acquisition is writing to it.

#### Scenario: First backup
- **WHEN** backup runs against an empty remote and a lake with 15 raw files
- **THEN** the remote holds the same 15 raw files with identical hashes, plus a metadata snapshot

#### Scenario: Raw file missing locally
- **WHEN** a raw file exists on the remote but not in the local lake
- **THEN** backup leaves the remote copy in place

#### Scenario: Backup during ingest
- **WHEN** backup runs while ingest is writing to the ledger
- **THEN** the uploaded metadata snapshot opens as a valid database

### Requirement: Remote is any configured target
The backup target SHALL be a named remote from the user's sync-tool configuration plus a path. Flashpoint SHALL NOT hold cloud credentials itself. Without a configured remote, backup SHALL be skipped with an informational message.

#### Scenario: No remote configured
- **WHEN** backup runs with no remote in configuration
- **THEN** it exits 0 with a message that backup is not configured

### Requirement: Backup in the watch loop
In watch mode, backup SHALL run after any cycle that changed raw files or metadata, at most once per configurable interval (default 15 min). A backup failure SHALL be recorded in status and SHALL NOT stop acquisition or ingest.

#### Scenario: Sync tool missing
- **WHEN** the sync tool isn't installed and a remote is configured
- **THEN** the status shows a backup error, and pulls and ingest still complete

#### Scenario: Nothing changed
- **WHEN** a cycle pulls nothing and ingests nothing
- **THEN** no backup runs

### Requirement: Restore
A restore command SHALL copy raw files and the latest metadata snapshot from the remote into an empty or new lake. It SHALL refuse a lake that already holds a ledger unless explicitly forced. It SHALL report that bronze and derived layers must be rebuilt. After restore and a rebuild, the lake SHALL answer the same match queries as the original.

#### Scenario: Restore drill
- **WHEN** the corpus lake is backed up to a local-directory remote, restored into a new lake, and rebuilt
- **THEN** the new lake's ledger lists the same file hashes, and its gold match features equal the original's

#### Scenario: Restore over existing lake
- **WHEN** restore targets a lake that already has a ledger, without force
- **THEN** it exits non-zero, and the lake is unchanged
