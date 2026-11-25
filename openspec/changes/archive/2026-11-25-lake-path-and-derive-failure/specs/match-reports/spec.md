## ADDED Requirements

### Requirement: Failed derive after a manual ingest
When the derive that `flashpoint ingest` or `flashpoint rebuild` runs afterwards fails (it exits non-zero for any reason, including a crash in detection or a configuration error), the command SHALL print, after derive's own output, that derive failed and Replay data was not built. The message SHALL name `flashpoint derive --report` as the command to run once the cause is fixed. The command SHALL then exit non-zero, even when every file in the run was ingested. No Replay build SHALL run after a failed derive.

The ingest summary line and the per-file results SHALL be printed as before, and the files ingested in the run SHALL stay ingested. A failed Replay build after a successful derive SHALL keep the rule in "Replay data rebuilt after a manual ingest": its error is printed and the exit code is the ingest's own.

#### Scenario: Derive fails on a bad robot configuration
- **WHEN** `flashpoint ingest` ingests a new match while a robot configuration file is invalid, so derive exits with a configuration error
- **THEN** the output shows the summary with the files ingested, then derive's configuration error, then a line saying derive failed, Replay data was not built, and to run `flashpoint derive --report`; no Replay build runs; and the command exits non-zero

#### Scenario: Retry after the fix
- **WHEN** the robot configuration from the previous scenario is fixed and `flashpoint derive --report` runs
- **THEN** derive succeeds, Replay data is built for the match, and the command exits 0

#### Scenario: Ingesting the same files again
- **WHEN** after the failed derive, `flashpoint ingest` runs again on the same files without fixing anything
- **THEN** every file is reported as `skipped`, derive does not run, and the command exits 0

#### Scenario: Quarantined file and failed derive together
- **WHEN** a run quarantines one file, ingests another, and derive then fails
- **THEN** the output reports both the quarantined file and the failed derive, and the command exits non-zero

#### Scenario: Rebuild reports a failed derive
- **WHEN** `flashpoint rebuild` re-ingests the lake and derive then fails
- **THEN** the output says derive failed and Replay data was not built, and the command exits non-zero

#### Scenario: Derive skipped by option
- **WHEN** `flashpoint ingest --no-derive` ingests new files
- **THEN** no derive runs, no derive failure is reported, and the exit code is the ingest's own
