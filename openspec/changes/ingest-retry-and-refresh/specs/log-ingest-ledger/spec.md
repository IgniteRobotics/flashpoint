## MODIFIED Requirements

### Requirement: Stage tracking
The ledger SHALL record, per file, its stage (`received`, `bronze`, `success`, `failed`, or `quarantined`), the pipeline version that produced it, and timestamps. A file is `success` only after every derived output for that file is durably written. `success` and `quarantined` are final for a pipeline version. `failed` is not: the file SHALL be processed again by the next ingest that is given it, by the next rebuild, and by the next acquire cycle that finds it in the inbox.

#### Scenario: Interrupted ingest resumes
- **WHEN** ingest is killed after a file reaches `received` but before `bronze`
- **THEN** the next run reprocesses that file, and it leaves no partial derived output visible to queries

#### Scenario: Failed file is retried
- **WHEN** a hoot ended at `failed` in one run and `flashpoint ingest` is given it again on a machine that can now convert it
- **THEN** the hoot is processed again and ends at `success`

### Requirement: Quarantine instead of abort
A file that cannot be processed because of its own content SHALL be marked `quarantined` with a machine-readable reason. These file reasons are `empty-file`, `invalid-header`, `too-old`, `unsupported-compliancy:<n>`, `unsupported-version`, and `no-signals-for-profile`. A file that hits a machine error 3 times (see "Machine failures are retried") SHALL also be quarantined, with that error's reason. Processing of other files in the same run SHALL continue. The command SHALL exit non-zero if any file was quarantined or failed in the run.

#### Scenario: Empty hoot in a batch
- **WHEN** a batch includes corpus `2026-gadal-q11-empty` (0 bytes) and `2026-gacmp-q7`
- **THEN** Q11 is quarantined with reason `empty-file`, Q7 reaches `success`, and the exit code is non-zero

#### Scenario: Not a log file
- **WHEN** a `.wpilog` file does not start with the `WPILOG` header
- **THEN** it is quarantined with reason `invalid-header`

## ADDED Requirements

### Requirement: Machine failures are retried
A file that cannot be processed because of the machine or the program, not the file, SHALL be marked `failed` with a machine-readable reason. It SHALL NOT be quarantined for that reason alone. There are two kinds of machine reason:

- **Environment:** `owlet-not-cached`, `owlet-download-failed`, `owlet-checksum-mismatch`, `owlet-unavailable-for-platform`. These SHALL be retried on every later run, however many times they fail.
- **Possibly the file:** `owlet-failed`, `owlet-timeout`, `internal-error:<type>`. The ledger SHALL count failed attempts per file. The third failure at the same pipeline version SHALL quarantine the file with that reason.

A file's attempt count SHALL reset when it reaches `success` or when the pipeline version changes. Any reason not listed in this spec SHALL be treated as `internal-error`.

Opening a lake written by an earlier version SHALL move every file that is `quarantined` with an environment or possibly-the-file reason to `failed`, with a count of 0. This SHALL happen once, without user action.

#### Scenario: No internet at the venue
- **WHEN** corpus `2026-gacmp-q7` is ingested on a machine with no owlet in its cache and no network, and then again after the network is back
- **THEN** the first run marks the hoots `failed` with reason `owlet-download-failed`, ingests the wpilog, and exits non-zero. The second run converts the hoots, and they end at `success`

#### Scenario: Environment errors never quarantine
- **WHEN** a hoot fails with `owlet-download-failed` in 5 runs in a row
- **THEN** after each run it is `failed`, never `quarantined`

#### Scenario: A file that keeps crashing owlet
- **WHEN** owlet exits with an error on the same hoot in 3 runs
- **THEN** after runs 1 and 2 the hoot is `failed` with reason `owlet-failed`, and after run 3 it is `quarantined` with reason `owlet-failed`

#### Scenario: Upgrading a lake with a machine-error quarantine
- **WHEN** a lake from an earlier version holds a hoot `quarantined` with reason `owlet-download-failed` and a wpilog `quarantined` with reason `invalid-header`, and the lake is opened by this version
- **THEN** the hoot is `failed` and is processed by the next ingest given it, and the wpilog stays `quarantined`

#### Scenario: Acquire keeps a failed file
- **WHEN** an acquire cycle ingests an inbox hoot that ends at `failed`
- **THEN** the hoot stays in the inbox, and the next cycle ingests it again

### Requirement: Re-runs report each file's real state
When ingest is given a file it will not process again, it SHALL report it by its stage: `skipped` for a file at `success`, and `quarantined earlier (<reason>)` for a quarantined file. A file that fails in this run SHALL be reported as `failed (<reason>), will retry`, or as quarantined if this was its third failure. The summary line SHALL count ingested, skipped, failed, quarantined, and quarantined earlier separately. Files quarantined in an earlier run SHALL NOT make the exit code non-zero. When at least one file was quarantined earlier, the output SHALL name the command that retries quarantined files.

#### Scenario: Re-running over a quarantined file
- **WHEN** `flashpoint ingest` is run twice over a folder holding corpus `2026-gadal-q11-empty` and `2026-gacmp-q7`
- **THEN** the second run reports Q7's files as `skipped` and Q11 as `quarantined earlier (empty-file)`, names `flashpoint rebuild --quarantined`, and exits 0

#### Scenario: Failed file in the summary
- **WHEN** a run marks one hoot `failed` with reason `owlet-timeout`
- **THEN** the output lists it as `failed (owlet-timeout), will retry`, the summary counts 1 failed, and the exit code is non-zero

### Requirement: Retrying quarantined files on request
`flashpoint rebuild --quarantined` SHALL reprocess every `quarantined` file from its retained raw copy, whatever its pipeline version, and SHALL reset its attempt count first. A plain `flashpoint rebuild` SHALL also reprocess `failed` files from raw. A file with no raw copy SHALL be reported and left as it is. The original logs are never needed.

#### Scenario: Retry after a fix
- **WHEN** a hoot was quarantined with reason `owlet-failed` after 3 attempts, the cause is fixed, and `flashpoint rebuild --quarantined` runs
- **THEN** the hoot is processed from raw storage and ends at `success`, and files at `success` are not reprocessed

#### Scenario: Bad files stay bad
- **WHEN** `flashpoint rebuild --quarantined` runs on a lake holding corpus `2026-gadal-q11-empty`
- **THEN** Q11 is processed again and is quarantined again with reason `empty-file`
