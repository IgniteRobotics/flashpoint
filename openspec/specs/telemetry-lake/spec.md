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

Every command SHALL turn the lake root into an absolute folder before it reads or writes the lake. A leading `~` SHALL be expanded from every source. A relative lake from any source (`--lake`, the lake environment variable, or `lake` in the configuration file) SHALL be taken from the folder the command runs in. `flashpoint init` SHALL write the lake into the configuration file as an absolute folder. `doctor` and every message that names the lake SHALL show the absolute folder.

#### Scenario: Copy to another laptop
- **WHEN** the lake directory is copied to a different machine with Flashpoint installed
- **THEN** the same queries return the same results

#### Scenario: Relative --lake on a corpus match
- **WHEN** `flashpoint ingest --lake lake .` runs in a folder holding corpus group `2026-gacmp-q7`
- **THEN** the files are ingested into `./lake`, derive and detection run without error, Replay data is built for the match, and the command exits 0

#### Scenario: Relative lake from the environment
- **WHEN** the lake environment variable is `lake` and `flashpoint doctor` runs in folder `/work`
- **THEN** `doctor` reports the lake as `/work/lake`

#### Scenario: Relative lake in the configuration file
- **WHEN** the configuration file in configuration folder `/cfg` says `lake = "lake"`, and a command runs in folder `/work`
- **THEN** the command uses `/work/lake`

#### Scenario: Home folder in a quoted path
- **WHEN** `flashpoint doctor --lake "~/frc-lake"` runs with the `~` not expanded by the shell
- **THEN** `doctor` reports the lake as the `frc-lake` folder in the user's home folder

#### Scenario: Init stores an absolute lake
- **WHEN** `flashpoint init --team 254 --lake lake` runs in folder `/work` with no configuration
- **THEN** the configuration file names the lake as `/work/lake`

#### Scenario: Server with a relative lake
- **WHEN** `flashpoint serve --lake lake` runs on a derived lake and the anomalies view is opened
- **THEN** the view loads without error

### Requirement: Ingest budget
Ingesting one full qualification match (one wpilog plus its CANivore and rio hoots, `health` profile) SHALL complete in under 30 seconds with peak memory under 1 GB on the reference pit laptop, excluding converter download and first-run compile warm-up.

#### Scenario: Q7 budget
- **WHEN** corpus group `2026-gacmp-q7` is ingested into an empty lake
- **THEN** wall time is under 30 s and peak RSS is under 1 GB

### Requirement: Silver and gold layers
The lake SHALL hold, alongside bronze:
- a **silver** dataset of mapped samples (session, slot, unit, metric, match time, phase, value) on the aligned wpilog clock;
- a **silver signal** dataset of mapped NetworkTables samples (session, signal, subsystem, component, metric, wpilog time, match time, phase, value);
- a **gold** feature dataset (see `match-features`);
- **meta** tables for sessions, alignment, slots, units, observations, and missing or unusable signals.

Each layer SHALL be rebuildable from bronze and configuration alone (robot and season configuration), without raw logs or network access.

#### Scenario: Rebuild after configuration change
- **WHEN** a robot configuration changes and a rebuild of the derived layers runs
- **THEN** silver, silver signals, gold, and identity tables are regenerated from bronze, and raw files are not re-read

#### Scenario: Season configuration change
- **WHEN** a season configuration's root changes and derive runs
- **THEN** the sessions of that season are re-derived, and their signal samples reflect the new root

### Requirement: Device enable state in silver
Silver SHALL hold each mapped device's enable state as the metric `device_enable`, valued 1 when the device reports itself enabled and 0 when it reports itself disabled. The state SHALL be read from the device's logged enable signal whether that signal was logged as text (`Enabled` / `Disabled`, exact match) or as a boolean. A text value other than those two SHALL produce no silver row for that sample, and SHALL NOT fail or quarantine the log or the session. Other device metrics SHALL keep their values unchanged.

A lake derived before this requirement SHALL gain `device_enable` rows the next time derive runs, without a raw-file rebuild.

#### Scenario: Text enable state is kept
- **WHEN** a hoot logs a TalonFX's enable signal as `Enabled` at one time and `Disabled` at a later time, and the session is derived
- **THEN** silver holds two `device_enable` rows for that slot, valued 1 and 0, on the aligned wpilog clock with phase and match time like any other metric

#### Scenario: Boolean enable state is still kept
- **WHEN** a device's enable signal is logged as a boolean true
- **THEN** silver holds a `device_enable` row valued 1

#### Scenario: Unknown text is dropped, not fatal
- **WHEN** a device's enable signal carries a text value that is neither `Enabled` nor `Disabled`
- **THEN** no silver row is written for that sample, the device's other samples are still written, and the session derives successfully

#### Scenario: Q7 enable state follows the match
- **WHEN** corpus group `2026-gacmp-q7` is ingested and derived
- **THEN** every TalonFX slot in the session has `device_enable` rows, all valued 0 or 1; at least 95 % of the session's `auto` and `teleop` rows are 1 and at most 1 % of its `pre`, `gap`, and `post` rows are 1; and per-unit powered and enabled seconds are unchanged from before this requirement

#### Scenario: Existing lake upgrades on derive
- **WHEN** a lake derived without `device_enable` is derived again by a version of Flashpoint that has this requirement
- **THEN** every session is re-derived and silver holds `device_enable` rows, and raw files are not re-read
