## ADDED Requirements

### Requirement: Replay data rebuilt after a manual ingest
After `flashpoint ingest` or `flashpoint rebuild` runs derive and the derive succeeds, the command SHALL build Replay data incrementally. That is the same build as the report command with no options, run as a separate process after derive and detection. `--no-report` SHALL skip the build. `--no-derive` SHALL skip it as well, so the acquire watch, which ingests with `--no-derive`, keeps running its own build once per cycle.

When another build holds the lake's Replay lock, the command SHALL print that a build is already running and that `flashpoint report` must be run after it finishes. When the build fails, the command SHALL print the build's error. Neither case SHALL change the command's exit code, which stays the ingest result.

#### Scenario: Wpilog arrives after its hoots
- **WHEN** a match's hoots are ingested on their own, Replay data is built (the match shows "No aligned samples"), and then the match's wpilog is ingested with `flashpoint ingest`
- **THEN** the second ingest rebuilds that match's Replay data, and Replay shows its aligned device data without a separate `flashpoint report`

#### Scenario: Only changed matches are rebuilt
- **WHEN** `flashpoint ingest` adds one new match to a lake whose other matches are already built
- **THEN** the build writes data for the new match only, and every other match's data file is unchanged byte-for-byte

#### Scenario: Opt out
- **WHEN** `flashpoint ingest --no-report` ingests a new match
- **THEN** derive runs and no Replay build runs

#### Scenario: Nothing ingested
- **WHEN** `flashpoint ingest` finds only files already in the lake
- **THEN** neither derive nor a Replay build runs

#### Scenario: Build already running
- **WHEN** `flashpoint ingest` ingests a new match while another Replay build holds the lock
- **THEN** the ingest output says a build is already running and tells the user to run `flashpoint report` after it, and the exit code is the ingest's own
