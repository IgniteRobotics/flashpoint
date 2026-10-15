## ADDED Requirements

### Requirement: Replay data kept current by the acquire watch
After a cycle's derive succeeds, the acquire command SHALL build Replay data incrementally (the same build as the report command with no options), in watch and one-shot modes alike, and SHALL NOT build it in a dry run. The build SHALL run as a separate process, after derive and detection and before backup, so the watch's memory stays within the existing build budget. A stop SHALL end the build like any other step.

A build that fails, is stopped, or finds another build running SHALL stay owed: the next cycle SHALL run it, even when nothing new arrived, and it SHALL stay owed across a restart of the watch. A failed build SHALL NOT fail the cycle, block later cycles' pulls, ingest or derive, or be retried more than once per cycle. The watch status SHALL show "Replay build pending" while a build is owed, and the tail of the last failed build's output until a build succeeds.

A running served app SHALL list a match built this way after the page is reloaded, without restarting the server.

#### Scenario: New match reaches Replay
- **WHEN** the watch is running beside `flashpoint serve`, and a cycle ingests and derives a new match
- **THEN** the same cycle builds that match's Replay data, and after a page reload Replay lists the match and opens it

#### Scenario: Only changed matches are rebuilt
- **WHEN** a cycle derives one new match in a lake whose other matches are already built
- **THEN** the build writes data for the new match only, and every other match's data file is unchanged byte-for-byte

#### Scenario: Nothing new
- **WHEN** a cycle has nothing to ingest and no build owed
- **THEN** no Replay build runs

#### Scenario: Derive failed
- **WHEN** a cycle's derive exits non-zero
- **THEN** no Replay build runs in that cycle

#### Scenario: Build fails and is retried
- **WHEN** the Replay build exits non-zero after a successful derive
- **THEN** the status shows the build's error and "Replay build pending", the next cycle runs the build again with nothing new to ingest, and after it succeeds the status shows neither

#### Scenario: Owed across a restart
- **WHEN** the watch stops while a Replay build is owed, and is started again
- **THEN** its first cycle runs the build

#### Scenario: Dry run
- **WHEN** the acquire command runs with `--dry-run`
- **THEN** no Replay build runs

### Requirement: One Replay build at a time
The report command SHALL hold a per-lake lock while it builds. When another build holds the lock, the report command SHALL exit with code 3 and a message naming the lock, and SHALL write nothing. The served app's start-up build SHALL treat exit 3 as a build already in progress and start normally. The acquire watch SHALL treat exit 3 as not done: the build stays owed for the next cycle, and it is not reported as an error. A lock left behind by a process that no longer runs SHALL NOT block later builds.

#### Scenario: Second build while one runs
- **WHEN** a Replay build holds the lake's lock and `flashpoint report` is run on the same lake
- **THEN** it exits 3, says a build is already running, and writes no data file, index or build state

#### Scenario: Watch meets a running build
- **WHEN** the watch's Replay build exits 3
- **THEN** the status shows "Replay build pending" and no error, and the next cycle runs the build

#### Scenario: Server starts during a watch build
- **WHEN** `flashpoint serve` starts while the watch's Replay build holds the lock
- **THEN** the server starts and serves the existing Replay data

#### Scenario: Stale lock
- **WHEN** a build was killed and left its lock file behind
- **THEN** the next `flashpoint report` builds normally
