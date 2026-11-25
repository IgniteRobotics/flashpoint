## MODIFIED Requirements

### Requirement: Log metadata record
For each wpilog, the system SHALL record:
- the FMS fields (event name, match number, match type, replay number, alliance, station), each with its last non-empty value. They all come from the FMSInfo entries when those give a match number. Otherwise they all come from the driver-station entries that AdvantageKit logs under `/DriverStation/` (`EventName`, `MatchNumber`, `MatchType`, `ReplayNumber`, `AllianceStation`), so one record never mixes the two. A match type MAY be logged as a number (1 practice, 2 qualification, 3 elimination) or as its name;
- which of the two the FMS fields came from (`fms-info` or `driver-station`);
- the build metadata (project, build date, commit hash, branch, dirty flag) found in the log;
- the filename-derived event, match and start time, when present. Both the WPILib `FRC_…` and the AdvantageKit `akit_…` names are read.

Missing fields SHALL be recorded as absent, not as empty strings or zeros. A driver-station value of the wrong type, or a match type that is neither a known number nor a known name, SHALL be recorded as absent, and ingest SHALL still succeed.

#### Scenario: Qualification match
- **WHEN** corpus `2026-gacmp-q7`'s wpilog is ingested
- **THEN** its metadata records event `GACMP`, match type qualification, and match number 7, from the in-log FMS data, with origin `fms-info`

#### Scenario: Non-FMS log
- **WHEN** corpus `2025-nofms` is ingested
- **THEN** no FMS match number is recorded, and ingest still succeeds

#### Scenario: AdvantageKit driver-station entries
- **WHEN** a synthetic wpilog with no FMSInfo entries logs `/DriverStation/EventName` = `GACMP`, `/DriverStation/MatchType` = `Qualification` and `/DriverStation/MatchNumber` = 7
- **THEN** its metadata records event `GACMP`, match type qualification, and match number 7, with origin `driver-station`

#### Scenario: AdvantageKit filename
- **WHEN** a synthetic wpilog named `akit_26-04-09_21-51-13_GACMP_q7.wpilog` is ingested
- **THEN** its filename-derived event is `GACMP`, its match is qualification 7, and its filename time is 2026-04-09 21:51:13 UTC

#### Scenario: Both present
- **WHEN** a synthetic wpilog logs FMSInfo match 7 at `GACMP` and `/DriverStation/` match 8 at `GACMP`
- **THEN** every recorded FMS field comes from FMSInfo (match 7), with origin `fms-info`

#### Scenario: Wrong-typed driver-station entry
- **WHEN** a synthetic wpilog logs `/DriverStation/MatchNumber` as a string
- **THEN** the match number is recorded as absent, the other fields are still read, and ingest succeeds

### Requirement: Wall-clock anchor
The system SHALL record a mapping from log timestamps to UTC wall-clock time, using the first of these sources that the log has:
1. the log's `systemTime` entries;
2. the epoch-time entry AdvantageKit logs with its system statistics, keeping only samples after 2020-01-01;
3. the filename timestamp, which gives a start time but no mapping.

The source used SHALL be recorded. A log anchored by either of the first two SHALL have a UTC start and end, so it can be grouped with hoots into a session. A wpilog ingested by an earlier version that has no FMS match number or no UTC end SHALL have its metadata read again from the raw store once, on the next derive. If the raw file is missing or unreadable, its earlier metadata SHALL be kept and derive SHALL still succeed.

#### Scenario: Anchor from systemTime
- **WHEN** a 2026 corpus wpilog is ingested
- **THEN** the UTC time in its filename falls between the log's anchored UTC start and end, and the anchor source is `systemTime`

#### Scenario: Anchor from the AdvantageKit epoch entry
- **WHEN** a synthetic AdvantageKit wpilog with no `systemTime` logs epoch-time samples whose offset from log time is constant, and overlaps a synthetic hoot group in that wall-clock time
- **THEN** its anchor source is `advantagekit-epoch`, it has a UTC start and end, and it is grouped into one session with the hoots

#### Scenario: Epoch entry before the clock is set
- **WHEN** the same synthetic log's first epoch samples read 1970 (before the roboRIO clock was set), and later samples are correct
- **THEN** only the later samples are used, and the anchor matches the correct samples

#### Scenario: No anchor entry
- **WHEN** a synthetic AdvantageKit wpilog has neither `systemTime` nor the epoch entry
- **THEN** its anchor source is `filename` with no UTC end, ingest succeeds, and any hoots from that power-on form a hoot-only session

#### Scenario: Lake ingested by an earlier version
- **WHEN** a synthetic AdvantageKit wpilog with the epoch entry was ingested by a version without the epoch anchor (recorded anchor `filename`, no UTC end), and the lake is derived with this version
- **THEN** its anchor source becomes `advantagekit-epoch`, it is grouped with its hoots, and corpus `2026-gacmp-q7`'s wpilog in the same lake is not read again

#### Scenario: Raw file gone before the re-read
- **WHEN** the same older lake is derived after that wpilog's raw file was removed
- **THEN** derive succeeds with one warning naming the log, its anchor stays `filename`, and the next derive does not try again
