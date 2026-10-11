# nt-signals Specification

## Purpose
Turn NetworkTables entries that the robot code and PhotonVision publish into labeled, match-aligned signals, so subsystem telemetry and vision can be analyzed next to motor data without hand-maintained CSVs.

## Requirements

### Requirement: Declared signals matched by name
A signal declared in a robot configuration SHALL match the wpilog entry whose name is the declared season root followed by the declared entry, compared exactly and case-sensitively. Signals SHALL be read from the session's wpilog only. Entries that no signal declares SHALL NOT be mapped, and SHALL stay queryable in bronze.

#### Scenario: 2026 declarations match the corpus
- **WHEN** corpus `2026-gacmp-q7` is derived with the 2026 competition robot configuration
- **THEN** every signal declared in that configuration outside the `flashpoint` root matches an entry in its wpilog (the corpus predates the bus-status logger)

#### Scenario: 2025 declarations match the corpus
- **WHEN** corpus `2025-gadal-q30` is derived with the 2025 competition robot configuration
- **THEN** every signal declared in that configuration matches an entry in its wpilog

#### Scenario: Undeclared entries stay in bronze
- **WHEN** a wpilog contains a SmartDashboard entry that no signal declares
- **THEN** no signal sample is written for it, and its samples are still queryable from bronze

### Requirement: Signal samples in silver
For every session mapped to a robot, derive SHALL write one silver signal sample for each sample of a matched signal, carrying the session, signal id, subsystem, component, metric, wpilog time, match time, match phase, and value. Numeric values SHALL be stored as numbers, and boolean values as 1 and 0. Signal samples SHALL be written whether or not the session has hoots. Sessions marked `robot = unknown` SHALL get no signal samples.

#### Scenario: Boolean signal
- **WHEN** a session's wpilog has a declared boolean beam-break entry that turns true twice
- **THEN** its signal samples hold 1 at those two times and 0 elsewhere, with the signal's subsystem and metric

#### Scenario: Session without hoots
- **WHEN** corpus `2026-gacmp-p2` (a wpilog with no hoots) is derived
- **THEN** signal samples are written for its declared signals, although it has no slot samples

#### Scenario: Unframed session
- **WHEN** a session has no match framing (for example corpus `2025-nofms`)
- **THEN** its signal samples are written with wpilog time, and match time and phase are empty

### Requirement: Missing and unusable signals reported
For each session, derive SHALL record every declared signal that is absent from the session's wpilog, or present with a non-numeric, non-boolean type, together with the reason (`missing` or `non-numeric`). It SHALL never fail the session for this. A signal that is present in only part of a log, such as one truncated by power loss, SHALL keep the samples that were read.

#### Scenario: Renamed entry
- **WHEN** a robot configuration declares an entry that a session's wpilog does not contain
- **THEN** the session's report lists that signal as `missing`, derive succeeds, and the other signals are written

#### Scenario: Non-numeric entry
- **WHEN** a declared entry holds string or array values in a session's wpilog
- **THEN** the signal is listed as `non-numeric` for that session, and no samples are written for it

#### Scenario: Truncated wpilog
- **WHEN** a session's wpilog ends early because of a truncated final record
- **THEN** signal samples up to the truncation are written, and the session is not quarantined because of signals

### Requirement: Entries Flashpoint asks the robot to log
A season configuration MAY declare a `flashpoint` root for entries that robot code writes straight to the wpilog for Flashpoint (such as `/Flashpoint/`). Signals under that root SHALL be matched, written to silver, and reported as missing exactly like other declared signals. A robot configuration that declares a signal under a root its season does not define SHALL be rejected when configuration loads, naming the signal and the root. The 2026 competition robot SHALL declare the CAN bus-status signals for each bus the robot code names (`rio` and `DriveTrain`): utilisation, bus-off count, TX-full count, receive error count, and transmit error count, under subsystem `can`, with the bus name as the component.

#### Scenario: Bus status mapped
- **WHEN** a synthetic 2026 wpilog contains `/Flashpoint/CANBus/DriveTrain/Utilization` and `/Flashpoint/CANBus/rio/TransmitErrorCount`
- **THEN** silver holds their samples with subsystem `can`, components `DriveTrain` and `rio`, and metrics `utilization` and `transmit_error_count`

#### Scenario: Robot code without the logger
- **WHEN** corpus `2026-gacmp-q7` is derived with the 2026 robot configuration
- **THEN** each bus-status signal is listed as `missing` for that session, and its other signals are written as before

#### Scenario: Root not defined for the season
- **WHEN** a 2025 robot configuration declares a signal under the `flashpoint` root and the 2025 season defines no such root
- **THEN** configuration loading fails with a message naming the signal and the root
