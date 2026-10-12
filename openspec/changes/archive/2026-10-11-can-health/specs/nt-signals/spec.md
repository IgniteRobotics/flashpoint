## MODIFIED Requirements

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

## ADDED Requirements

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
