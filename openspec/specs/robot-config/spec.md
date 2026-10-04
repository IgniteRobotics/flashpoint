# robot-config Specification

## Purpose
Describe each robot and season as validated data: which motor fills which role, through which gearing, and when hardware was swapped. Mapping never depends on hard-coded years or hand-edited CSVs.

## Requirements

### Requirement: Robot configuration file
Each robot SHALL be described by one configuration file declaring:
- the robot id, its season, and the event codes it competed at;
- one slot per device, giving the bus, the device model, the CAN id, the subsystem, the role, and optionally the gear ratio and swap dates.

Configuration SHALL be validated on load. Unknown keys, duplicate (bus, model, CAN id) slots, and malformed dates SHALL be rejected with a message naming the file and the offending entry.

#### Scenario: Valid 2026 configuration
- **WHEN** the 2026 competition robot configuration is loaded
- **THEN** it yields 23 slots: 18 motors, plus the gyro and 4 steer encoders on the CANivore bus. All 22 devices present in corpus `2026-gacmp-q7` (17 motors and 5 sensors) map to a slot

#### Scenario: Duplicate slot
- **WHEN** two slots declare the same bus, model, and CAN id
- **THEN** loading fails with an error that names both entries

### Requirement: Robot selection for a log
The system SHALL decide which robot configuration applies to a log from the log's build project name and the season of its wall-clock anchor. If no configuration matches, the log SHALL be marked `robot = unknown`, and its samples SHALL remain queryable unmapped.

#### Scenario: 2026 log selects the 2026 robot
- **WHEN** corpus `2026-gacmp-q7` is processed
- **THEN** it is mapped with the 2026 competition robot configuration

#### Scenario: No configuration
- **WHEN** a log's project and season match no configuration
- **THEN** processing succeeds, and the log is marked `robot = unknown` with no slot mapping

### Requirement: Legacy configuration migration
A one-time migration SHALL convert the legacy per-season CSV datamaps and the power-tracking motor names into robot configuration files. It SHALL report every legacy row it could not map, and it SHALL correct the known `Pheonix6` misspelling.

#### Scenario: Misspelled legacy entry
- **WHEN** the legacy rio map containing `Pheonix6/TalonFX-15/Position` is migrated
- **THEN** the resulting slot maps `Phoenix6/TalonFX-15`, and the migration report lists the correction
