## ADDED Requirements

### Requirement: Season configuration file
Each season that has declared signals SHALL have one season configuration file. It names the season's NetworkTables roots: robot telemetry, PhotonVision, CameraPublisher, Preferences, and FMS. Each root SHALL be written out in full, including the `NT:` prefix and any slash, and never inferred. The file SHALL be validated on load. Unknown keys, a missing season number, or an empty root SHALL be rejected with a message naming the file and the offending key.

#### Scenario: Valid 2025 season
- **WHEN** the 2025 season configuration is loaded
- **THEN** it yields the robot telemetry root `NT:Robot/m_robotContainer/` and the PhotonVision root `NT:/photonvision/`, matching the legacy 2025 log configuration

#### Scenario: Unknown key
- **WHEN** a season configuration contains a key that is not defined
- **THEN** loading fails with an error naming the file and the key

## MODIFIED Requirements

### Requirement: Robot configuration file
Each robot SHALL be described by one configuration file declaring:
- the robot id, its season, and the event codes it competed at;
- one slot per device, giving the bus, the device model, the CAN id, the subsystem, the role, and optionally the gear ratio and swap dates;
- optionally, one signal per NetworkTables entry to map, giving a signal id, a root defined by the season configuration, the entry name under that root, the subsystem, an optional component, and the metric.

Configuration SHALL be validated on load. The following SHALL be rejected with a message naming the file and the offending entry:
- unknown keys;
- duplicate (bus, model, CAN id) slots;
- malformed dates;
- duplicate signal ids, and duplicate (root, entry) signals;
- a signal that names a root its season configuration doesn't define, or a robot that declares signals when its season has no season configuration.

#### Scenario: Valid 2026 configuration
- **WHEN** the 2026 competition robot configuration is loaded
- **THEN** it yields 23 slots: 18 motors, plus the gyro and 4 steer encoders on the CANivore bus. All 22 devices present in corpus `2026-gacmp-q7` (17 motors and 5 sensors) map to a slot
- **AND** it declares signals for the Intake, Shooter, Indexer, Hunter, and Drivetrain subsystems and for each of the three PhotonVision cameras

#### Scenario: Duplicate slot
- **WHEN** two slots declare the same bus, model, and CAN id
- **THEN** loading fails with an error that names both entries

#### Scenario: Undefined root
- **WHEN** a signal names a root that its season configuration does not define
- **THEN** loading fails with an error naming the signal and the root

### Requirement: Legacy configuration migration
A one-time migration SHALL convert the legacy per-season CSV datamaps, the 2025 log configuration, and the power-tracking motor names into robot and season configuration files.

It SHALL account for every legacy row of the 2025 device maps, metrics map, and vision map, and every prefix of the 2025 log configuration. Each one SHALL be either mapped or listed in the migration report with one of these reasons:
- *superseded by CAN slot `<id>`*, for motor voltage, current, or temperature rows of a subsystem that has CAN slots;
- *not in reference log*;
- *non-numeric in reference log*.

Presence and type SHALL be checked against a 2025 reference wpilog. The migration SHALL correct the known `Pheonix6` misspelling. 2024 legacy files are out of scope.

#### Scenario: Misspelled legacy entry
- **WHEN** the legacy rio map containing `Pheonix6/TalonFX-15/Position` is migrated
- **THEN** the resulting slot maps `Phoenix6/TalonFX-15`, and the migration report lists the correction

#### Scenario: Motor row superseded by a CAN slot
- **WHEN** the legacy metrics row `corraler/Motor Current` is migrated
- **THEN** no signal is created for it, and the report lists it as superseded by the corraler motor's CAN slot

#### Scenario: Every legacy row accounted for
- **WHEN** the 2025 migration runs with corpus `2025-gadal-q30` as the reference log
- **THEN** every row of the 2025 metrics and vision maps and every prefix of the 2025 log configuration appears either as a mapped signal or root, or in the report with a reason
