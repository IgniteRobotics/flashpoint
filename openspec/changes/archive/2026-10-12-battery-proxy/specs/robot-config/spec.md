## MODIFIED Requirements

### Requirement: Robot configuration file
Each robot SHALL be described by one configuration file declaring:
- the robot id, its season, and the event codes it competed at;
- one slot per device, giving the bus, the device model, the CAN id, the subsystem, the role, and optionally the gear ratio and swap dates;
- optionally, a battery-proxy slot: a low-current device on the main power bus whose supply voltage stands in for the battery;
- optionally, one signal per NetworkTables entry to map, giving a signal id, a root defined by the season configuration, the entry name under that root, the subsystem, an optional component, and the metric.

Configuration SHALL be validated on load. The following SHALL be rejected with a message naming the file and the offending entry:
- unknown keys;
- duplicate (bus, model, CAN id) slots;
- malformed dates;
- duplicate signal ids, and duplicate (root, entry) signals;
- a battery proxy naming a slot the robot does not have;
- a signal that names a root its season configuration doesn't define, or a robot that declares signals when its season has no season configuration.

#### Scenario: Valid 2026 configuration
- **WHEN** the 2026 competition robot configuration is loaded
- **THEN** it yields 23 slots: 18 motors, plus the gyro and 4 steer encoders on the CANivore bus. All 22 devices present in corpus `2026-gacmp-q7` (17 motors and 5 sensors) map to a slot
- **AND** it declares the gyro as its battery proxy
- **AND** it declares signals for the Intake, Shooter, Indexer, Hunter, and Drivetrain subsystems and for each of the three PhotonVision cameras

#### Scenario: Duplicate slot
- **WHEN** two slots declare the same bus, model, and CAN id
- **THEN** loading fails with an error that names both entries

#### Scenario: Undefined root
- **WHEN** a signal names a root that its season configuration does not define
- **THEN** loading fails with an error naming the signal and the root

#### Scenario: Unknown battery proxy
- **WHEN** a robot configuration declares a battery proxy of `pdh`, which is not one of its slots
- **THEN** loading fails with an error naming the file and `pdh`
