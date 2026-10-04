## MODIFIED Requirements

### Requirement: Signal profiles
Conversion SHALL support named signal profiles.
- `health` is the default. It covers per-device voltages, currents, temperatures, velocity, position, duty cycle, enable state, faults, and firmware version, plus robot enable and mode. It also includes rotor (motor-shaft) velocity, the device-reported motor constants (torque constant, velocity constant, stall current), and the drivetrain pose used for clock alignment.
- `all` exports every signal.

The profile used SHALL be recorded per log.

#### Scenario: Default profile
- **WHEN** a hoot is ingested without specifying a profile
- **THEN** only `health`-profile signals appear in the output, and the log's metadata records `profile = health`

#### Scenario: Alignment and physics signals present
- **WHEN** corpus `2026-gacmp-q7`'s CANivore hoot is ingested with the default profile
- **THEN** its output includes the drivetrain pose and each TalonFX's motor constants
