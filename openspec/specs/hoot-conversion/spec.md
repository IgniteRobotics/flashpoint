# hoot-conversion Specification

## Purpose
Turn CTRE Phoenix 6 signal logs (.hoot) into decodable logs, choosing the converter version that matches each file's format version, and report licensing and errors explicitly.

## Requirements

### Requirement: Converter chosen by file format version
For each hoot, the system SHALL determine the file's format version (compliancy) and convert it with a converter release that supports exactly that version. It SHALL NOT choose a converter based on filename, year, or host OS alone.

#### Scenario: 2026 log
- **WHEN** corpus `2026-gacmp-q7`'s CANivore hoot (compliancy 19) is converted
- **THEN** a converter supporting compliancy 19 is used, and the conversion succeeds

#### Scenario: 2025 log
- **WHEN** corpus `2025-gacmp-q19`'s rio hoot (compliancy 13) is converted
- **THEN** a converter supporting compliancy 13 is used, and the conversion succeeds

#### Scenario: No matching converter
- **WHEN** a hoot reports a compliancy that no registered converter supports
- **THEN** the file is quarantined with reason `unsupported-compliancy:<n>`

### Requirement: Verified converter binaries
Converter binaries SHALL be obtained from a pinned manifest of version, platform, architecture, URL, and SHA-256. They SHALL be verified before use and cached locally, so later runs work offline. Binaries SHALL NOT be stored in the repository.

#### Scenario: Offline after first use
- **WHEN** the converter cache was populated on a previous run, and the network is unavailable
- **THEN** conversion still succeeds

#### Scenario: Tampered binary
- **WHEN** a cached converter's hash does not match the manifest
- **THEN** it is not executed, and the user is told to re-fetch it

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

### Requirement: Licensing and integrity reporting
The system SHALL record, per hoot, whether it contains a Pro-licensed device. Empty or unreadable hoots SHALL be quarantined. Because the converter does not reject truncated hoots, the system SHALL flag a hoot whose signal coverage ends well before its sibling logs.

#### Scenario: Pro flag
- **WHEN** any corpus 2026 hoot is ingested
- **THEN** its metadata records `pro_licensed = true`

#### Scenario: Empty hoot
- **WHEN** corpus `2026-gadal-q11-empty` is ingested
- **THEN** it is quarantined with reason `empty-file`, and no converter is run

#### Scenario: Incomplete read kept and flagged
- **WHEN** corpus `2026-gacmp-e10`'s `19-29-15` hoots are ingested (truncated by a real power loss; the converter reports it could not read to the end)
- **THEN** the samples it could read are ingested, the hoot's read status is `incomplete`, and the file carries warning `incomplete-read`

#### Scenario: Truncated hoot flagged
- **WHEN** corpus `2026-gacmp-e10-truncated` is ingested alongside the rest of the E10 group
- **THEN** the hoot is ingested, but marked with integrity warning `short-coverage`

### Requirement: Non-Pro hoots convert with the free signal set
A hoot with no Pro-licensed device SHALL convert, and SHALL be ingested with whatever signals the converter exports for it. In CTRE's free set these are supply and stator current, supply and motor voltage, position, velocity and the enable state, but not continuous device temperature. The missing temperature SHALL NOT quarantine the hoot or add an integrity warning to it, and SHALL NOT fail the session. If the converter refuses to export a requested signal because the hoot is not Pro-licensed, the hoot SHALL be converted again without that signal, and the signals that were dropped SHALL be recorded on the hoot. Each hoot keeps its own Pro flag, so a session can hold a Pro hoot on one bus and a non-Pro hoot on another.

#### Scenario: Non-Pro hoot with a converter that reports it
- **WHEN** a hoot is ingested through a fake converter that reports it as not Pro-licensed and exports current, voltage, position and velocity but no `DeviceTemp`
- **THEN** ingest succeeds, the hoot records `pro_licensed = false`, its current and voltage samples reach bronze, and it carries no quarantine reason or integrity warning

#### Scenario: Converter refuses a Pro-only signal
- **WHEN** a fake converter fails when asked to export `DeviceTemp` from a non-Pro hoot and succeeds without it
- **THEN** the hoot is ingested without `DeviceTemp`, and its metadata lists `DeviceTemp` as dropped for licensing

#### Scenario: Mixed licensing in one session
- **WHEN** a synthetic session holds a Pro-licensed CANivore hoot and a non-Pro rio hoot from the same hoot group
- **THEN** both are ingested into one session, the CANivore hoot records `pro_licensed = true` and the rio hoot `pro_licensed = false`

#### Scenario: Real non-Pro hoot
- **WHEN** a non-Pro hoot from an outside team, or one recorded on a bench with unlicensed devices, is ingested (the opt-in `outside` test set)
- **THEN** ingest succeeds, it records `pro_licensed = false`, and its devices' current samples reach bronze
