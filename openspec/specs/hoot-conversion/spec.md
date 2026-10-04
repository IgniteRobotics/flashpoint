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
Conversion SHALL support named signal profiles. `health` is the default: per-device voltages, currents, temperatures, velocity, position, duty cycle, enable state, faults, and firmware version, plus robot enable and mode. `all` exports every signal. The profile used SHALL be recorded per log.

#### Scenario: Default profile
- **WHEN** a hoot is ingested without specifying a profile
- **THEN** only `health`-profile signals appear in the output, and the log's metadata records `profile = health`

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
