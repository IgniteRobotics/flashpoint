## ADDED Requirements

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
