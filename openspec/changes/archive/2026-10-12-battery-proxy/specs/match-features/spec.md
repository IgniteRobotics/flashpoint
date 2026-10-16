## MODIFIED Requirements

### Requirement: Feature table contract
For every framed session with a match key, the system SHALL write one row per (match, phase in {auto, teleop, match}, slot, unit). Each row holds:
- time-weighted mean and P95 of supply and stator current;
- maximum temperature and temperature rise rate;
- supply and motor energy, and stall time;
- residual P95 where available;
- supply path resistance where available (motor physics);
- the robot id, alignment confidence, and source log ids.

#### Scenario: Qualification match rows
- **WHEN** corpus `2026-gacmp-q7` is processed with the 2026 robot configuration
- **THEN** the feature table has one `match` row for each of the 17 mapped motors, plus `auto` and `teleop` rows for each

#### Scenario: Low-confidence alignment carried through
- **WHEN** a session aligned by `enable-edges`
- **THEN** its feature rows carry `alignment = low`
