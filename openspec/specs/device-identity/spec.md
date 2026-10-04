# device-identity Specification

## Purpose
Track physical motors (units) separately from the roles they fill on a robot (slots), so a motor's wear history follows it through swaps, rebuilds, and moves between robots.

## Requirements

### Requirement: Slot and unit observations
For every session, each mapped device SHALL produce an observation linking its slot to a unit:
- when the session's log carries a valid CAN inventory, the unit is the device's serial number (`ctre:<serial>`);
- otherwise it is a legacy unit `legacy:<slot>:<epoch>`, where the epoch increments at each swap date declared in the robot configuration.

#### Scenario: Log without inventory
- **WHEN** corpus `2026-gacmp-q7` (recorded before the inventory logger existed) is processed
- **THEN** each mapped device gets a legacy unit for its slot

#### Scenario: Log with inventory
- **WHEN** a log contains a valid inventory listing serial `ABC` at bus `rio`, CAN id 5, model `Talon FX`
- **THEN** the slot for rio TalonFX-5 is observed as unit `ctre:ABC`

### Requirement: Swaps detected
If two inventories in one session disagree for the same slot, the system SHALL record a mid-session swap at the later inventory's timestamp. Features SHALL be attributed to the unit that was present at each moment.

#### Scenario: Pit swap between inventories
- **WHEN** a session's inventories show serial `A` and then serial `B` in the same slot
- **THEN** a swap is recorded, and samples after the second inventory are attributed to `ctre:B`

### Requirement: Unmapped devices reported
Devices that appear in a log or inventory with no configured slot SHALL be listed in a per-log report, never silently dropped.

#### Scenario: Unconfigured device
- **WHEN** a log contains a TalonFX on a CAN id that no slot declares
- **THEN** the log's report lists it as unmapped, and its samples stay queryable in bronze
