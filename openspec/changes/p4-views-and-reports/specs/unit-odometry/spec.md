## Purpose

Record how much each physical unit has been used across its whole life (practice, matches, and testing on any robot), so wear can be judged per motor rather than per role.

## ADDED Requirements

### Requirement: Per-session unit usage
For every session with derived samples, practice and non-match sessions included, the system SHALL write one usage row per (session, slot, unit) holding:
- powered-on time: the span over which the unit reported samples in that slot;
- enabled time: the part of powered-on time during which the robot was enabled;
- supply and motor energy in watt-hours;
- stall time;
- the number of thermal cycles;
- the maximum temperature;
- the robot, the match key if any, the alignment confidence, and the session start time.

The usage rows SHALL be rebuildable from bronze and configuration alone, like the other derived layers. When a unit is swapped mid-session, each unit SHALL be credited only with the time it was present.

#### Scenario: Practice session counted
- **WHEN** a session with no match key and 10 minutes of samples is derived
- **THEN** each mapped unit in it has a usage row with about 600 s of powered-on time and no match key

#### Scenario: Match session agrees with features
- **WHEN** corpus `2026-gacmp-q7` is derived
- **THEN** every unit's usage-row supply energy is at least its `match`-phase feature energy, since the session also covers time outside the match, less any energy the unit regenerated outside the match (a coasting drive returns a few joules; supply energy is net)

#### Scenario: Swap splits usage
- **WHEN** a session's inventories show serial `A` and then serial `B` in one slot
- **THEN** `ctre:A` is credited with time before the second inventory and `ctre:B` with the time after, and the two powered-on times sum to the slot's span

### Requirement: Thermal cycles
A thermal cycle SHALL be counted for a unit when its temperature rises by at least a rise threshold (default 10 °C) above the lowest temperature since the previous cycle, and then either falls by at least a fall threshold (default 3 °C) from its peak or the session ends. Both thresholds SHALL be configurable. A unit with no logged temperature SHALL have no cycle count, not zero.

#### Scenario: One cycle with cool-down
- **WHEN** a synthetic temperature goes 22 → 40 → 30 °C within a session
- **THEN** one thermal cycle is counted

#### Scenario: Powered off hot
- **WHEN** a synthetic temperature goes 22 → 36 °C and the session ends at its peak
- **THEN** one thermal cycle is counted

#### Scenario: Small wobble
- **WHEN** a synthetic temperature varies between 30 and 37 °C
- **THEN** no thermal cycle is counted

#### Scenario: No temperature
- **WHEN** a unit has no temperature samples in a session
- **THEN** its cycle count and maximum temperature are absent and flagged `no-temperature`

### Requirement: Unit odometry totals
For each unit, the system SHALL report lifetime totals derived from the usage rows: powered-on hours, enabled hours, supply and motor watt-hours, thermal cycles, stall seconds, maximum temperature ever seen, session count, match count, and first and last seen dates. Totals SHALL be filterable by season and by robot.

#### Scenario: Totals equal the sum of sessions
- **WHEN** a unit has usage rows in three sessions
- **THEN** its lifetime totals equal the sums (and maxima) of those three rows

#### Scenario: Legacy units kept apart
- **WHEN** a slot has a legacy unit before an inventory exists and `ctre:<serial>` afterwards
- **THEN** the two units have separate totals, and neither is merged into the other

### Requirement: Device panel and lifeline
Selecting a unit in History SHALL open a device panel showing:
- its model and serial (or legacy id);
- its health status;
- odometry tiles: matches in service, powered-on hours, supply Wh, thermal cycles, and stall seconds;
- the count of sessions without aligned samples;
- a lifeline, newest first, of events derived from slot observations.

The lifeline events are:
- first seen (installed in a slot);
- moved to another slot or robot;
- replaced (another unit took its slot);
- last seen.

Each event shows the date, the slot (robot, role, bus, and CAN id), and the identity source (inventory or legacy epoch). The panel SHALL be addressable by a link to the unit.

#### Scenario: Unit moved between robots
- **WHEN** serial `ABC` was observed in the practice robot's drive-fl slot and later in the competition robot's drive-fr slot
- **THEN** its lifeline shows "first seen" in drive-fl and "moved" to the competition robot's drive-fr, in time order, with totals covering both

#### Scenario: Replaced unit
- **WHEN** unit `ctre:B` takes over a slot previously held by `ctre:A`
- **THEN** `ctre:A`'s lifeline ends with "replaced by ctre:B", and `ctre:B`'s begins with "first seen"

#### Scenario: Unknown unit
- **WHEN** a link names a unit id that is not in the lake
- **THEN** the app says the unit is not found and lists the closest matching unit ids
