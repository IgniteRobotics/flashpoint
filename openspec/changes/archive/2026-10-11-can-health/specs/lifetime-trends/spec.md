## MODIFIED Requirements

### Requirement: History trend lines
The History view SHALL plot one point per match, ordered by match time, for a selected metric:
- maximum temperature;
- lowest supply voltage under load;
- supply energy;
- stall time;
- current P95;
- reboots;
- isolated CAN gaps;
- CAN frames missed per enabled minute;
- longest isolated CAN gap.

By default there SHALL be one line per **unit**, following the physical device by serial across slots and robots. A gap SHALL be shown wherever the unit was not installed. The user SHALL be able to switch to one line per slot. Each metric SHALL show its reference line where a limit is known (65 °C warning; 7.0 V danger). Low-confidence alignment points SHALL be visibly marked, not hidden. The selected unit SHALL be drawn emphasised over the others. For the CAN metrics, a match where the unit's bus had no hoot SHALL be a gap in the line, never a zero.

#### Scenario: Per-unit line follows a move
- **WHEN** a unit is observed in slot A for its first matches and in slot B afterwards
- **THEN** the per-unit view shows one line for that unit across both slots, with the slot change marked

#### Scenario: Not installed
- **WHEN** a unit is absent from matches 10–20
- **THEN** its line has a gap over those matches, not interpolated points

#### Scenario: Per-slot lines
- **WHEN** the user switches to per-slot lines for robot `2026-comp`
- **THEN** each mapped slot has one line with a point for every match key in gold for that robot

#### Scenario: Low-confidence point
- **WHEN** a feature row carries `alignment = low`
- **THEN** its point is drawn with a distinct marker, and its tooltip states the alignment confidence

#### Scenario: Worsening connector
- **WHEN** a synthetic season's CAN-health rows give one unit 13, 46, 14, and then 101 isolated gaps over four matches
- **THEN** the isolated-gap metric plots those four points for that unit in match order, and selecting the last point opens that match in Replay

#### Scenario: No hoot for a match
- **WHEN** a unit's slot has CAN-health rows for matches 1 and 3 but match 2 has no hoot on its bus
- **THEN** the reboot line for that unit has a gap at match 2, not a zero
