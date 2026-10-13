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

Lines SHALL be told apart by pointing, not by colour. While the pointer is over the chart, the line nearest the pointer at the hovered match, within a short distance, SHALL be drawn emphasised, and the hover readout SHALL name that line's unit (or slot, with one line per slot), its value with the metric's unit, its slot, and its alignment confidence. When no line is near the pointer, the readout SHALL give the selected unit's value at that match, or only the match and its time when no unit is selected; it SHALL NOT name a line the pointer is not on. With one line per unit, selecting a line other than the selected unit's SHALL select that unit, exactly as selecting its row in the device table does.

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

#### Scenario: Hover names the line under the pointer
- **WHEN** no unit is selected, the metric is maximum temperature, and the pointer is on the one point of match `2026gacmp_qm13` that reads 66 °C (unit `ctre:FLYWHEEL-RIGHT`)
- **THEN** that unit's line is drawn emphasised, and the readout names `ctre:FLYWHEEL-RIGHT`, 66.0 °C, its slot, and its alignment

#### Scenario: Hover near no line
- **WHEN** no unit is selected and the pointer is at a match, well above every line
- **THEN** no line is emphasised, and the readout gives only the match and its time

#### Scenario: Selecting a line selects its unit
- **WHEN** no unit is selected and the user selects the 66 °C point of match `2026gacmp_qm13`
- **THEN** `ctre:FLYWHEEL-RIGHT` becomes the selected unit: its table row is marked, the unit panel shows it, and the address carries it, and Replay does not open

#### Scenario: Selecting another unit's line while one is selected
- **WHEN** `ctre:HOOD-G` is selected and the user selects the 76 °C point of match `2026gacmp_qm21` (unit `ctre:INDEXER-MAIN`), far from the hood's line
- **THEN** `ctre:INDEXER-MAIN` becomes the selected unit, and Replay does not open

### Requirement: Drill-through to Replay and raw logs
From a table row's match list, or from a plotted point of the selected unit, the app SHALL open that match in Replay with the unit's slot selected as a track. Selecting the chart at a match where the selected unit's line is the nearest, or where no other line is near, SHALL count as selecting the selected unit's point. With one line per slot, selecting a line SHALL open that match in Replay with that slot as a track. It SHALL also offer the match's raw logs as a download for opening in AdvantageScope. When the launch is available (see `advantagescope-launch`), it SHALL also offer the "Open in AdvantageScope" action for that match.

#### Scenario: Point opens Replay
- **WHEN** unit `ctre:ABC` is selected and the user selects its point for match `2026gacmp_qm7`
- **THEN** Replay opens on Q7 with that unit's slot as a track, and the raw-log download is offered

#### Scenario: Slot line opens Replay
- **WHEN** lines are per slot and the user selects the `intake-roller` line's point at match `2026gadal_qm5`
- **THEN** Replay opens on that match with `intake-roller` as a track

#### Scenario: Launch from a unit's match list
- **WHEN** the user opens a unit's raw logs for `2026johnson_qm15` in a locally served app with a stand-in program configured as AdvantageScope, and chooses "Open in AdvantageScope"
- **THEN** the stand-in is started on the staged Q15 wpilog, and the download stays offered

#### Scenario: Replay data not built
- **WHEN** no Replay data exists for the selected match
- **THEN** the app says it is not built and shows the command that builds it, and the raw-log download and the launch still work
