# lifetime-trends Specification

## Purpose

Let students and mentors see how each physical unit and each slot trends across matches, events, seasons, and robots, by querying the lake locally and offline instead of loading whole tables.

## Requirements

### Requirement: Read-only local query service
In served mode, the app SHALL answer History queries from the lake through a local query service. The service SHALL run with no network access. It SHALL be read-only toward logs and derived layers: no request changes raw files, bronze, silver, gold, or the ledger's records of files and logs. The only writes it SHALL accept are triage changes, as guarded and stored by anomaly review. Query parameters SHALL be treated as data, never as query text.

#### Scenario: Offline and read-only
- **WHEN** the app is served from a corpus lake with networking disabled, and every History query is exercised
- **THEN** each query answers, and the lake files are byte-identical afterwards

#### Scenario: Hostile parameter
- **WHEN** a History query is sent with the event filter `x' OR 1=1 --`
- **THEN** it returns no rows and no error, and the lake is unchanged

#### Scenario: Empty lake
- **WHEN** History is opened on a lake with no gold rows
- **THEN** it shows a "no matches ingested" message naming the lake path, and does not error

#### Scenario: Triage is the only write
- **WHEN** every History and Anomalies query is exercised and one anomaly is set to investigating
- **THEN** raw, bronze, silver, and gold files are byte-identical afterwards, and the only new metadata record is that triage change

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

### Requirement: Filters run as queries
Filters SHALL be applied in the query that reads the lake:
- range: a season, or all time;
- robot, event, and match type;
- phase;
- subsystem, slot, and unit.

The app SHALL NOT load the full feature or usage table into the browser or into memory and filter it afterwards. A refreshed view SHALL return in under 2 seconds on the reference laptop for a full season of gold.

#### Scenario: Event filter reads less
- **WHEN** the user filters to one event
- **THEN** the issued query contains the event predicate, and the rows returned equal the rows for that event only

#### Scenario: Season-scale refresh
- **WHEN** a synthetic gold table of one season (60 matches × 3 phases × 23 slots) is filtered by subsystem
- **THEN** the view refreshes in under 2 seconds

### Requirement: Device table and summary tiles
Below the chart, History SHALL list every unit in range with:
- its current slot (role) and serial;
- its in-service span;
- the selected metric's latest value and change over the range;
- a small trend line;
- a health status.

Health SHALL be the worse of two parts:
- temperature limits: OK, WARN (a match maximum at or above the warning limit in range), or HOT (at or above the fault limit);
- the unit's open anomalies at normal sensitivity: a MED or LOW anomaly counts as WARN, and a HIGH anomaly counts as HOT.

In served mode, a unit with open anomalies SHALL show their count and highest severity in its row, linking to the Anomalies view filtered to that unit. Above the chart, tiles SHALL show units tracked, matches in range, total powered-on hours in range, and units with a WARN or HOT status.

#### Scenario: Health from temperature
- **WHEN** a unit's match maximum temperature reached 66 °C once in the range
- **THEN** its health reads WARN, and the "units warn/hot" tile counts it

#### Scenario: Health from an anomaly
- **WHEN** a unit stays under every temperature limit but has an open HIGH SIBLING anomaly
- **THEN** its health reads HOT, its row shows 1 HIGH anomaly linking to the Anomalies view, and the "units warn/hot" tile counts it

#### Scenario: Closed anomalies do not count
- **WHEN** a unit's only anomaly is dismissed
- **THEN** its health is computed from temperature alone

### Requirement: Drill-through to Replay and raw logs
From any plotted point or table row, the app SHALL open that match in Replay with the unit's slot selected as a track. It SHALL also offer the match's raw logs as a download for opening in AdvantageScope. When the launch is available (see `advantagescope-launch`), it SHALL also offer the "Open in AdvantageScope" action for that match.

#### Scenario: Point opens Replay
- **WHEN** the user selects a point for match `2026gacmp_qm7` on unit `ctre:ABC`
- **THEN** Replay opens on Q7 with that unit's slot as a track, and the raw-log download is offered

#### Scenario: Launch from a unit's match list
- **WHEN** the user opens a unit's raw logs for `2026johnson_qm15` in a locally served app with a stand-in program configured as AdvantageScope, and chooses "Open in AdvantageScope"
- **THEN** the stand-in is started on the staged Q15 wpilog, and the download stays offered

#### Scenario: Replay data not built
- **WHEN** no Replay data exists for the selected match
- **THEN** the app says it is not built and shows the command that builds it, and the raw-log download and the launch still work

### Requirement: User-supplied values are escaped
Every value from logs or configuration (match keys, slot names, roles, event names, serials) SHALL be rendered as text, never as markup or script.

#### Scenario: Hostile slot name
- **WHEN** a robot configuration names a slot role `<img src=x onerror=alert(1)>`
- **THEN** History shows that literal text, and no script runs

### Requirement: History uses the shared filters
History SHALL take its season, robot, event, and unit from the shared filters, as defined by view filters, shown in the shared filter bar. Season, robot, and event SHALL narrow the chart, the device table, and the tiles, as queries. The unit filter SHALL select that unit in History: its line is emphasised, its row is marked, and its details fill the right rail. The other units in range SHALL stay in the chart and table. Selecting a unit in the table SHALL set the shared unit filter, and selecting it again SHALL clear it.

History's match type, phase, subsystem, and slot filters SHALL stay History-only. They SHALL sit in the same filter bar after the divider, SHALL be kept in the page address, and SHALL be cleared by the shared clear control on History.

#### Scenario: Unit chosen in History reaches Anomalies
- **WHEN** the user selects unit `u-12345` in History's table and opens Anomalies from the navigation
- **THEN** Anomalies lists only that unit's anomalies, and its unit chip reads `u-12345`

#### Scenario: Unit set elsewhere
- **WHEN** the shared unit filter is `u-12345` and History is opened
- **THEN** that unit's line is emphasised and its details are in the right rail, and the other units are still listed

#### Scenario: History-only filter stays on History
- **WHEN** the user sets History's subsystem filter to `shooter` and opens Anomalies
- **THEN** Anomalies is not filtered by subsystem, and History still reads `shooter` when the user returns from the navigation

#### Scenario: Clear on History
- **WHEN** History has event `2026gacmp` and subsystem `shooter` set, and the user presses CLEAR FILTERS
- **THEN** the event, the subsystem, and every other filter read "all", and the chart shows all time
