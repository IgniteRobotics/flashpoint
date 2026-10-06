# lifetime-trends Specification

## Purpose

Let students and mentors see how each physical unit and each slot trends across matches, events, seasons, and robots, by querying the lake locally and offline instead of loading whole tables.

## Requirements

### Requirement: Read-only local query service
In served mode, the app SHALL answer History queries from the lake through a local query service. The service SHALL run with no network access and SHALL be read-only: no request changes the lake. Query parameters SHALL be treated as data, never as query text.

#### Scenario: Offline and read-only
- **WHEN** the app is served from a corpus lake with networking disabled, and every History query is exercised
- **THEN** each query answers, and the lake files are byte-identical afterwards

#### Scenario: Hostile parameter
- **WHEN** a History query is sent with the event filter `x' OR 1=1 --`
- **THEN** it returns no rows and no error, and the lake is unchanged

#### Scenario: Empty lake
- **WHEN** History is opened on a lake with no gold rows
- **THEN** it shows a "no matches ingested" message naming the lake path, and does not error

### Requirement: History trend lines
The History view SHALL plot one point per match, ordered by match time, for a selected metric:
- maximum temperature;
- lowest supply voltage under load;
- supply energy;
- stall time;
- current P95.

By default there SHALL be one line per **unit**, following the physical device by serial across slots and robots. A gap SHALL be shown wherever the unit was not installed. The user SHALL be able to switch to one line per slot. Each metric SHALL show its reference line where a limit is known (65 °C warning; 7.0 V danger). Low-confidence alignment points SHALL be visibly marked, not hidden. The selected unit SHALL be drawn emphasised over the others.

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

In P4, health is derived only from temperature limits: OK, WARN (a match maximum at or above the warning limit in range), or HOT (at or above the fault limit). Above the chart, tiles SHALL show units tracked, matches in range, total powered-on hours in range, and units with a WARN or HOT status.

#### Scenario: Health from temperature
- **WHEN** a unit's match maximum temperature reached 66 °C once in the range
- **THEN** its health reads WARN, and the "units warn/hot" tile counts it

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
