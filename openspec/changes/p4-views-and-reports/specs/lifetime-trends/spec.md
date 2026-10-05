## Purpose

Let students and mentors see how each slot and each physical unit trends across matches, events, seasons, and robots, by querying the lake locally and offline instead of loading whole tables.

## ADDED Requirements

### Requirement: Local offline trends app
The system SHALL provide a lifetime trends app, started by one command, that reads an existing lake and runs on a pit laptop with no network access. It SHALL open read-only: no view action changes the lake.

#### Scenario: Offline start
- **WHEN** the trends app is started against a lake built from the golden corpus, with networking disabled
- **THEN** it serves its views locally, and the lake files are byte-identical afterwards

#### Scenario: Empty lake
- **WHEN** the trends app is started against a lake with no gold rows
- **THEN** it shows an "no matches ingested" message naming the lake path, and does not error

### Requirement: Per-slot and per-unit trend lines
For a selected feature (for example match maximum temperature, supply energy, stall time, or current P95), the app SHALL plot one point per match, ordered by match time, with one line per slot or per unit as the user chooses. Points SHALL be grouped by event and labelled with the match key. Rows with low alignment confidence SHALL be visibly marked, not hidden.

#### Scenario: Per-slot trend
- **WHEN** the user picks feature "max temperature", phase `match`, and groups by slot for robot `2026-comp`
- **THEN** each mapped slot has one line with a point for every match key in gold for that robot

#### Scenario: Per-unit trend follows a swap
- **WHEN** a unit is observed in slot A for its first matches and slot B afterwards
- **THEN** the per-unit view shows one continuous line for that unit across both slots, with the slot change marked

#### Scenario: Low-confidence point
- **WHEN** a feature row carries `alignment = low`
- **THEN** its point is drawn with a distinct marker and its tooltip states the alignment confidence

### Requirement: Filters run as queries
Filters for season, robot, event, match type, phase, subsystem, slot, and unit SHALL be applied in the query that reads the lake. The app SHALL NOT load the full feature or sample table into memory and filter it afterwards. A refreshed view SHALL return in under 2 seconds on the reference laptop for a full season of gold.

#### Scenario: Event filter reads less
- **WHEN** the user filters to one event
- **THEN** the issued query contains the event predicate, and the rows returned equal the rows for that event only

#### Scenario: Season-scale refresh
- **WHEN** a synthetic gold table of one season (60 matches × 3 phases × 23 slots) is filtered by subsystem
- **THEN** the view refreshes in under 2 seconds

### Requirement: Sibling comparison per slot
For a selected match, the app SHALL compare slots that share a subsystem and model (for example the four drive motors) on the selected feature, and SHALL highlight any slot whose value deviates from the sibling median by more than a configurable fraction (default 25 %).

#### Scenario: One hot drive motor
- **WHEN** three drive slots have match maximum temperature 40 °C and the fourth has 55 °C
- **THEN** the fourth slot is highlighted as deviating from its siblings

### Requirement: Drill-through to the match report and raw log
From any plotted point, the app SHALL link to that match's static report if it has been built, and SHALL offer the raw logs for that match as a download for opening in AdvantageScope.

#### Scenario: Point links out
- **WHEN** the user selects a point for match `2026gacmp_qm7`
- **THEN** the app shows a link to the Q7 report page and a download for the Q7 wpilog and its hoots

#### Scenario: Report not built
- **WHEN** no report exists for the selected match
- **THEN** the app states that the report is not built and shows the command that builds it, and the raw log download still works

### Requirement: User-supplied values are escaped
Every value from logs or configuration (match keys, slot names, roles, event names, serials) SHALL be rendered as text, never as markup or script.

#### Scenario: Hostile slot name
- **WHEN** a robot configuration names a slot role `<img src=x onerror=alert(1)>`
- **THEN** the app shows that literal text, and no script runs
