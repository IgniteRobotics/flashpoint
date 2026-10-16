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

Every row's trend line SHALL be drawn on one vertical scale shared by all listed rows for the selected metric, so rows can be compared at a glance. The scale SHALL start at zero when no value in range is negative, and otherwise run from the lowest to the highest value in range. A match where the unit has no value for the metric (for example, temperature not logged) SHALL be a gap in its line and SHALL NOT widen the scale. A unit with fewer than two values in range SHALL show no line. Each trend line's description SHALL give the unit's own lowest and highest value in range and the shared scale, with the metric's unit.

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

#### Scenario: Rows share one scale
- **WHEN** the selected metric is a count, one unit's values in range are 3, 0, 1, 3 and another's are 7, 20, 1, 42
- **THEN** both trend lines are drawn on the scale 0 to 42, the first line stays in the bottom tenth of its cell while the second reaches the top, and the first line's description reads its own range 0 to 3 and the shared scale 0 to 42

#### Scenario: Negative values in range
- **WHEN** the selected metric's values in range run from −2 to 5
- **THEN** every trend line is drawn on the scale −2 to 5

#### Scenario: Unit missing values in some matches
- **WHEN** a unit has no value in two matches in range and its other values are 10 to 12, and the highest value of any unit is 30
- **THEN** its line has gaps at those two matches, and the shared scale is 0 to 30

#### Scenario: One value in range
- **WHEN** a unit has a single value in range
- **THEN** its trend cell shows no line, its description still gives its value, and the shared scale is unchanged

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

### Requirement: Competition boundaries in History
The History trend chart SHALL show where each competition starts, taken from each plotted match's event in match order. A competition is a run of consecutive matches with the same event; the same event appearing again after another one SHALL start a new run. The chart SHALL name the first competition at its start with no line, and SHALL draw a labelled boundary line, named for the new competition, between the last match of one competition and the first match of the next. Above the chart, History SHALL list the competitions in match order: "COMPETITION · <name>" when there is one, or "COMPETITIONS · " followed by each name and its number of matches, joined by "→", when there are more.

A competition's name SHALL be its event code in capitals without the year (`2026gacmp` reads `GACMP`). When the plotted matches span more than one year, every name SHALL carry its year (`2025 GACMP`). Names SHALL be shown as text.

When a boundary's label would overlap the label before it, the chart SHALL leave that label off and still draw its line; the list above the chart SHALL still name every competition. The boundaries SHALL follow the plotted matches: they SHALL change with the filters, the metric, and per-unit or per-slot lines exactly as the x axis does. With no matches in range, History SHALL show neither boundaries nor the competitions list.

#### Scenario: Season spans two competitions
- **WHEN** History shows the synthetic season (30 matches at `2026gadal`, then 30 at `2026gacmp`) with no event filter
- **THEN** the list reads "COMPETITIONS · GADAL · 30 MATCHES → GACMP · 30 MATCHES", and the chart names GADAL at its start and has one boundary line, labelled GACMP, between the 30th and 31st matches

#### Scenario: One competition
- **WHEN** the event filter is `2026gacmp`
- **THEN** the list reads "COMPETITION · GACMP", and the chart names GACMP at its start with no boundary line

#### Scenario: Boundaries follow the filters
- **WHEN** the event filter is cleared after being `2026gacmp`
- **THEN** the GADAL start label and the GACMP boundary line return, without reloading the page

#### Scenario: Spans more than one year
- **WHEN** the plotted matches are 2 at `2025gaalb` then 3 at `2026gadal`
- **THEN** the list reads "COMPETITIONS · 2025 GAALB · 2 MATCHES → 2026 GADAL · 3 MATCHES", and the boundary line is labelled `2026 GADAL`

#### Scenario: Crowded labels
- **WHEN** two boundaries fall so close together on the chart that their labels would overlap
- **THEN** both lines are drawn, only the earlier of the two labels is drawn, and the list above the chart names both competitions

#### Scenario: No matches in range
- **WHEN** the filters leave no match in range
- **THEN** History shows its "No matches in this range." notice and no competitions list
