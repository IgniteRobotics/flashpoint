# anomaly-review Specification

## Purpose
Give students and mentors one place to see what Flashpoint flagged, judge the evidence, and record what they did about it, so findings are acted on or closed instead of piling up and being ignored.

## Requirements

### Requirement: Anomalies view
In served mode, the app shell SHALL include an Anomalies view, built to the canvas board "Anomalies". It SHALL have:
- a header with the number of open anomalies, the number that are HIGH, and how far detection has scanned (the latest match judged);
- the shared filter bar, as defined by view filters;
- a queue with three tabs: OPEN (new and investigating), RESOLVED, and DISMISSED, each with its count. Items are ordered by severity, then by most recent last-flagged match;
- a GROUP control that lists the queue as one list or grouped by device;
- for each queue item: its severity glyph and label, subject, method, title, first-seen match, and triage state;
- a sensitivity control (relaxed, normal, strict), as defined by anomaly detection, that changes which anomalies are listed and counted;
- a legend explaining each method in one line;
- a detail panel for the selected anomaly: subject with slot, model, and serial; title; the facts (actual, expected, first seen, matches affected, confidence, method); the evidence chart with its expected band and flagged matches (and the peer line for SIBLING); why it was flagged; likely causes, labelled as suggestions, not a diagnosis; seen this before; and the outcome for closed anomalies.

The header counts, tab counts, and queue SHALL count only the anomalies that pass the shared filters. The view SHALL be absent in static mode. The selected tab, sensitivity, grouping, and anomaly SHALL be kept in the URL like other view state.

#### Scenario: Queue from the corpus
- **WHEN** the app is served on a lake built from corpus `2026-gacmp-q7` with a test configuration that sets the brownout threshold to 8.5 V and the sag threshold to 9.0 V (above Q7's lowest battery proxy voltage), and Anomalies is opened
- **THEN** the OPEN tab lists the HIGH brownout anomaly for `power` first, and its detail panel shows its evidence and why it was flagged

#### Scenario: Sensitivity
- **WHEN** a lake has one strict-only LOW anomaly and one MED anomaly, and the user switches from normal to strict
- **THEN** the OPEN count goes from 1 to 2, and switching to relaxed shows only the MED anomaly

#### Scenario: Nothing open
- **WHEN** no anomalies are open at the current sensitivity
- **THEN** the queue says nothing is flagged, and says how many units are still building their baseline

#### Scenario: Nothing passes the filters
- **WHEN** anomalies are open, but none passes the shared filters
- **THEN** the queue says nothing passes these filters and offers to clear them

#### Scenario: Link keeps the selection
- **WHEN** the user copies the address of an open anomaly at strict sensitivity, and opens it in a new window
- **THEN** the same tab, sensitivity, and anomaly are selected

### Requirement: Drill-through from evidence
Selecting a point on the evidence chart SHALL open that match in Replay with the anomaly's slot selected as a track. Selecting the subject SHALL open History for that unit with the anomaly's metric selected. For a `power` subject, the match SHALL open in Replay with the battery track selected.

#### Scenario: Evidence point opens Replay
- **WHEN** the user selects the onset point of a unit's STEP anomaly
- **THEN** Replay opens on the onset match with that unit's slot as a track

### Requirement: Triage states and actions
Each anomaly SHALL have a triage state: new, investigating, resolved, or dismissed. Anomalies with no triage record are new. In served mode, the detail panel SHALL offer:
- for new: INVESTIGATE, DISMISS AS EXPECTED;
- for investigating: MARK RESOLVED, DISMISS AS EXPECTED;
- for resolved or dismissed: REOPEN.

MARK RESOLVED and DISMISS AS EXPECTED SHALL require an outcome text of 1 to 500 characters. DISMISS AS EXPECTED SHALL offer a "restart baseline from the onset match" choice. Every change SHALL be recorded with its time, the previous state, the new state, and the outcome text. Earlier records SHALL never be overwritten, so the history of an anomaly can be read back. Outcome text SHALL be shown as text, never as markup.

The canvas's CREATE PIT TASK action SHALL NOT be offered.

#### Scenario: Investigate then resolve
- **WHEN** a new anomaly is set to investigating, then resolved with the outcome "wheel bearing replaced"
- **THEN** it moves to the RESOLVED tab, its outcome reads "wheel bearing replaced", and its history lists both changes in order

#### Scenario: Close without a reason
- **WHEN** MARK RESOLVED is sent with an empty outcome
- **THEN** the request is refused with a message asking for an outcome, and the state does not change

#### Scenario: Hostile outcome text
- **WHEN** an anomaly is dismissed with the outcome `<img src=x onerror=alert(1)>`
- **THEN** the outcome is shown literally, and no script runs

### Requirement: Dismissal effects
A dismissed anomaly SHALL stay dismissed while its episode continues. New flagged matches SHALL extend it without reopening it. A flag after the episode ends SHALL create a new anomaly in state new.

When dismissed with "restart baseline", the subject's baseline for that metric SHALL use only matches from the onset match onward on the next detection run. This applies to that metric only. Every other metric's baseline and every other unit's baseline SHALL be unchanged. Until that run has happened, the view SHALL say the baseline restart is pending.

#### Scenario: Gearbox change
- **WHEN** a climber unit's stator current P95 steps up after a ratio change, its STEP anomaly is dismissed with "restart baseline", and detection runs again
- **THEN** no new STEP or DRIFT anomaly is raised for that unit's stator current P95 from the new level, and its temperature anomalies are unchanged

#### Scenario: Dismissed episode extends
- **WHEN** a dismissed anomaly's subject is flagged again in the next match, within its episode
- **THEN** the anomaly stays dismissed, and its matches-affected count goes up by one

### Requirement: Guarded triage writes
Triage changes SHALL be accepted only by the locally bound server, under the same origin and host checks as the AdvantageScope launch. When the app is shared on the network, or opened as a static export, triage actions SHALL be absent and the view SHALL be read-only. Triage SHALL be stored in the lake's metadata database, so that:
- the metadata backup carries it and restore brings it back;
- it is never removed by derive, detect, rebuild, or a pipeline version change.

A triage record for an anomaly id that the latest detection run no longer produces SHALL be kept, and listed in its tab as no longer detected.

#### Scenario: Shared server is read-only
- **WHEN** the app is served on a non-local address and a triage change is sent from another machine
- **THEN** the request is refused, the state is unchanged, and the view shows no triage actions

#### Scenario: Cross-site request
- **WHEN** a triage change is sent to the local server with a foreign origin
- **THEN** it is refused, and no record is written

#### Scenario: Survives rebuild
- **WHEN** an anomaly is resolved, and the lake is then rebuilt from raw and re-derived
- **THEN** the anomaly is produced again with the same id and is still resolved with its outcome

#### Scenario: No longer detected
- **WHEN** a resolved anomaly's metric is removed from detection configuration and detection runs again
- **THEN** the anomaly is still listed under RESOLVED, marked as no longer detected

### Requirement: Navigation badge
In served mode, the shell navigation SHALL show the number of open anomalies at normal sensitivity next to ANOMALIES. It SHALL be emphasised when any of them is HIGH. The count SHALL refresh when the view changes and after a triage change.

#### Scenario: Badge count
- **WHEN** the lake has 2 open anomalies, one of them HIGH
- **THEN** the navigation shows 2 next to ANOMALIES in the emphasised style

### Requirement: Anomaly filters
The Anomalies view SHALL take the shared season, robot, event, and unit filters, as defined by view filters, shown in the shared filter bar. An anomaly SHALL pass when every chosen filter passes:
- robot: the anomaly's robot is the chosen robot;
- season and event: at least one of its flagged matches is in the chosen season and event. An anomaly with no flagged matches recorded (one no longer detected) uses its onset match;
- unit: the anomaly is about the chosen unit.

The query service SHALL apply these filters, and the browser SHALL NOT receive anomalies the filters exclude. The tab counts, the open and HIGH counts in the header, and the queue SHALL count only the anomalies that pass. Filter values SHALL be treated as data.

#### Scenario: Event filter
- **WHEN** a lake has one open anomaly flagged only at `2026gadal` and one flagged at both `2026gadal` and `2026gacmp`, and the event filter is set to `2026gacmp`
- **THEN** the OPEN tab lists only the second anomaly, and the open count reads 1

#### Scenario: Robot filter
- **WHEN** the robot filter is set to `practice` on a lake whose only HIGH anomaly is on `comp`
- **THEN** the header reads 0 HIGH, and that anomaly is not listed

#### Scenario: No longer detected
- **WHEN** a triaged anomaly is no longer detected, its onset match is `2026gadal_qm12`, and the event filter is `2026gadal`
- **THEN** it is listed in its tab

#### Scenario: Hostile filter value
- **WHEN** `/api/anomalies` is called with the event `x' OR 1=1 --`
- **THEN** it answers with an empty queue and no error, and the lake is unchanged

### Requirement: Group anomalies by device
The queue SHALL offer a GROUP control with two choices: NONE, the queue as one list, and DEVICE. With DEVICE, the queue SHALL group the listed anomalies by the physical unit they are about. Anomalies about a robot's power SHALL form one group per robot, named for its battery. Each group header SHALL show the device (its role, and its serial or "legacy id"), the number of listed anomalies, and their highest severity. Groups SHALL be ordered by highest severity, then by the most recent last-flagged match among their anomalies. Within a group, anomalies SHALL keep the queue order.

Each group SHALL collapse and expand from its header, which SHALL be a button reachable by keyboard that announces whether the group is expanded. The group holding the selected anomaly SHALL start expanded. Grouping SHALL use the same tab, sensitivity, and filters as the list. The GROUP choice SHALL be kept in the page address. An unknown value SHALL show the ungrouped list.

#### Scenario: Two anomalies on one unit
- **WHEN** a lake has a HIGH STEP and a LOW DRIFT anomaly on unit `u-12345` and one MED anomaly on unit `u-67890`, and the user chooses GROUP DEVICE
- **THEN** the queue shows two groups: `u-12345`'s first with 2 anomalies and HIGH, then `u-67890`'s with 1 and MED

#### Scenario: Battery group
- **WHEN** the OPEN tab holds two brownout anomalies on `comp` and the user chooses GROUP DEVICE
- **THEN** they are in one group named for `comp`'s battery

#### Scenario: Collapse a group
- **WHEN** the user collapses a group
- **THEN** its anomalies are hidden, and its header still shows the device, count, and highest severity

#### Scenario: Link keeps the grouping
- **WHEN** the user copies the address with GROUP DEVICE and an anomaly selected, and opens it in a new window
- **THEN** the queue is grouped by device, and the selected anomaly's group is expanded with it selected

#### Scenario: Unknown group value
- **WHEN** the address holds `group=bogus`
- **THEN** the queue shows the ungrouped list

### Requirement: Competitions in the evidence
The detail panel SHALL name the competition the evidence comes from, taken from each evidence match's event. When the evidence spans more than one competition, the panel SHALL list them in match order, each with its number of matches, and the evidence chart SHALL draw a labelled boundary line between the last match of one competition and the first match of the next. The chart SHALL also name the first competition at its start. A competition's name SHALL be shown as text. When the evidence spans more than one year, every name SHALL carry its year (`2025 GACMP`). When a boundary's label would overlap the label before it, the chart SHALL leave that label off and still draw its line; the panel SHALL still name every competition.

#### Scenario: One competition
- **WHEN** every evidence match of the selected anomaly is at `2026gadal`
- **THEN** the panel reads "COMPETITION · GADAL", and the evidence chart names GADAL at its start with no boundary line

#### Scenario: Evidence spans competitions
- **WHEN** the selected anomaly's evidence is 3 matches at `2026gadal` then 2 at `2026gacmp`
- **THEN** the panel reads "COMPETITIONS · GADAL · 3 MATCHES → GACMP · 2 MATCHES", and the evidence chart names GADAL at its start and has one boundary line, labelled GACMP, between the third and fourth matches
