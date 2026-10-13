# anomaly-review Specification

## Purpose
Give students and mentors one place to see what Flashpoint flagged, judge the evidence, and record what they did about it, so findings are acted on or closed instead of piling up and being ignored.

## Requirements

### Requirement: Anomalies view
In served mode, the app shell SHALL include an Anomalies view, built to the canvas board "Anomalies". It SHALL have:
- a header with the number of open anomalies, the number that are HIGH, and how far detection has scanned (the latest match judged);
- a queue with three tabs: OPEN (new and investigating), RESOLVED, and DISMISSED, each with its count. Items are ordered by severity, then by most recent last-flagged match;
- for each queue item: its severity glyph and label, subject, method, title, first-seen match, and triage state;
- a sensitivity control (relaxed, normal, strict), as defined by anomaly detection, that changes which anomalies are listed and counted;
- a legend explaining each method in one line;
- a detail panel for the selected anomaly: subject with slot, model, and serial; title; the facts (actual, expected, first seen, matches affected, confidence, method); the evidence chart with its expected band and flagged matches (and the peer line for SIBLING); why it was flagged; likely causes, labelled as suggestions, not a diagnosis; seen this before; and the outcome for closed anomalies.

The view SHALL be absent in static mode. The selected tab, sensitivity, and anomaly SHALL be kept in the URL like other view state.

#### Scenario: Queue from the corpus
- **WHEN** the app is served on a lake built from corpus `2026-gacmp-q7` with a test configuration that sets the brownout threshold to 8.5 V and the sag threshold to 9.0 V (above Q7's lowest battery proxy voltage), and Anomalies is opened
- **THEN** the OPEN tab lists the HIGH brownout anomaly for `power` first, and its detail panel shows its evidence and why it was flagged

#### Scenario: Sensitivity
- **WHEN** a lake has one strict-only LOW anomaly and one MED anomaly, and the user switches from normal to strict
- **THEN** the OPEN count goes from 1 to 2, and switching to relaxed shows only the MED anomaly

#### Scenario: Nothing open
- **WHEN** no anomalies are open at the current sensitivity
- **THEN** the queue says nothing is flagged, and says how many units are still building their baseline

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
