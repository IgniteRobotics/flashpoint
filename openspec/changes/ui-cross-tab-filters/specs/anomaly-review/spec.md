## ADDED Requirements

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

## MODIFIED Requirements

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
