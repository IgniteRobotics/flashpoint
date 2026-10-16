## MODIFIED Requirements

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
