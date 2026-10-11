## ADDED Requirements

### Requirement: Anomaly badges in Replay
Replay SHALL show anomalies that affect the matches it lists:
- each match in the match list with an open anomaly flagged at that match SHALL show the highest severity glyph among them;
- each track whose slot has an open anomaly flagged at the shown match SHALL show a badge with the anomaly's method and title;
- a `power` anomaly SHALL badge the battery track.

The badges SHALL use the same sensitivity setting as the Anomalies view. In served mode, the badges SHALL come from the current anomalies and triage, without rebuilding Replay data, and selecting a badge SHALL open that anomaly in the Anomalies view. In static mode, the export SHALL carry a snapshot of the open anomalies for its matches, labelled with the time it was taken. Static badges SHALL be read-only and SHALL NOT link to the Anomalies view. Building Replay data SHALL NOT depend on anomalies, so a detection run never rebuilds match data.

#### Scenario: Served badge
- **WHEN** the app is served on a corpus lake built from `2026-gacmp-q7`, and Replay opens Q7
- **THEN** Q7 in the match list shows the HIGH glyph, the battery track shows the brownout badge, and selecting it opens that anomaly in the Anomalies view

#### Scenario: Resolved anomaly drops its badge
- **WHEN** the Q7 brownout anomaly is resolved, and Replay is refreshed
- **THEN** Q7 shows no anomaly glyph, and the Replay data files are unchanged

#### Scenario: Static snapshot
- **WHEN** a static export of Q7 is made and opened from disk with networking disabled
- **THEN** the brownout badge shows with the snapshot time, and it does not link anywhere

#### Scenario: Detection does not rebuild Replay
- **WHEN** detection runs again after a configuration change, and the report command runs
- **THEN** no match data file is rewritten
