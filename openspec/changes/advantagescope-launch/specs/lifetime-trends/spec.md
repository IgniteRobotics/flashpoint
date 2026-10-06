## MODIFIED Requirements

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
