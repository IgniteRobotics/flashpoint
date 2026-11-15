## ADDED Requirements

### Requirement: Hoot-only sessions are reported
A hoot-only session (see "Session grouping") has no wpilog to align its hoots to, so its motors get no history: no aligned samples, usage or match features. Flashpoint SHALL say so where a team will see it, and SHALL NOT change any exit code for it.
- **Derive:** when a derive run processes hoot-only sessions, it SHALL print one warning giving how many, up to three of their match keys or start times, that their motors get no history, and that motor history needs the robot to write a wpilog (WPILib data logging). Ingest runs derive, so the warning SHALL appear in ingest's output. A re-run that derives nothing new SHALL NOT repeat it.
- **Setup checks:** a hoot-only check SHALL look at the latest season that has sessions with hoots. It SHALL be a warning when every session with hoots in that season is hoot-only, naming the season and that the robot needs to write a wpilog. When only some are, it SHALL be ok and give their count. With no hoot-only session it SHALL be ok.

A hoot file that was quarantined forms no session and SHALL NOT be counted.

#### Scenario: Hoot logger restart
- **WHEN** every corpus `2026-gacmp-e10` file is ingested into an empty lake
- **THEN** the ingest output carries one warning naming 1 session with hoots but no wpilog and its match key `2026gacmp_e10`, ingest exits 0, and the hoot-only check is ok with a count of 1

#### Scenario: Hoots only
- **WHEN** only corpus `2025-gacmp-q19` (hoots, no wpilog) is ingested into an empty lake
- **THEN** the ingest output warns that 1 session has hoots but no wpilog, and the hoot-only check is a warning naming season 2025 and WPILib data logging

#### Scenario: Re-run with nothing new
- **WHEN** `flashpoint derive` runs again on the lake from "Hoots only" with nothing changed
- **THEN** it prints no hoot-only warning

#### Scenario: Quarantined hoot
- **WHEN** corpus `2026-gadal-q11-empty` (a zero-byte hoot) is ingested into an empty lake
- **THEN** the file is quarantined, no hoot-only warning is printed, and the hoot-only check is ok with nothing to report
