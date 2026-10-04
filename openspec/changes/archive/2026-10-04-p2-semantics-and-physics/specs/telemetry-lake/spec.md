## ADDED Requirements

### Requirement: Silver and gold layers
The lake SHALL hold, alongside bronze:
- a **silver** dataset of mapped samples (session, slot, unit, metric, match time, phase, value) on the aligned wpilog clock;
- a **gold** feature dataset (see `match-features`);
- **meta** tables for sessions, alignment, slots, units, and observations.

Each layer SHALL be rebuildable from bronze and configuration alone, without raw logs or network access.

#### Scenario: Rebuild after configuration change
- **WHEN** a robot configuration changes and a rebuild of the derived layers runs
- **THEN** silver, gold, and identity tables are regenerated from bronze, and raw files are not re-read
