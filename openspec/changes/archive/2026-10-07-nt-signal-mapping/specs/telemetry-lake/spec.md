## MODIFIED Requirements

### Requirement: Silver and gold layers
The lake SHALL hold, alongside bronze:
- a **silver** dataset of mapped samples (session, slot, unit, metric, match time, phase, value) on the aligned wpilog clock;
- a **silver signal** dataset of mapped NetworkTables samples (session, signal, subsystem, component, metric, wpilog time, match time, phase, value);
- a **gold** feature dataset (see `match-features`);
- **meta** tables for sessions, alignment, slots, units, observations, and missing or unusable signals.

Each layer SHALL be rebuildable from bronze and configuration alone (robot and season configuration), without raw logs or network access.

#### Scenario: Rebuild after configuration change
- **WHEN** a robot configuration changes and a rebuild of the derived layers runs
- **THEN** silver, silver signals, gold, and identity tables are regenerated from bronze, and raw files are not re-read

#### Scenario: Season configuration change
- **WHEN** a season configuration's root changes and derive runs
- **THEN** the sessions of that season are re-derived, and their signal samples reflect the new root
