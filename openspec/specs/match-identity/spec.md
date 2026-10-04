# match-identity Specification

## Purpose
Give every match a single canonical key, so that logs from the same match and the same match across robots and seasons can be found reliably, without trusting filenames first.

## Requirements

### Requirement: Canonical match key
Each identified match SHALL have a key of the form `<season><event>_<level><number>` (for example `2026gacmp_qm7`), using the levels `pm`, `qm`, and `e`. A replay SHALL append `r<n>` when the replay number is greater than 1.

#### Scenario: Qualification match
- **WHEN** corpus `2026-gacmp-q7` is identified
- **THEN** its match key is `2026gacmp_qm7`

#### Scenario: Practice match
- **WHEN** corpus `2026-gacmp-p2` is identified
- **THEN** its match key is `2026gacmp_pm2`

### Requirement: Source precedence
Identity SHALL come from in-log FMS data first. The filename is used only when the log has no FMS match number. The source used SHALL be recorded. If the two sources disagree, the in-log value wins and a `match-identity-conflict` warning is recorded.

#### Scenario: Filename disagrees with FMS
- **WHEN** a log's filename says `Q8` but its FMS data says match 7
- **THEN** the match key uses 7, and the log carries warning `match-identity-conflict`

#### Scenario: Non-match log
- **WHEN** corpus `2025-nofms` is identified
- **THEN** no match key is assigned, and the log is classified as `non-match`

### Requirement: Optional online enrichment
When online access and an API key are configured, the system MAY resolve elimination matches to their official bracket key. It SHALL never require network access to assign a match key.

#### Scenario: Offline
- **WHEN** identification runs with no network
- **THEN** every FMS-identified log still receives a match key
