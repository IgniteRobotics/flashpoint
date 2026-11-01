## Purpose

Tell the team, in the app and in `doctor`, when logs or configuration have gone too long without a backup, so a laptop never quietly becomes the only copy of the team's data.

## ADDED Requirements

### Requirement: Overdue measured from the data
Logs SHALL count as waiting for a backup when the lake received them after the last successful backup. Configuration SHALL count as waiting when its files differ from those last backed up successfully; it has waited since the oldest of its changed files was modified. A backup SHALL be overdue when anything has waited longer than the configured limit (default 24 hours). This SHALL hold whether logs were imported by the watch or by hand. A lake that has never had a successful backup SHALL count every log it holds as waiting since it was received.

#### Scenario: Imported by hand, no watch
- **WHEN** two logs were ingested with `flashpoint ingest` 30 hours ago, backup is configured, and no backup has run since
- **THEN** the backup is overdue, with 2 logs waiting since they were received

#### Scenario: Recent logs only
- **WHEN** the last backup succeeded 3 days ago and the only log received since arrived 2 hours ago
- **THEN** the backup is not overdue

#### Scenario: Configuration edited after the last backup
- **WHEN** a robot file was saved 2 days ago, after the last successful backup, and no logs arrived since
- **THEN** the backup is overdue, and the configuration is named as waiting

#### Scenario: Nothing changed for a week
- **WHEN** the last backup succeeded 7 days ago and no logs or configuration changed since
- **THEN** the backup is not overdue

### Requirement: Last successful backup recorded
Every successful backup, from the watch or the backup command, SHALL record its time and the configuration it copied. A failed backup SHALL keep the previous record. A restore SHALL record the restored backup as the last successful one, so the logs and configuration it brought back don't count as waiting.

#### Scenario: Failed attempt keeps the last success
- **WHEN** a backup succeeded at 10:00 and the next attempt at 11:00 failed
- **THEN** the last successful backup is still 10:00, and the last attempt shows the error

#### Scenario: Fresh restore
- **WHEN** a lake is restored from a backup into a new folder
- **THEN** it is not overdue, and a log ingested afterwards counts as waiting

### Requirement: Backup check
The setup checks SHALL include a backup check, shown by `doctor` and the Setup view. It SHALL report a problem when the backup is overdue, naming the number of logs waiting, whether configuration is waiting, how long they have waited, and the last attempt's error if there was one. It SHALL report a warning when backup isn't configured and the lake holds logs, or when the last attempt failed and nothing is overdue yet. Otherwise it SHALL report ok with the time of the last successful backup. Its fix SHALL be the command `flashpoint backup`, or the guide section on backup when backup isn't configured. The check SHALL NOT contact the remote, and SHALL report a warning rather than fail when the lake's records can't be read.

#### Scenario: Offline for two days
- **WHEN** backup is configured, the last attempt failed with a connection error, and logs have waited 2 days
- **THEN** the check reports a problem with the log count, "2 days" and the connection error, and `doctor` exits non-zero

#### Scenario: Failing but not overdue
- **WHEN** the last attempt failed an hour ago and the oldest waiting log arrived 3 hours ago
- **THEN** the check reports a warning naming the error

#### Scenario: Not configured
- **WHEN** no backup remote is set and the lake holds logs
- **THEN** the check reports a warning that the logs and configuration exist only on this machine, pointing to the backup guide

#### Scenario: Empty lake
- **WHEN** the lake holds no logs and the configuration has never been backed up
- **THEN** the check reports ok with nothing to back up

### Requirement: Overdue badge on the Setup tab
When the app is served, the Setup tab SHALL show a badge on every view while the backup is overdue, and none otherwise. Its accessible label SHALL name the problem. The badge SHALL come from a read-only status request that reads only recorded state, so switching views stays fast. A static export SHALL show no badge.

#### Scenario: Overdue at the shop
- **WHEN** Replay is open and the backup is overdue
- **THEN** the Setup tab shows the badge, labelled with the backup problem

#### Scenario: Backed up
- **WHEN** the backup is not overdue
- **THEN** the Setup tab shows no badge
