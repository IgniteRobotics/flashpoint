## MODIFIED Requirements

### Requirement: Backup check
The setup checks SHALL include a backup check, shown by `doctor` and the Setup view. It SHALL report a problem when the backup is overdue, naming the number of logs waiting, whether configuration is waiting, how long they have waited, and the last attempt's error if there was one. It SHALL report a warning when the last attempt failed and nothing is overdue yet. When backup isn't configured, it SHALL report ok, marked optional, saying the logs and configuration exist only on this machine and pointing to the backup section of the published guide; backup is something a team opts into, so not having one SHALL NOT be a warning. When backup is configured and the copy tool it uses (rclone) isn't installed, the check SHALL be at least a warning, saying rclone isn't installed, and its fix SHALL be to install rclone (naming its install page) and then run `flashpoint backup`. Otherwise it SHALL report ok with the time of the last successful backup, and the fix for a configured backup SHALL be the command `flashpoint backup`. The check SHALL NOT contact the remote, and SHALL report a warning rather than fail when the lake's records can't be read.

#### Scenario: Offline for two days
- **WHEN** backup is configured, rclone is installed, the last attempt failed with a connection error, and logs have waited 2 days
- **THEN** the check reports a problem with the log count, "2 days" and the connection error, and `doctor` exits non-zero

#### Scenario: Failing but not overdue
- **WHEN** the last attempt failed an hour ago and the oldest waiting log arrived 3 hours ago
- **THEN** the check reports a warning naming the error

#### Scenario: Not configured
- **WHEN** no backup remote is set and the lake holds corpus `2026-gacmp-q7`
- **THEN** the check reports ok, marked optional, saying the logs and configuration exist only on this machine and giving the published address of the backup guide, and it adds no warning to `doctor`

#### Scenario: Configured without rclone
- **WHEN** a backup remote is set, rclone is not on the path, and nothing is overdue
- **THEN** the check reports a warning that rclone isn't installed, with the fix to install it from its install page and then run `flashpoint backup`

#### Scenario: Overdue without rclone
- **WHEN** a backup remote is set, rclone is not on the path, and logs have waited 2 days
- **THEN** the check reports a problem, and its fix names installing rclone before `flashpoint backup`

#### Scenario: Empty lake
- **WHEN** the lake holds no logs and the configuration has never been backed up
- **THEN** the check reports ok with nothing to back up

#### Scenario: Unreadable records
- **WHEN** backup is configured and the lake's ledger file is corrupt
- **THEN** the check reports a warning that the backup state can't be read, and `doctor` still runs every other check
