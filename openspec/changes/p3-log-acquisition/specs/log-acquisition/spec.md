## Purpose

Copy every `.wpilog` and `.hoot` off a reachable robot into the local inbox, verified and without loss, so no log is lost to robot-side rotation and no robot file is ever modified.

## ADDED Requirements

### Requirement: Robot discovery
The system SHALL try a configured, ordered list of robot hosts. The default list is the team's static radio address, the roboRIO mDNS name, and the USB-tether address. It SHALL use the first host that accepts a connection within a short timeout (default 2 s). An unreachable robot SHALL NOT be an error. If a host's key differs from the one seen previously, the system SHALL accept it and log a warning (a reimaged rio gets a new key).

#### Scenario: Robot on the USB tether only
- **WHEN** the radio address and mDNS name time out and the tether address answers
- **THEN** logs are pulled from the tether address, and the pull records which host served them

#### Scenario: No robot in range
- **WHEN** no configured host answers
- **THEN** the cycle completes without an error status, and nothing is written to the inbox

#### Scenario: Robot reimaged
- **WHEN** a host presents a key different from the previous session's
- **THEN** the pull proceeds, and a warning naming the host is logged

### Requirement: Log discovery on the robot
The system SHALL recursively list `.wpilog` and `.hoot` files under the configured robot log roots. The defaults are the rio's internal log directory and the rio's USB log directory. A missing root SHALL be skipped silently.

#### Scenario: Hoot session folders
- **WHEN** the robot holds `logs/2026-03-14_10-22-33/rio_2026-03-14_10-22-33.hoot` and `logs/FRC_20260314_102233_GACMP_Q7.wpilog`
- **THEN** both are discovered, and each keeps its path relative to its root

#### Scenario: No USB stick in the robot
- **WHEN** the robot's USB log root does not exist
- **THEN** the internal root is still listed, and no error is reported

### Requirement: Active files are not pulled by default
A file SHALL be pulled only once it is stable. Stable means its size and modification time are unchanged across two observations at least a settle interval apart (default 5 s, within the same cycle). The newest `.wpilog` in each directory, and every `.hoot` in the newest hoot session directory, SHALL be treated as active until a newer one exists, because a new file only appears when the robot code restarts. An explicit include-active option SHALL pull active files immediately, and every file pulled that way SHALL be marked `incomplete-read` in the ledger.

#### Scenario: Log still being written
- **WHEN** a wpilog that is not the newest in its directory grows between the two observations of a cycle
- **THEN** it is not pulled in that cycle, and it is pulled in the first cycle where both observations agree

#### Scenario: Current hoot session
- **WHEN** the newest hoot session directory holds a rio hoot and a CANivore hoot, and only the rio hoot grew since the last poll
- **THEN** neither hoot is pulled until a newer hoot session directory exists (the robot code restarted)

#### Scenario: Include-active
- **WHEN** acquisition runs with include-active and the robot is mid-match
- **THEN** the current wpilog and hoots are pulled and ingested, and their ledger entries carry `incomplete-read`

#### Scenario: Completed copy after an active pull
- **WHEN** a file pulled with include-active later becomes stable at a larger size
- **THEN** the complete file is pulled as a new entry, and the earlier partial entry is kept and still marked `incomplete-read`

### Requirement: Verified transfer
Each file SHALL be downloaded to a temporary name and checked against the robot's copy, then atomically moved into `inbox/<host>/<path relative to root>`. A file is `verified` when its size and SHA-256 both match a hash the robot computes. When the robot can't compute a hash, it is `size-verified`: size matches and a warning is logged. A failed check SHALL discard the temporary file and retry on later cycles. After 3 failed attempts, the file SHALL be recorded as `failed` with a reason, and no further automatic retries SHALL happen.

#### Scenario: Clean pull
- **WHEN** a stable 40 MB wpilog is pulled
- **THEN** the inbox copy's SHA-256 equals the robot's, the pull is recorded as `verified`, and no temporary file remains

#### Scenario: Connection drops mid-transfer
- **WHEN** the connection is lost halfway through a file
- **THEN** nothing appears in the inbox under the final name, the temporary file is removed, and the file is retried next cycle

#### Scenario: Hash mismatch three times
- **WHEN** the downloaded bytes disagree with the robot's hash on 3 consecutive attempts
- **THEN** the file is recorded as `failed` with reason `hash-mismatch`, it appears in the health report, and later cycles skip it

#### Scenario: Robot cannot hash
- **WHEN** the robot has no hashing command
- **THEN** files are accepted on size match, recorded as `size-verified`, and one warning per host per cycle is logged

### Requirement: Pull ledger and deduplication
The system SHALL record every pull attempt by source (host or volume), remote path, size, and modification time, together with the resulting SHA-256, status, and time. A source file already recorded as `verified` or `size-verified` with the same path, size, and modification time SHALL NOT be downloaded again.

#### Scenario: Second cycle with nothing new
- **WHEN** two cycles run against an unchanged robot
- **THEN** the second cycle transfers zero bytes

#### Scenario: Same log from two sources
- **WHEN** a log is pulled over the network and later the same bytes arrive from a USB stick
- **THEN** both pulls are recorded, and ingest stores the content once, by hash

### Requirement: Never modify the robot
The system SHALL NOT delete, rename, truncate, or write any file on the robot. No configuration SHALL enable it.

#### Scenario: After a successful pull and ingest
- **WHEN** a file has been pulled, verified, ingested, and backed up
- **THEN** the robot's directory listing is identical to before the pull

### Requirement: Free-space warning
On every successful connection, the system SHALL read the free space of each robot log root's filesystem. Below a configurable threshold (default 100 MB, which is above the 50 MB at which WPILib and Phoenix rotate logs away), it SHALL raise a `low-space` warning naming the host, the root, and the free bytes. The warning SHALL appear in the acquisition status and the health report until a later reading is above the threshold.

#### Scenario: Robot nearly full
- **WHEN** the robot's log filesystem reports 80 MB free
- **THEN** the status shows `low-space` for that host with 80 MB, and the warning is repeated each cycle while the condition lasts

#### Scenario: Space recovered
- **WHEN** a later reading reports 500 MB free
- **THEN** the `low-space` warning is cleared

### Requirement: Authentication and connection errors
A failure to authenticate or open a session SHALL be logged at most once per host per hour. It SHALL NOT stop other hosts or sources from being processed in the same cycle.

#### Scenario: Wrong password configured
- **WHEN** a host rejects the configured credentials
- **THEN** one error is logged for that host, USB volumes and inbox drops are still processed, and the error isn't repeated for an hour
