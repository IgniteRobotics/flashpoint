## Purpose

Run acquisition, ingest, derive, and backup unattended in one loop on a pit laptop or always-on pit machine, so a robot's matches reach the lake within minutes of it coming into range.

## ADDED Requirements

### Requirement: One-shot and watch modes
The acquire command SHALL support two modes. One-shot runs a single cycle and exits. Watch repeats the cycle every poll interval (default 30 s) until stopped. A cycle SHALL process, in order: robot hosts, removable volumes, ingest and derive of the inbox, then backup. Stopping the watch SHALL let the current file transfer finish or discard it, and SHALL never leave a partial file under a final inbox name.

#### Scenario: One-shot with a robot in range
- **WHEN** acquire runs once with a reachable robot holding 3 new stable logs
- **THEN** the 3 logs are pulled, ingested, and derived, and the command exits 0

#### Scenario: Interrupt during watch
- **WHEN** the watch is interrupted mid-transfer
- **THEN** it exits within 5 s, and the inbox has no partial file under a final name

### Requirement: Dry run
A dry-run option SHALL report what would be pulled or copied, from which source, and its size. It SHALL NOT transfer, ingest, or back up anything.

#### Scenario: Dry run against a robot
- **WHEN** acquire runs with dry-run against a robot holding 2 new stable logs
- **THEN** it lists those 2 files with sizes, and the inbox and lake are unchanged

### Requirement: Inbox ingest
Each cycle SHALL ingest the inbox when it has new files, either from this cycle's pulls or from manual drops, and derive afterwards. Derive SHALL run as a separate process, so the watch's memory stays within the existing derive budget. Manually dropped files SHALL be ingested only once stable. An inbox file SHALL be removed once the ledger records it as `success`, `skipped`, or `quarantined` (raw storage holds the verified copy). If ingest or derive fails as a whole, inbox files SHALL stay and be retried next cycle.

#### Scenario: Manual drop
- **WHEN** a student copies a `.wpilog` into the inbox by hand
- **THEN** it is ingested once its size is stable, and removed from the inbox afterwards

#### Scenario: Quarantined file
- **WHEN** a pulled hoot is quarantined by ingest
- **THEN** it is removed from the inbox, its quarantine reason is in the ledger, and it is not pulled again from the robot

#### Scenario: Ingest crashes
- **WHEN** the ingest process exits abnormally without updating the ledger
- **THEN** the inbox files remain, and the next cycle retries them

### Requirement: Time to lake
In watch mode, a robot that comes into range holding a newly stable match log SHALL have that match's derived data in the lake within 5 minutes, with no human action.

#### Scenario: Robot plugged in after a match
- **WHEN** a robot holding one new stable qualification wpilog and its hoots becomes reachable
- **THEN** within 5 minutes the match appears in the lake's derived match tables

### Requirement: Single watcher per lake
At most one acquire process SHALL run against a given lake at a time. A second one SHALL exit non-zero with a message naming the lock holder. A lock left by a process that is no longer running SHALL be taken over.

#### Scenario: Second watcher
- **WHEN** acquire is started while another acquire is running on the same lake
- **THEN** the second exits non-zero, and the first is unaffected

#### Scenario: Stale lock
- **WHEN** a previous watcher was killed and left its lock behind
- **THEN** a new acquire takes over the lock and runs

### Requirement: Status reporting
After every cycle, the system SHALL write a machine-readable status to the lake's metadata directory. It holds the time of the last cycle, each source seen and its outcome, active warnings (such as `low-space`), failed files, and the last backup time and result. The health report command SHALL show this status.

#### Scenario: Health report after a low-space cycle
- **WHEN** the last cycle saw a robot below the free-space threshold
- **THEN** the health report shows the `low-space` warning with host and free bytes

#### Scenario: Failed files listed
- **WHEN** a file has been recorded as `failed`
- **THEN** the health report lists it with its source, path, and reason

### Requirement: Configuration
Acquisition settings SHALL be read from an `acquire.toml` in the configuration root. The settings are robot hosts and credentials, robot log roots, poll interval, free-space threshold, whether removable media is scanned, and the backup remote. Command-line options SHALL override file settings. Missing settings SHALL use the defaults in these specs. An invalid file SHALL stop acquire before any transfer, with a message naming the bad key.

#### Scenario: No config file
- **WHEN** no `acquire.toml` exists
- **THEN** acquire runs with the default hosts, roots, 30 s poll, 100 MB threshold, removable media on, and no backup

#### Scenario: Bad key
- **WHEN** `acquire.toml` contains an unknown key
- **THEN** acquire exits non-zero naming that key, and nothing is transferred

### Requirement: Service packaging
The project SHALL ship a Linux user-service definition that starts the watch at login and restarts it on failure. It SHALL ship a Windows scheduled-task definition that starts the watch at logon. Both SHALL invoke an installed `flashpoint` executable directly, with no shell compound commands and no display server dependency (#7).

#### Scenario: Linux service definition
- **WHEN** the shipped Linux service file is checked with the platform's service verifier
- **THEN** it reports no errors, and its start command is a single absolute executable invocation of the watch

#### Scenario: Windows task definition
- **WHEN** the shipped Windows task definition is imported
- **THEN** it creates a logon-triggered task that runs the watch
