## Why

The current puller shells out to `sshpass scp`. It deletes robot logs after a copy it can't verify (#50), its date filter is broken (#52), and its inbox never connects to ingest (#9). Meanwhile WPILib and Phoenix rotate logs away at 50 MB free (#51), so a missed pull loses the data for good. Roadmap phase **P3**.

## What Changes

- An SFTP acquirer that pulls `.wpilog` and `.hoot` from roboRIO and USB log paths into the inbox, and verifies each file by size and hash
- **Never deletes on the robot** unless explicitly configured to
- Free-space monitoring with a warning before rotation is reached
- An inbox watcher that triggers ingest
- Packaging: a single multi-arch container or a systemd unit (no X11, valid `ExecStart`, #7)
- Backup of `lake/raw` and `lake/meta` to cloud storage (replaces `drive-backup.py`, #55)

## Capabilities

### New Capabilities
- `log-acquisition`: robot discovery, pull, verification, retention safety, free-space alerts
- `lake-backup`: backup and restore of the raw files and metadata

### Modified Capabilities
None.

## Non-goals

- Live telemetry or NetworkTables streaming
- Deleting or managing files on the robot

## Impact

New `src/flashpoint/acquire/`, `deploy/`. Retires `docker-services/`, `drive-backup.py`, `old_sync_scripts/`.
