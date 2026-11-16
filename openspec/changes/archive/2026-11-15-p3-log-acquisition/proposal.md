## Why

The current puller shells out to `sshpass scp`. It deletes robot logs after a copy it can't verify (#50), its date filter is broken (#52), and its inbox never connects to ingest (#9). Meanwhile WPILib and Phoenix rotate logs away at 50 MB free (#51), so a missed pull loses the data for good. Students also pull the robot's USB stick and carry it to the pit, and nothing picks those logs up. Roadmap phase **P3** (docs/rewrite/06-roadmap.md §P3; architecture in 05-target-architecture.md §2 and §5).

## What Changes

- A robot puller that copies `.wpilog` and `.hoot` from the rio's internal and USB log roots into the inbox over SFTP, verifying each file by size and SHA-256
- A removable-media source that finds robot USB sticks (or any removable drive) plugged into the pit machine on **macOS, Linux, and Windows**, and copies their logs into the inbox with the same verification
- **Never modifies the robot or the stick.** There's no deletion option at all (the earlier draft allowed one when configured; it was dropped as contradicting the non-goals)
- Logs still being written are skipped until stable; an include-active option pulls them anyway and tags them `incomplete-read`
- A free-space warning before rotation is reached (default threshold 100 MB)
- A watch loop: robots → USB volumes → ingest and derive the inbox (including manual drops) → backup, every 30 s. Status goes into `flashpoint doctor`
- Packaging: a Linux user systemd unit with a valid `ExecStart` (#7) and a Windows logon scheduled task. **The container is deferred** (ADR-0009 amended)
- Backup of `lake/raw` and `lake/meta` to any configured rclone remote, plus restore (replaces `drive-backup.py`, #55)
- A Windows CI job for the unit suite, and fixes for any P1/P2 Windows defects it finds

## Capabilities

### New Capabilities
- `log-acquisition`: robot discovery, pull, verification, pull ledger, never-modify, free-space alerts
- `removable-media-acquisition`: per-OS removable volume detection, scan, verified copy, safe-to-eject notice
- `acquisition-service`: one-shot and watch modes, inbox ingest, time-to-lake, locking, status, config, service packaging
- `lake-backup`: backup and restore of raw files and metadata

### Modified Capabilities
None.

## Non-goals

- Live telemetry or NetworkTables streaming
- Deleting or managing files on the robot or on removable media
- Container images (deferred; ADR-0009)
- Deduplicating a partial include-active pull against its later complete copy
- Multiple robots pulled concurrently (one robot is reachable at a time in practice)
- A Windows service wrapper; the scheduled task runs the watch at logon

## Impact

New `src/flashpoint/acquire/`, `deploy/systemd/`, `deploy/windows/`, `config/acquire.toml` (optional). New runtime dependency: an SSH/SFTP client library. Supersedes `docker-services/` and `drive-backup.py` (removed by `retire-legacy-code` stage 2c). `old_sync_scripts/` was already removed in stage 1.
