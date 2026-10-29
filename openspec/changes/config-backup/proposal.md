## Why

`flashpoint backup` copies the lake's raw files and metadata, and `restore` plus `rebuild` brings every match back. But `rebuild` derives silver and gold from the **configuration** (robot files, unit registry, season roots, thresholds), and nothing backs that up. Since `team-setup`, configuration lives in a per-user folder (`~/.config/flashpoint`, `%APPDATA%\flashpoint`), outside any repository. For a team that installed from PyPI, a lost pit laptop loses the one thing they can't get back from the robot: which device fills which slot, the motor serials and swap dates from the Tuner X walk, and their failure labels. Ignite has the repo's `config/`; other teams have nothing.

This is a follow-up to roadmap phase **P3** (log acquisition, docs/rewrite/06-roadmap.md §P3, "Backup") and to `team-setup` (the config home). Keeping configuration separate from the lake is decided in design.md (D-a).

## What Changes

- **Backup includes configuration.** Every backup (manual `flashpoint backup` and the watch) also copies the configuration files to `<remote>/config/latest`. Files it replaces or deletes move to `<remote>/config/history/<UTC time>/`, as metadata does, so a bad edit never destroys the only good copy.
- **Only configuration files.** TOML files at the top of the config folder and in `seasons/` and `robots/`, and CSV files in `units/`. Not the service's `env` file, hidden files, or anything else someone left in the folder.
- **A configuration change makes a backup due.** The watch backs up after a cycle that changed raw or metadata. It now also backs up when the configuration differs from what was last backed up, so editing a robot in the Setup view reaches the remote within one interval, even on a quiet day.
- **Restore brings configuration back, without overwriting.** `flashpoint restore` copies the backed-up configuration into the config folder only when that folder holds no configuration files. Otherwise it leaves the folder alone, puts the backed-up copy in the lake's `restored-config/` folder, and says so. Older backups without configuration still restore.
- **The restore drill checks configuration.** The guide's drill restores into a scratch lake and a scratch config folder, so practising never touches the real configuration.

## Capabilities

### New Capabilities

- `config-backup`: which configuration files are backed up, history on the remote, when a configuration change makes a backup due, and restore that never overwrites configuration.

### Modified Capabilities

None. Backup is specified in `lake-backup`, still only in the unarchived `p3-log-acquisition` change, so this gets its own capability (as `acquire-replay-build` did) and folds into `lake-backup` when P3 archives.

## Non-goals

- Moving configuration into the lake. Configuration stays in its own folder (design.md, D-a).
- Recording which configuration built each derive run (a config hash per session, and `doctor` flagging silver built from a different robot file). A useful follow-up, separate from backup.
- Merging a restored configuration into an existing one. Restore copies into an empty folder or leaves the copy beside the lake; comparing is up to the person.
- Encrypting the backup (the remote's access control applies, as for raw logs).

## Pitfalls fixed

None from 03-pitfalls-and-performance.md.

## Impact

- `src/flashpoint/acquire/backup.py`: a configuration snapshot beside the metadata snapshot, a second `rclone sync` with history, and the configuration step in `restore`.
- `src/flashpoint/acquire/backup.py` `BackupSchedule` and the status file: the hash of the last backed-up configuration, and "configuration changed" as a reason a backup is owed.
- `src/flashpoint/cli.py`: `backup` and `restore` messages; `restore` takes the configuration root.
- Tests: unit tests with a local-directory remote (the existing drill fixture), including the file filter, history, and restore never overwriting.
- Docs: `docs/guide/auto-import.md` (backup and the restore drill), `docs/guide/cli-reference.md`, and a dated P3 follow-up note in the roadmap.
