## Context

- **Backup today** (`acquire/backup.py`, P3 `lake-backup`): `run_backup` copies `raw/` with `rclone copy --immutable`, then writes a metadata snapshot to `<lake>/tmp/meta-snapshot/` and `rclone sync`s it to `<remote>/meta/latest` with `--backup-dir <remote>/meta/history/<UTC>`. `restore` copies `meta/latest` and `raw` into a lake that has no ledger (or with `--force`) and clears pipeline versions so `rebuild` reprocesses everything. Anomaly triage lives in the ledger (ADR-0015), so it is already covered.
- **The watch's backup** (`BackupSchedule`): owed when a cycle changed raw or the ledger; runs once `interval_min` (default 15) has passed since the last attempt; outcome and `pending` live in the status file's `backup` slot.
- **Configuration** (`config.py`, `team-setup`): the root is an explicit path, then `FLASHPOINT_CONFIG`, then the per-user folder. Each file falls back to the bundled default (`DEFAULTS_ROOT`) when the root doesn't have it. The Setup editor saves atomically and keeps the last 20 versions of each file under `<root>/.backups/` (ADR-0017). The auto-import guide puts the service's environment file at `<root>/env`.
- Configuration is small: Ignite's is 8 files and under 50 KB. It doesn't touch full logs, so no new memory or time budget beyond "adds one small `rclone sync` to each backup".

## Goals / Non-Goals

**Goals:**
- The team's own configuration files survive the loss of the machine, with history.
- Restore can never destroy configuration that's already there.

**Non-Goals:**
- Versioning configuration beyond what the remote's history folders give (git is the tool for that, and Ignite uses it).
- Backing up the bundled defaults: they come back with the package.

## Decisions

### D-a. Configuration stays separate from the lake
Configuration stays in its own folder and is backed up **with** the lake, not moved **into** it.
- The lake is mostly rebuildable and machine-written. Configuration is small, hand-edited, and belongs in version control for teams that use it.
- One configuration serves several lakes (pit lake, restore drill, scratch lakes, the test corpus), and one lake is read with several configurations (threshold tuning with `backtest`). Inside the lake, each lake would carry its own copy and they would drift.
- `team-setup` just moved configuration to the per-user folder (a breaking change). A second move gains nothing.
- *Alternative:* configuration inside the lake (`<lake>/config/`). One folder to back up, but it loses the points above. Rejected.
- Consistent with D1 (lake = Parquet + ledger; nothing in ADR-0001 puts configuration in it) and D9 (per-user install). No ADR change.

### D-b. Allow-list, snapshot, then sync with history
- `snapshot_config(root, target)` copies the allow-listed files (`config_files`: top-level `*.toml`, `seasons/*.toml`, `robots/*.toml`, `units/*.csv`; no names starting with `.`) into `<lake>/tmp/config-snapshot/`, keeping their relative paths. Each folder is listed directly, so an unreadable folder is an error: `Path.glob` would skip it silently, and the sync would then drop its files from the remote's latest copy. Then `rclone sync --backup-dir <remote>/config/history/<UTC>` to `<remote>/config/latest`, the same pattern and history timestamp as metadata.
- A snapshot, rather than syncing the folder with rclone filters: the allow-list is tested in Python on every OS, the same set is hashed for D-c, and a save that happens mid-sync can't produce a half-copied set (each editor save is atomic, so each copied file is whole).
- An allow-list, not a deny-list: the folder is the user's, and `env` may hold anything. A new kind of configuration file must be added to the list on purpose. `theme.toml` (team-branding) is top-level TOML, so it's already included.
- Order: raw, then metadata, then configuration. A configuration sync failure fails the backup (recorded, retried) but runs after raw and metadata, so it never costs a log backup. Configuration runs even when the watch's raw copy stopped at `--max-duration` (it's tiny).
- An empty or missing root: no sync at all (an empty `sync` would delete the remote's latest copy; history would keep it, but it's the wrong outcome). The summary says "no configuration files found".

### D-c. A configuration hash makes a backup owed
- `config_hash(root)` (SHA-256 over sorted relative paths and contents) is taken just before the backup and stored as `config_sha` in the status file's `backup` slot when it succeeds. Taken before, so an edit during the backup can only cause one extra backup, never a missed one.
- `BackupSchedule` computes the current hash each cycle (a few files, well under a millisecond) and marks the backup `pending` when it differs. The interval rule is unchanged.
- A manual `flashpoint backup` records the hash the same way, so the watch doesn't repeat it.
- *Alternative:* back up configuration on every cycle. Rejected: it adds an rclone call every cycle to a remote that may be slow at a venue.

### D-d. Restore copies configuration in only when the folder is empty
- Restore copies `<remote>/config/latest` into `<lake>/tmp/restore-config/` first (the staging pattern metadata uses).
- If the configuration root has no allow-listed files, the staged files move into the root (creating it). Otherwise they move to `<lake>/restored-config/`, which restore replaces each time, and the output names it.
- `--force` is about the lake's ledger only. Overwriting configuration takes a person copying files.
- `restore` takes the configuration root the same way the other commands do (`FLASHPOINT_CONFIG` or the per-user folder). The restore drill sets `FLASHPOINT_CONFIG` to a scratch folder so it exercises the empty-folder path without touching the real configuration.
- A remote without `config/latest`: rclone exits 3 ("directory not found") on local and Drive remotes, and copies nothing on bucket remotes. Restore allows exit 3 and treats it, or an empty copy, as "no configuration in this backup". (The proposal planned an `rclone lsf` check first; allowing exit 3 does the same in one call. Checked with rclone 1.75.2 by a corpus-marked test.)
- `restore` returns a `RestoreResult` (files, and where configuration went: "restored", "kept", "none") so the CLI can say which.

## Risks / Trade-offs

- [A broken configuration (bad TOML) is backed up as faithfully as a good one] → History keeps every earlier version; the guide says how to fetch one. Backup doesn't validate: refusing to back up a broken file would leave the remote's copy even further behind.
- [Robot files and serials go to the team's cloud drive] → Same remote and access as the raw logs, which already hold the serials (CAN inventory). No new exposure.
- [History grows without bound] → Configuration is tiny, and nothing prunes metadata history either. The guide already says to delete old folders by hand.
- [Two machines backing up to the same remote with different configurations overwrite each other's latest] → The same is true of metadata today. History keeps both. One remote per pit laptop is the documented setup.
- [Ignite's `FLASHPOINT_CONFIG=config` points into a git checkout] → Backed up like any folder. A restore into a checkout that already has files never overwrites (D-d).

## Migration Plan

- No format changes. Existing remotes gain `config/latest` and `config/history/` at the next backup.
- A status file without `config_sha` counts as "configuration not backed up yet": the first watch cycle after upgrading owes a backup.
- Rollback: an older version ignores `config/` on the remote and the extra status key.
