## Context

- The status file's `backup` slot (`acquire/watch.py` `StatusTracker`, written by `BackupSchedule` and `cli._backup`) holds `last_time`, `result`, `pending` and, since `config-backup`, `config_sha`. A failure overwrites `last_time` and `result`, so the last *successful* backup is lost.
- Only the watch marks a backup as owed. `flashpoint ingest` never does.
- The ledger's `files.first_seen` is an ISO-8601 UTC timestamp with seconds (`Ledger._now`), set once when a file is first registered.
- Setup checks (`setup/checks.py` `run_checks`) feed both `doctor` (exit 1 on any problem) and `GET /api/setup/checks` (the Setup view). Running them calls `systemctl`/`schtasks` and searches for AdvantageScope, which can take a second or more.
- The shell refreshes every view's nav badge on each route change (`FP.refreshBadges` in `shell.js`); Anomalies uses it for its open count.
- No full logs are read: one `MIN`/`COUNT` over `files` (thousands of rows at most) and one status file. Budget: under 50 ms per status request on the corpus lake.

## Goals / Non-Goals

**Goals:**
- One rule for "overdue", computed the same way for `doctor`, the Setup view and the badge.
- The badge costs one small request per navigation.

**Non-Goals:**
- Tracking metadata-only changes, or proving the remote still holds the files (see proposal).

## Decisions

### D-a. Overdue from the ledger and the configuration, not from `pending`
`backup_status(lake, config_root, settings, now)` returns the waiting logs (count and oldest `first_seen` after `last_ok`), whether configuration is waiting (current `config_hash` ≠ recorded `config_sha`; waiting since the oldest mtime among configuration files newer than `last_ok`, or `last_ok` itself if none is newer, as when a file was deleted), and whether that's past `overdue_h`.
- *Alternative:* `pending` plus the time it was first set. Rejected: only the watch sets it, so hand imports would never warn.
- Times are compared as parsed UTC datetimes, not strings, because the watch writes microseconds and the ledger doesn't.

### D-b. `last_ok` beside the attempt
- The backup slot gains `last_ok` (time of the last successful backup). Success sets it and `config_sha`; failure leaves both. `cli._backup` passes the previous slot's values through on failure, instead of clearing `config_sha` as `config-backup` does today.
- Migration: a slot without `last_ok` whose `result` starts with `ok` uses `last_time`. Without either, the lake was never backed up from this machine.
- Restore writes a fresh status with `backup: {last_ok: <now>, config_sha: <hash of the restored configuration, or of the current root when it was kept>}`. It's still a "fresh" lake (the source's watch state isn't copied), but its contents are known to be on the remote.

### D-c. The check reads recorded state only
- `_backup(ctx)` in `run_checks`, after `_lake`. It loads `acquire.toml` the way `_team` does (already loaded by `_files`; pass it in) and calls `backup_status`.
- Levels as in the spec. "Not configured" is a warning, not n/a: a team without a backup has no copy of anything, and the Setup view is where they'd learn that. It's only reported when the lake holds logs, so a fresh install isn't nagged.
- A ledger that can't be opened (missing, locked past the busy timeout, corrupt) gives a warning naming the error. `_lake` already reports a missing lake as a problem.

### D-d. `GET /api/backup/status` for the badge
- Returns `{"level": "ok"|"warning"|"problem", "message": ...}` from `backup_status` alone, read-only, no guard needed (like the other GET endpoints, ADR-0013).
- The Setup view registers `badge: async () => level === 'problem' ? { text: '!', hot: true, label: message } : null`.
- *Alternative:* badge from `/api/setup/checks`. Rejected: it runs every check on each navigation.
- *Alternative:* a banner on every view. Rejected for now: the badge is the shell's existing pattern, and a banner competes with Replay's own notices. Easy to add later if the badge is missed.

### D-e. `backup.overdue_h`
- In `acquire.toml` under `[backup]`, default 24, at least 1. The Setup view's editor saves through a `tomlkit` round-trip that keeps entries it doesn't edit (ADR-0017), so no editor change; the guide documents it.

## Risks / Trade-offs

- [An event weekend without internet makes the check a problem by Saturday evening, and `doctor` exits 1] → It's true: nothing from that weekend is backed up. The message says so plainly. A team that wants quiet raises `overdue_h`.
- [`first_seen` is when this lake registered the file, so re-ingesting a deleted lake makes old logs "new"] → They really aren't backed up from this lake; a backup copies only what's missing (`--immutable` copy), so it's cheap.
- [Clock changes (a laptop with a wrong clock) skew "how long"] → Times are this machine's UTC throughout; a negative wait counts as zero.
- [The badge hides a warning (failing, not overdue) to avoid nagging] → The Setup view's check list still shows it.

## Migration Plan

- Old status files work (D-b). No other format changes.
- Rollback: older versions ignore `last_ok` in the status. `BackupConfig` forbids unknown keys, so an older version refuses an `acquire.toml` that sets `overdue_h`; remove the line before rolling back. Only teams that changed the default are affected.
