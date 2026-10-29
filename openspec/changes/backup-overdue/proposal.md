## Why

When backups fail (no network at an event, an expired rclone token, a deleted remote), the watch records the error and retries every interval, and the only place it shows is one line in `flashpoint doctor`. Nobody runs `doctor` at the shop. A laptop can go weeks holding the only copy of new matches and configuration edits, and the team finds out when it dies. Backups only run from the watch, so a team that imports by hand with `flashpoint ingest` gets no backup and no warning at all. This is a follow-up to roadmap phase **P3** (log acquisition, docs/rewrite/06-roadmap.md §P3, "Backup") and to `config-backup`. It builds on that change's configuration hash.

## What Changes

- **A backup check in the setup checks.** It's shown by `doctor` and the Setup view like every other check:
  - **ok:** everything is backed up, or nothing has waited long. The message says when the last successful backup was.
  - **warning:** backup isn't configured, or the last attempt failed but nothing has waited past the limit. The message names the error.
  - **problem:** logs or configuration have been waiting for a backup longer than `backup.overdue_h` (default 24 hours). The message says how many logs, whether configuration changed, since when, and the last error. `doctor` exits non-zero, as it does for any problem.
- **Overdue is measured from the data, not the watch.** A log is waiting when the lake received it after the last successful backup. Configuration is waiting when it differs from what was last backed up. So a team importing by hand without the watch gets the same warning.
- **The last successful backup is recorded.** Every successful backup, from the watch or `flashpoint backup`, stores its time. A restore records the backup it came from, so a freshly restored lake doesn't report its whole history as unbacked.
- **The Setup tab shows a badge when backup is overdue**, on every view. Hovering it names the problem. It comes from a small read-only endpoint, so switching views doesn't re-run the slower checks.

## Capabilities

### New Capabilities

- `backup-status`: when a backup counts as overdue, the backup check and its messages, the recorded last success, and the Setup tab badge.

### Modified Capabilities

None. The setup checks are specified in `team-setup`, and backup in `lake-backup` and `config-backup`, none of them archived yet. The new behaviour gets its own capability and can fold in when those archive.

## Non-goals

- Notifications outside the app (email, Slack, desktop notifications).
- Counting metadata-only changes (anomaly triage) as waiting. Triage goes up with the next log or configuration backup.
- Running a backup from the Setup view (a fix button). The check shows `flashpoint backup` as the command. A fix action can come later.
- Checking that the remote is reachable from the check itself. The check reads recorded state and never touches the network.

## Pitfalls fixed

None from 03-pitfalls-and-performance.md.

## Impact

- `src/flashpoint/acquire/backup.py` and `src/flashpoint/acquire/watch.py`: `last_ok` in the status's backup slot, written on success and by restore; an overdue summary read from the ledger and status.
- `src/flashpoint/acquire/config.py`: `backup.overdue_h`.
- `src/flashpoint/setup/checks.py`: the `backup` check.
- `src/flashpoint/web/server.py`: `GET /api/backup/status`; `web/static/views/setup.js`: the tab badge.
- Tests: check and overdue unit tests, an API test, a browser test for the badge.
- Docs: `docs/guide/auto-import.md`, `docs/guide/cli-reference.md` (`doctor`), and the roadmap P3 follow-up note.
