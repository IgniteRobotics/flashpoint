## 1. Last successful backup

- [x] 1.1 Write failing tests in `tests/acquire/test_backup.py`: a successful watch backup and a successful `flashpoint backup` set `last_ok` and `config_sha`; a failed one keeps the previous `last_ok` and `config_sha` and records the error in `result` (spec "Failed attempt keeps the last success"); a status slot without `last_ok` but with an `ok` result reads `last_time` as `last_ok`; `StatusTracker` carries `last_ok` across a restart. Implement it in `BackupSchedule`, `cli._backup` and `StatusTracker` (design D-b). Verify the backup and watch tests pass
- [x] 1.2 Write failing tests: after `restore` the new lake's status has `backup.last_ok` and the `config_sha` of the configuration now in the root (restored or kept), and nothing else from the source's watch state (spec "Fresh restore"). Implement it in `restore`/`cli._restore`. Verify the restore tests pass

## 2. Overdue rule

- [x] 2.1 Add `backup.overdue_h` (default 24, minimum 1) to `BackupConfig` with tests for the default, a set value and a value below 1. Verify the acquire config tests pass
- [x] 2.2 Write failing tests for `backup_status(lake, config_root, settings, now)` with a seeded ledger and status: hand-imported logs 30 h old and no backup (spec "Imported by hand, no watch"); last backup 3 days ago and one log 2 h old ("Recent logs only"); a robot file modified 2 days ago after `last_ok` ("Configuration edited after the last backup"); a deleted configuration file counts from `last_ok`; nothing changed for 7 days ("Nothing changed for a week"); a never-backed-up lake counts every log; microsecond and second timestamps compare correctly; a clock in the future gives a zero wait; an unreadable ledger is reported, not raised. Implement it in `acquire/backup.py` (design D-a). Verify the tests pass and one call on the corpus lake takes under 50 ms

## 3. Check, endpoint and badge

- [x] 3.1 Write failing tests in `tests/setup/test_checks.py` for the `backup` check: problem with count, wait and last error ("Offline for two days"), and `doctor` exits non-zero; warning when failing but not overdue ("Failing but not overdue"); warning when not configured and the lake has logs, pointing to `auto-import.md#backup-and-restore` ("Not configured"); ok on an empty lake ("Empty lake"); ok with the last success time; the fix command is `flashpoint backup`; the check never runs rclone. Add it to `run_checks` after the lake check (design D-c). Verify the check and doctor tests pass
- [x] 3.2 Write failing tests in `tests/web/` for `GET /api/backup/status`: level and message for an overdue, a failing and a backed-up lake; it runs no setup check commands (the runner isn't called); it's absent in a static export. Implement it in `web/server.py` (design D-d). Verify the API tests pass
- [x] 3.3 Add the Setup view's `badge` and a browser test: with an overdue lake the Setup tab shows the hot badge on Replay, History and Anomalies with the problem as its label; with a backed-up lake there's no badge ("Overdue at the shop", "Backed up"). Verify `pytest -m browser` passes

## 4. Docs and gate

- [x] 4.1 Update `docs/guide/auto-import.md` (the backup check, what "overdue" means, `overdue_h`, the Setup badge, the rollback note for `overdue_h`), `docs/guide/cli-reference.md` (`doctor` lists the backup check), and the P3 follow-up note in `docs/rewrite/06-roadmap.md`. Verify the docs link/drift tests pass
- [ ] 4.2 Run the full gate locally (ruff, ruff format, mypy, unit, corpus, browser), open a PR from `feature/backup-overdue` (into `main` once PR #30 merges, otherwise stacked on `feature/config-backup`), and verify `ci-ok` is green
- [ ] 4.3 [HUMAN] On the pit laptop: turn off Wi-Fi, import a match, set `overdue_h = 1`, and confirm the Setup badge and the `doctor` problem appear after an hour and clear after a backup with Wi-Fi back. Record the result in design.md
