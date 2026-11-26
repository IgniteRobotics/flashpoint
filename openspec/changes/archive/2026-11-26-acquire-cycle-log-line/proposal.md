## Why

Each acquire cycle ends with one summary log line, and that is the line students read in the terminal and in `journalctl`. Right now it prints Python dict reprs (`transfers {'verified': 3}; inbox {'success': 3}`), which is noise to anyone who doesn't write Python, and the auto-import guide quotes it as-is (#12, found while verifying project-documentation task 6.1).

Roadmap phase: P3 (Acquire & automate), `docs/rewrite/06-roadmap.md` § P3. No pitfall from `03-pitfalls-and-performance.md` applies; this is cosmetic.

## What Changes

- The `cycle done` line writes counts as `<n> <status>` pairs joined by `, `, for example `transfers 3 verified; inbox 3 success` or `transfers 2 verified, 1 retry; inbox 2 success, 1 quarantined`.
- The pairs come in a fixed order (the order the statuses are defined in), not in the order the files happened to finish, so two cycles with the same results print the same line.
- Nothing else changes: the source list, the ` (stopped)` marker, and the `none` / `empty` words for no transfers and an empty inbox stay as they are.
- The auto-import guide's example line is updated to match.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `acquisition-service`: adds a requirement for the per-cycle summary log line and its format. No existing requirement covers the log line today.

## Impact

- Code: `src/flashpoint/acquire/watch.py` (the cycle summary). No change to the status file, `doctor` output, exit codes, or the CLI.
- Docs: `docs/guide/how-to/auto-import.md`, the example under "Check it worked".
- Tests: a new test in `tests/acquire/test_watch.py` for the line.
- Anyone grepping logs for the old `{'verified': ...}` form needs the new form. We know of no such script.

## Non-goals

- Changing other log lines (backup, ingest, derive) or the level they log at.
- Structured (JSON) logging.
- Changing the status file or what `flashpoint doctor` shows.
- Renaming any status words (`verified`, `size-verified`, `retry`, ...).
