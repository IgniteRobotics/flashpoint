## Why

Two ingest bugs lose matches at events. When a laptop can't download owlet (no internet at a venue), the hoot is quarantined, and every later `flashpoint ingest` skips it and reports it as `skipped` (#14). The file was fine; the machine wasn't. And when a match changes after its Replay data was built (here, the missing wpilog arriving after the hoots), Replay keeps showing the old "No aligned samples" data until someone runs `flashpoint report`, which nothing tells them to do (#15). GAGAI1 Q4 hit both on 2026-10-16.

This is a follow-up to roadmap phases **P1** (core readers and lake, docs/rewrite/06-roadmap.md §P1: idempotent, resumable ingest with quarantine) and **P4** (views and reports, §P4). It narrows the "Corrupt tails" domain pitfall (docs/rewrite/03-pitfalls-and-performance.md §6): quarantine is for files that are bad, not for machines that failed.

## What Changes

- **Quarantine only bad files.** Read errors that come from the file (`empty-file`, `invalid-header`, `too-old`, `unsupported-compliancy:*`, `unsupported-version`, `no-signals-for-profile`) still quarantine it for good.
- **New `failed` stage for machine errors, retried next time.** owlet missing, download failed, checksum mismatch, or no build for this platform, plus owlet crashes, timeouts and internal errors, leave the file at `failed` with its reason. The next `ingest`, `rebuild` or acquire cycle tries it again. Crashes, timeouts and internal errors, which could still be the file's fault, are tried 3 times and then quarantined. Machine errors are tried again every time.
- **Existing lakes are fixed on upgrade.** Files already quarantined for a machine reason move to `failed`, so the next ingest picks them up.
- **Honest re-run output.** A file quarantined in an earlier run is reported as `quarantined earlier (<reason>)` instead of `skipped`. A failed file is reported as `failed (<reason>), will retry`. The summary line counts both. The exit code is non-zero when anything failed or was quarantined in this run.
- **Manual retry.** `flashpoint rebuild --quarantined` reprocesses quarantined files from their raw copies. Use it after a fix, or for a crash that hit the 3-try limit.
- **`ingest` rebuilds Replay data.** After its derive succeeds, `ingest` (and `rebuild`) runs the incremental Replay build, the same build the acquire watch runs. `--no-report` turns this off, and `--no-derive` skips it too, so the acquire watch keeps building Replay data itself. If another build holds the lock, `ingest` says so and tells the user to run `flashpoint report` afterwards. This reverses a non-goal of `acquire-replay-build` ("manual runs keep today's behaviour"), because #15 showed the cost.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `log-ingest-ledger`: "Stage tracking" adds `failed`. "Quarantine instead of abort" is limited to file errors. New requirements cover retrying machine failures, reporting earlier quarantines, and `rebuild --quarantined`.
- `match-reports`: new requirement "Replay data rebuilt after a manual ingest".

## Non-goals

- Showing failed or quarantined files in the app or in `doctor`. That's Live import updates (#23) and the Import status view (#19).
- Retrying inside one run beyond what owlet already does (one retry per call, issue #4).
- Fixing the Linux owlet flake itself (#4).
- Updating open pages without a reload.

## Pitfalls fixed

None by number from 03-pitfalls-and-performance.md. This narrows the §6 "Corrupt tails" rule as described above.

## Impact

- `src/flashpoint/readers/hoot.py`, `readers/wpilog.py`: classify each error reason as file or machine (one table, next to the errors).
- `src/flashpoint/lake/ledger.py`: the `failed` stage, an `attempts` column (added to existing lakes on open), moving machine-reason quarantines to `failed` on upgrade, and `needs_processing` retrying `failed`.
- `src/flashpoint/ingest.py`: `_finish` routes errors to failed or quarantined. `FileResult` gets `failed` and `quarantined-earlier` statuses. `rebuild` gains a quarantined mode.
- `src/flashpoint/cli.py`: summary wording, `rebuild --quarantined`, and `--no-report` plus the report step after derive for `ingest` and `rebuild`.
- `src/flashpoint/acquire/cycle.py`: needs no change in behaviour, because a `failed` file is not done and stays in the inbox. Tests cover it.
- Tests: ledger, ingest (with a fake registry that fails), CLI, the acquire inbox, and an ingest → report → ingest regression for #15.
- Docs: `docs/guide/cli-reference.md` (ingest, rebuild), `docs/guide/user-guide.md` (quarantine wording).
