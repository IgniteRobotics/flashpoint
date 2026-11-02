## Context

See proposal.md (Why) and issues #14 and #15.

- `Ingestor._finish` (`src/flashpoint/ingest.py`) quarantines on any `HootError`/`WpilogError` and on any other exception (`internal-error:*`). `Ledger.needs_processing` treats `quarantined` at the current `PIPELINE_VERSION` as final, so later runs report the file as `skipped`.
- Error reasons are plain strings raised in `readers/hoot.py` (owlet resolution, download, run) and `readers/wpilog.py` (header). Nothing records which ones are about the file.
- The raw copy is stored before the file is decoded (`raw.store` in `_process`), so every quarantined or failed file can be retried from raw without the original.
- `cli.py` runs `derive` as a subprocess after ingest when anything succeeded, and ignores its exit code. Replay data is built only by `serve` at start, by `flashpoint report`, and by the acquire cycle (`acquire-replay-build`), all under `meta/report.lock` (exit 3 when held).
- The acquire cycle runs `ingest --no-derive`, then `derive`, then `report`, each as its own process. It clears an inbox file only when the ledger stage is `success` or `quarantined` (`_DONE_STAGES`). Anything else stays in the inbox for the next cycle.
- The Replay build's per-match fingerprint hashes each session's `derived_state` fingerprint. A re-derived session gets a new fingerprint, so an incremental build picks up the change (the #15 sequence) without `--all`.

## Goals / Non-Goals

**Goals:**
- Sort errors by cause in one place, so adding a new error reason means deciding which kind it is.
- Fix lakes that already hold wrongly quarantined files, with no command to run.
- Keep the acquire watch's behaviour and memory the same.

**Non-Goals:**
- Showing failure state in the app or in `doctor` (#19, #23).
- Retrying inside one run. owlet's own retry (issue #4) stays as it is.

## Decisions

### 1. A new `failed` stage instead of reusing `received`

The issue suggested leaving machine failures at `received`. A separate `failed` stage keeps the reason and the attempt count visible in `meta/files.parquet` and in SQL. `received` means "interrupted" today, and it carries no reason. `failed` is not in `_DONE_STAGES`, so the acquire inbox keeps the file with no change to `cycle.py`.

*Alternative:* keep `quarantined` and have `needs_processing` look at the reason. Rejected: "quarantined" would then mean two things, and the ledger snapshot would still say quarantined for a file about to be retried.

### 2. Reasons sorted into three kinds by a table next to the errors

A module-level mapping in `readers/` names each reason's kind: `file`, `environment`, or `suspect`. Prefixed reasons (`unsupported-compliancy:*`, `internal-error:*`) are matched by prefix. An unknown reason counts as `suspect`, so a new error retries a few times rather than locking a file out for good or retrying forever. A unit test fails if any `HootError`/`WpilogError` reason raised in `readers/` is missing from the table (found by scanning the source for literal reasons).

- `file` → quarantine now.
- `environment` (owlet missing, download, checksum, platform) → `failed`, retried with no limit. Each try fails fast (no network: one request; no platform build: no request at all), so retrying every acquire cycle is cheap.
- `suspect` (`owlet-failed`, `owlet-timeout`, `internal-error:*`) → `failed`, quarantined on the 3rd attempt. `owlet-failed` is usually the Linux flake (#4), but a damaged hoot can also crash owlet every time. Without a limit the watch would re-run owlet on it every cycle, forever, and exit non-zero each time.

*Alternative:* a time-based backoff. Rejected: it adds state and makes test behaviour depend on the clock. Counting runs is enough.

### 3. `attempts` column and an on-open migration

`files` gains `attempts INTEGER NOT NULL DEFAULT 0`. On open, the ledger checks `PRAGMA table_info(files)` and runs `ALTER TABLE ... ADD COLUMN` when the column is missing. It then runs one idempotent `UPDATE` that moves `quarantined` rows with an environment or suspect reason to `failed` with `attempts = 0`. Both run inside the existing open, which already executes `SCHEMA`. They don't bump `PIPELINE_VERSION`, because no derived output changes.

`attempts` goes up on each `failed` write, and resets to 0 on `success`, on a pipeline-version change (in `rebuild`), and in `rebuild --quarantined`.

### 4. Manual retry lives on `rebuild`, not `ingest`

The issue suggested `ingest --retry-quarantined`. `ingest` needs the original paths, but `rebuild` already reprocesses from raw storage, which is what a student who cleared the robot USB stick still has. So `rebuild --quarantined` selects quarantined files, and plain `rebuild` also picks up `failed` files. `ingest` keeps its contract: process what you're given.

### 5. Report statuses on `FileResult`

`FileResult.status` gains `failed` and `quarantined-earlier`. Choosing `quarantined-earlier` needs the stage when the file was registered, which `needs_processing` already reads, so it returns the stage instead of a bool (or a second query; the implementer can choose). `IngestReport.exit_code` is 1 when `failed` or `quarantined` is non-zero. `quarantined-earlier` doesn't count, so re-running over a folder that holds a known-bad file still exits 0, as the "Re-ingest is a no-op" scenario requires.

### 6. `ingest` runs the Replay build as a third subprocess

After `derive` exits 0, `cli.py` runs `python -m flashpoint report --lake <root>` with `check=False`, the same command the acquire cycle uses. On exit 3 it prints the "already running, run `flashpoint report` after" line. On any other non-zero exit it prints the tail of the build's output. The ingest exit code doesn't change, which matches how derive failures are handled today.

Gating: only when derive ran (something succeeded, no `--no-derive`) and `--no-report` is not set. `rebuild` gets the same step, because `derive --all` can change every match.

Running it as a separate process keeps peak memory at max(ingest, derive, report), not their sum. That's the same reason derive is already a subprocess (measured 1155 MB vs 870 / 589 MB). No new memory budget is needed. Time cost is the incremental build: about one match build per changed match, already measured under the `acquire-replay-build` budget.

*Alternative:* only print "Replay data is out of date; run `flashpoint report`". Rejected: #15 happened because a student had to know to run a second command. A printed hint is easy to miss.

### Decisions D1–D11

No deviation. D4 (owlet fetched and cached by checksum) is why the environment reasons exist. This change only stops treating them as a verdict on the file. No ADR is needed.

## Risks / Trade-offs

- [A file that is truly bad gets an `environment` reason and is retried forever] → Each environment retry fails before owlet reads the file, so the cost is one quick check per run, and the summary shows it as failed every time. The classification test keeps the table complete.
- [Moving old quarantines to `failed` re-runs files that were quarantined for a reason the team had accepted (e.g. a known owlet crash)] → Suspect reasons get 3 more attempts and then quarantine again. That is bounded, and it happens once.
- [`ingest` gets slower by one incremental build] → Usually seconds. `--no-report` opts out, and acquire is unaffected (`--no-derive`).
- [The incremental build misses the #15 change because the session id changed rather than its fingerprint] → A removed or new session changes the match fingerprint either way. The #15 regression test (task 4.3) runs the actual hoot-then-wpilog sequence.
- [#15 may not be the report at all; the issue notes "not yet confirmed that `flashpoint report` clears it"] → The regression test reproduces the sequence on the corpus. If Replay still shows no aligned samples after the build, the cause is alignment, and that task records it and splits a new issue instead of shipping a no-op.

## Migration Plan

- Automatic on first open of a lake by the new version: the column is added and machine-reason quarantines move to `failed`.
- Downgrade: older code ignores the extra column, and its `needs_processing` would retry `failed`. But its `Ledger.stage()` can't parse `failed`, and the acquire inbox step calls it, so a downgraded watch would error on those files. To roll back, revert the change and run `UPDATE files SET stage = 'received' WHERE stage = 'failed'` on `meta/flashpoint.sqlite`. The release notes must say so.
- Docs: cli-reference (`ingest` output and `--no-report`, `rebuild --quarantined`), user-guide quarantine wording.
