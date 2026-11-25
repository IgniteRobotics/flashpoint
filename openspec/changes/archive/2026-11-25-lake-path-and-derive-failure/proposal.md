## Why

`flashpoint ingest --lake lake logs/` with a relative lake path ingests every file. The automatic derive then crashes in detect (`ValueError: relative path can't be expressed as a file URI`), yet ingest prints its normal summary and exits 0. No Replay data is built, and nothing says so. Running the same ingest again skips every file, so derive never runs again. The only way back is `flashpoint derive --report`, and nobody is told that. A relative path is what a new user is most likely to type, so this is an MVP first-run problem (M1). Filed as issue #107.

The change continues P3 (Acquire & automate, `docs/rewrite/06-roadmap.md`) and the lake layout in `05-target-architecture.md` §4. It fixes no numbered pitfall from `03-pitfalls-and-performance.md`.

## What Changes

- Every command resolves the lake folder to an absolute path once, before anything uses it. `--lake`, `FLASHPOINT_LAKE` and a relative `lake` in `acquire.toml` all resolve against the current folder, as they do today, and `~` is expanded in all three.
- `flashpoint init --lake <relative>` already writes the absolute folder into `acquire.toml`; a test now pins that.
- `doctor` and every message that names the lake show the absolute folder.
- **BREAKING (exit code only):** when the derive that `flashpoint ingest` or `flashpoint rebuild` runs afterwards fails, the command prints that Replay data was not built. It names `flashpoint derive --report` as the retry, and it exits non-zero even if every file ingested. Nothing inside Flashpoint reads this exit code: the acquire watch ingests with `--no-derive`, and serve runs derive itself. Scripts that call `flashpoint ingest` may see a new non-zero exit.
- A failed Replay build after a successful derive keeps today's rule: an error message, and the ingest's own exit code.
- The support-matrix known-issue entry for #107 is removed.

## Non-goals

- Making `flashpoint ingest` re-run a derive owed from an earlier failed run. The message names the retry. Tracking owed derives like the acquire watch does is a larger change.
- Changing the acquire watch's derive and Replay retry, which already works.
- Changing how detect's own handled failures (configuration error, lock held) affect derive. They still do not fail the derive.
- Moving or migrating existing lakes, or rewriting relative `lake` values already in users' `acquire.toml`. They keep resolving against the current folder.
- Any change to the lake layout on disk.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `telemetry-lake`: "Lake location and portability" gains the rules for resolving a relative or `~` lake path from each source, and for showing it as an absolute path.
- `match-reports`: adds a requirement next to "Replay data rebuilt after a manual ingest": a failed post-ingest derive is reported and makes the command exit non-zero.

## Impact

- Code: `config.py` (`lake_root`, where every command's lake comes from), and `cli.py` (`_derive_and_report` reports a failed derive and returns it as the exit code, for `ingest` and `rebuild`).
- Crash sites `anomaly/restarts.py` and `views/anomalies.py` (`as_uri()` on the ledger) are fixed by the absolute root, not by local patches.
- Docs: remove the support-matrix known issue; say what a relative `lake` is relative to (`reference/config/acquire.md`); add the "derive failed" message to troubleshooting.
- Tests: new unit and CLI tests for path resolution and the failed-derive exit code, and an end-to-end check with a relative `--lake` on a corpus match.
- No new dependencies. No lake data change, so no `DERIVE_VERSION` or `PIPELINE_VERSION` bump.
