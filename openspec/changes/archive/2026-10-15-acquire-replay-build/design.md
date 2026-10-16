## Context

See proposal.md for why. Current state:

- `run_cycle` (`src/flashpoint/acquire/cycle.py`) runs robot → volumes → inbox ingest (`flashpoint ingest --no-derive`) → derive (`flashpoint derive`, which also detects) → detect if a baseline restart is pending → backup. Each subprocess runs in its own process group through `run_subprocess(cmd, stop)`. `derive_pending` carries a failed derive into the next cycle, and the watch status file persists it (`watch.py`, `"derive": {"pending", "error"}`).
- `flashpoint report` (`ReportBuilder.build`) is incremental by fingerprint, and writes every data file, the index and `site-state.json` atomically. It has no lock. `serve` runs it as a subprocess on start unless `--no-build`.
- `detect` already guards itself with `AcquireLock(meta/detect.lock)` and exits `EXIT_LOCKED` (3) when it is held. The cycle treats 3 as "still pending, not an error".
- The served app reads `report/data/*` from disk on every request (`server._data`), so new files show up immediately. The browser keeps the index for the page's life (`FP.loadIndex`), so a new match appears after a reload.

D6 (Replay with a static export) and D7 (one local app) are unchanged. This change only adds a trigger for the existing build. Budgets: the `match-reports` "Build budget" (under 5 s and 500 MB per match) applies per rebuilt match. The build runs in its own process, so the watch's own memory doesn't grow. "Time to lake" (5 min) gains about one match build per new match.

## Goals / Non-Goals

**Goals:**
- A match the watch derives is in Replay without a human.
- No two builds write the same lake at once.
- Failures are visible in the watch status (`flashpoint doctor`) and retried, like derive.

**Non-Goals:**
- Live page updates; building from `ingest` or `derive`; static export from the watch.

## Decisions

### 1. Trigger in the cycle, not in `derive`

The cycle adds a `_report_step` after the derive and detect steps, run when `result.report_pending` is set. `report_pending` becomes true when derive exits 0 in this cycle, or comes in from the previous cycle (passed in as `derive_pending` is). It is cleared when the build exits 0. The step is skipped when the stop is set or in a dry run. It runs the build only if `meta/sessions.parquet` exists; otherwise there's nothing to build, and the debt is cleared.

*Alternative:* have `flashpoint derive` call the report build at its end, as it calls detect. Rejected: every manual `ingest` would get slower, the ingest perf budget is already over (Q7 33.7–35.9 s against 30 s), and `serve` builds on start anyway. The watch is the unattended path that needs it.

### 2. Report as a subprocess, with the same retry shape as derive

`report_command(lake) = [python, -m, flashpoint, report, --lake, root]`, injectable like `derive_command`, for tests. The result goes into `CycleResult.report: DeriveOutcome | None` (reusing the returncode-plus-output-tail shape), with `report_pending: bool`. An exit that isn't 0 or 3 adds `report: exit N` to `result.errors` and leaves the build pending. Exit 3 (locked) leaves it pending with no error. The step runs at most once per cycle, so a persistent failure (for example a robot config error) costs one attempt per poll interval, as derive does today.

### 3. Status file and `doctor`

`watch.py` persists `"report": {"pending": bool, "error": str | None}` next to `"derive"`, loads it on start (so a debt survives a restart), and `describe_status` (what `flashpoint doctor` prints) shows `Replay build error: …` until a build succeeds and `Replay build pending` while one is owed, both at once after a failure (the spec's "Build fails" scenario). The error is kept until a build succeeds, with the same tail length as derive's (`DERIVE_ERROR_CHARS`). An older status file without the key reads as not pending.

### 4. A lock on `flashpoint report`

`_report` wraps `builder.build(...)` (and the static export, so an export never reads a half-built index) in `AcquireLock(lake.meta / "report.lock")`. `LockHeldError` prints "a Replay build is already running on this lake (lock: …)" and returns `EXIT_LOCKED`. The lock is taken after the configuration checks, so a usage error still exits 2. The lock is an OS lock that the OS drops when its holder dies, so a killed build can't wedge the lake (the stale-lock scenario).

`serve`'s start-up `subprocess.run(build)` already ignores the return code (`check=False`), so a held lock just means the server starts on the existing data. No code change is needed there beyond a test that pins it.

*Alternative:* no lock; rely on atomic writes. Rejected: two builds each write `site-state.json` and the index from their own view, so the later writer can drop the other's new matches from the index until some later build. That is rare, but it shows up as exactly the bug this change fixes (a match missing from Replay).

### 5. Spec placement

The watch's requirements (`acquisition-service`) exist only as a delta in the open `p3-log-acquisition` change, which is waiting on [HUMAN] 11.x checks. A delta against that capability here would need P3 archived first, or would create the main spec with only this requirement in it. So the behaviour is specified under `match-reports` (it is about keeping Replay data current), and P3's "One-shot and watch modes" list (robot → volumes → ingest/derive → backup) isn't edited. When P3 archives, its list is still true; this change's requirement adds the build step between derive and backup. The roadmap note records this, so whoever archives P3 can fold the step into `acquisition-service`'s list.

## Risks / Trade-offs

- [A large rebuild (config change → every match's fingerprint changes) makes one cycle long] → it runs in the cycle like derive. Pulls wait for it, but a stop ends it, and the next cycle continues incrementally from what was written. Measured on the full lake as task [HUMAN].
- [A watch build holds the lock while the user runs `flashpoint report` by hand] → the manual run exits 3 with a clear message. Run it again after the cycle.
- [The browser needs a reload] → stated in the spec and the auto-import guide. Live updates are a separate change.

## Migration Plan

None. The status file gains an optional key; older files read as not pending. Rollback is reverting the PR.
