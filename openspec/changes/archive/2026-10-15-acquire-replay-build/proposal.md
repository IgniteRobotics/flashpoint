## Why

The acquire watch ingests, derives and detects every new log on its own, but it never builds Replay data. A running `flashpoint serve` only builds Replay data when it starts. So at an event, History and Anomalies show a new match within minutes, while Replay doesn't list it until someone runs `flashpoint report` or restarts the server. That defeats the unattended pit loop P3 set up: plug in the robot, walk away, open the match. This is a follow-up to roadmap phases **P3** (log acquisition, docs/rewrite/06-roadmap.md §P3) and **P4** (Views, §P4), and keeps the "Time to lake" goal true for Replay as well as the derived tables.

## What Changes

- **The watch builds Replay data.** After derive succeeds, the acquire cycle runs an incremental `flashpoint report` as a separate process. One-shot `acquire` does the same; a dry run doesn't. Only matches whose derived inputs changed are rebuilt (the existing fingerprint rule), so a cycle with one new match costs about one match build.
- **Retried until it succeeds.** A failed or interrupted build stays owed and runs again next cycle, even after the watch restarts, exactly as a pending derive does. The watch status shows "Replay build pending" or the error.
- **One Replay build at a time.** `flashpoint report` takes a per-lake lock. A second build while one is running exits with the "locked" code (3, as `detect` does) instead of racing on the match index. `serve`'s start-up build skips quietly when the lock is held, and the watch leaves its build owed for the next cycle.
- **A running server shows the new match after a page reload.** It serves the files as they change on disk, and no restart is needed. Open pages are not updated live.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `match-reports`: adds "Replay data kept current by the acquire watch" (build after derive, retry, status) and "One Replay build at a time" (the lock).

The watch's own requirements live in `acquisition-service`, which is still only in the open `p3-log-acquisition` change (not yet archived to `openspec/specs/`). To avoid depending on P3's archive order, the new behaviour is specified under `match-reports`, which owns Replay data. design.md explains this.

## Non-goals

- Updating open pages without a reload (polling or server push). A separate, larger change.
- Building Replay data from `flashpoint ingest` or `flashpoint derive` themselves. Manual runs keep today's behaviour; `serve` still builds on start.
- Exporting the static USB site from the watch (`report --static`).
- Changing the Replay build's budget or what it builds.

## Pitfalls fixed

None from 03-pitfalls-and-performance.md.

## Impact

- `src/flashpoint/acquire/cycle.py`: a report step after derive, `report_pending`, and the build's outcome in `CycleResult`.
- `src/flashpoint/acquire/watch.py`: persists `report` pending/error in the status file, and `flashpoint doctor` prints it with the rest of the watch status.
- `src/flashpoint/cli.py`: `report` takes `meta/report.lock` and exits 3 when it is held; `serve`'s start-up build treats exit 3 as "already building".
- Tests: cycle and watch unit tests with fake commands; a CLI test for the lock.
- Docs: `docs/guide/auto-import.md`, `docs/guide/cli-reference.md` (report exit code 3), and a dated P3 follow-up note in the roadmap.
