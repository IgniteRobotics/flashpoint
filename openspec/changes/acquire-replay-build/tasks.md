## 1. One Replay build at a time

- [x] 1.1 Write failing tests in `tests/test_cli_report.py`: with `AcquireLock(meta/report.lock)` held by the test, `flashpoint report` exits 3, names the lock, and leaves the data files, index and `site-state.json` untouched; after the lock is released (holder gone), the build runs normally; a configuration error still exits 2 with the lock free. Then take the lock in `_report` (design §4). Verify the tests and the existing report tests pass
- [x] 1.2 Add a test that `serve`'s start-up build with the lock held still starts the server and serves the existing index (design §4, "Server starts during a watch build"). Verify it passes with no change to `_serve`, or make the smallest change that does

## 2. Build in the cycle

- [x] 2.1 Write failing tests in `tests/acquire/test_cycle.py` with a fake `report_command`: it runs after a successful derive and before backup; it doesn't run when derive fails, when there's nothing to ingest and nothing owed, or in a dry run; exit 1 leaves `report_pending` set with `report: exit 1` in `errors`; exit 3 leaves it pending with no error; an owed build runs on the next cycle with an empty inbox; a set stop skips it. Then add `report_command`, `_report_step`, `report`/`report_pending` to `CycleResult` and the `run_cycle` parameter (design §1, §2). Verify the cycle tests pass
- [x] 2.2 Write failing tests in `tests/acquire/test_watch.py`: the status file holds `report.pending`/`report.error`; a debt is loaded on restart and the first cycle runs the build; an older status file without `report` reads as not pending; `flashpoint doctor` (which prints the watch status; there is no `acquire --status`) shows "Replay build pending" and the error tail. Then wire it through `watch.py` (design §3). Verify the watch tests pass

## 3. End to end

- [x] 3.1 Extend `tests/acquire/test_acquire_e2e.py` (fake robot, real subprocesses): after one `acquire --once` that pulls a new match, `report/data/matches.js` lists it and its data file exists; a second `--once` with nothing new rebuilds nothing (the data file's bytes are unchanged). Add a browser test that a served app, started before the cycle, lists the new match in Replay after a reload. Verify both pass, and run the acquire corpus test (`-m corpus tests/acquire`)

## 4. Docs and gate

- [x] 4.1 Update `docs/guide/auto-import.md` (the cycle now builds Replay data; reload the page to see a new match; the status lines), `docs/guide/cli-reference.md` (`report` exit 3 and its lock), and add a dated follow-up note under P3 in `docs/rewrite/06-roadmap.md` that also records the spec placement (design §5) for P3's archive. Verify the docs link/drift tests pass
- [ ] 4.2 Run the full gate locally (ruff, ruff format, mypy, unit, corpus, browser), open a PR from `feature/acquire-replay-build` into `rewrite`, and verify CI is green on all blocking jobs
- [ ] 4.3 [HUMAN] At the pit (or on `~/flashpoint-lake` with the robot or a stick): run the watch beside `flashpoint serve`, bring in a new match, and confirm it appears in Replay after a page reload with no restart; note the cycle time with the build. Record the result in design.md
