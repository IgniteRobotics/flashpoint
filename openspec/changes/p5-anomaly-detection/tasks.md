## 1. Configuration and storage

- [x] 1.1 Write failing tests for `config/anomaly.toml` loading: defaults, unknown key (`drift_z_score`) rejected naming the file and key, a negative threshold rejected, strict above normal rejected. Then implement the loader in `src/flashpoint/anomaly/settings.py` and ship the default file; verify `pytest tests/anomaly/test_settings.py` passes
- [x] 1.2 Write failing tests for `[[siblings]]` in the robot config (spec robot-config: 2026 groups, unknown slot, fewer than two, slot in two groups, duplicate name, no groups). Then extend `robot_config.py` and declare `drive`, `steer`, `flywheel`, `intake-roller`, and `intake-extension` in `config/robots/2026-comp.toml`; verify the robot-config tests pass
- [x] 1.3 Write failing tests for `config/anomaly-causes.toml` and `config/failure-labels.toml` validation (bad slot `drive-xx`, missing unit and slot, bad match key). Then implement the loaders and seed both files: causes from the canvas examples that apply to the logged metrics, and labels for Johnson Q105 (roller leader reboot, extension follower degradation) and GACMP Q7 (brownouts with intake stalls); verify the tests pass and the seeded labels resolve to a robot and slot
- [x] 1.4 Write failing tests for the `anomaly_triage` ledger table: it is created by migration on an existing ledger, it is append-only (the current state is the last row), and it survives `rebuild` and a `PIPELINE_VERSION` change. Then implement it in `lake/ledger.py`; verify `pytest tests/lake` passes

## 2. Detection inputs

- [x] 2.1 Write a synthetic gold builder in `tests/anomaly/gold_builder.py` (per-unit series → `match_features`, `can_health`, and `unit_usage` partitions, with options for swaps, missing rows, and low alignment); verify a round-trip test reads back the series in match order
- [x] 2.2 Write failing tests for the input frame: whole-match rows only, a `match_features` × `can_health` join, ordering by `session_start` then `match_key`, missing values kept as null per metric, and skipped rows counted by reason. Then implement `anomaly/frame.py` with DuckDB (`threads=2`, `con.execute` with params); verify the tests pass
- [x] 2.3 Write failing tests for the brownout cache: the count equals Replay's brownout FAULT markers for Q7; only matches whose silver fingerprint or thresholds changed are recounted; one match's silver at a time. Then implement `anomaly/power.py` reusing `report.markers.power_markers`; verify the tests pass, including the corpus Q7 test
- [x] 2.4 Measure the uncached brownout count on Q7 (time and peak RSS) and record it in design.md Budgets; verify it is ≤ 0.5 s, or record the deviation with its cause

## 3. Per-unit baselines (DRIFT and STEP)

- [x] 3.1 Write failing tests for the baseline at index *i*: window, median, 1.4826 × MAD, per-metric floors, `min_baseline` (5), the restart index, and no lookahead (appending values never changes earlier z). Then implement `anomaly/baseline.py`; verify the tests pass
- [x] 3.2 Write failing tests from the spec scenarios Sudden step, Slow drift, One bad match, Swapped-in unit, and Building baseline (3 of 5). Then implement the STEP and DRIFT flags with strict and normal thresholds; verify the tests pass
- [x] 3.3 Write failing tests for episodes: Episode continues (21, 22, 23, 25 → one anomaly, 4 matches), Episode ends (two ids), stable sha1 ids, and sensitivity tagging (the Strict-only finding scenario keeps one id across sensitivities). Then implement `anomaly/episodes.py`; verify the tests pass

## 4. SPEC rules and sibling comparison

- [ ] 4.1 Write failing tests for the SPEC table: hot motor 66 °C → MED with "< 65 °C"; 75 °C → HIGH; mean 55 °C; stall 3 s; reboot while enabled → HIGH, otherwise MED; isolated gap 0.25 s / 1 s; brownout ≥ 1 → HIGH on `power`; severity is the episode maximum. Then implement `anomaly/spec_rules.py`; verify the tests pass
- [ ] 4.2 Write failing tests for sibling comparison: One drive motor runs hot, Whole group warms together, Missing member, and the pair rule (the anomaly goes to the member further from its own baseline, with the peer named). Then implement `anomaly/siblings.py`; verify the tests pass

## 5. Anomalies table, explanations, and commands

- [ ] 5.1 Write failing tests for severity (z ≥ 6 HIGH, ≥ 4 MED) and confidence (Low, Medium, High), and for explanation text and evidence (Evidence for a step: points 1–23, band, flagged 21–23; SIBLING carries the peer series; text is stored as plain strings). Then implement `anomaly/explain.py`; verify the tests pass
- [ ] 5.2 Write failing tests for the table writer: atomic write of `gold/anomalies/anomalies.parquet` and `summary.json`, row-identical reruns, the fingerprint skip, a configuration error leaving the previous table in place, and baseline restarts read from triage. Then implement `anomaly/detect.py`; verify the tests pass
- [ ] 5.3 Write failing CLI tests for `flashpoint detect` (exit codes, summary line with anomalies by severity and building-baseline count, `meta/detect.lock` held → `EXIT_LOCKED`). Then wire it into `cli.py` with deferred heavy imports, and extend the import guard test; verify `pytest tests/test_cli.py tests/acquire/test_acquire_imports.py` passes
- [ ] 5.4 Write failing tests that derive runs detection when gold changed, and that `run_cycle` runs detect when `restart_pending` is set. Then implement both; verify `pytest tests/semantics tests/acquire` passes
- [ ] 5.5 Write corpus tests: Q7 gives exactly one HIGH SPEC brownout anomaly on `power` for `2026-comp`; a synthetic Q105 copy gives the HIGH reboot and MED longest-gap anomalies at `2026johnson_qm105`; the full corpus, including truncated and quarantined files, completes with skip counts; a lake without raw gives identical results. Verify `pytest -m corpus` passes
- [ ] 5.6 Write the detection budget test (synthetic 60 × 23 season, second run: < 5 s, < 400 MB via VmHWM/ru_maxrss as the existing perf tests do), and check that derive + detect on Q7 stays inside the 30 s / 1 GB ingest budget; verify the perf tests pass

## 6. Optional model detector

- [ ] 6.1 Add the Poetry extra `ml` (scikit-learn). Write failing tests: off by default (no sklearn import, checked via `sys.modules`); enabled but missing → one warning, other anomalies unchanged; deterministic flags; MED cap and `normal` tag; training uses only earlier events; explanations name the three features with the largest |z|. Then implement `anomaly/model.py`; verify the tests pass with and without the extra installed (CI installs it in one job)

## 7. API and triage writes

- [ ] 7.1 Write failing API tests: `GET /api/anomalies` (tab, sensitivity, unit, and match filters; counts; `scanned_through`; building baseline; `restart_pending`), `GET /api/anomalies/<id>` (evidence, causes resolved at read time, "No causes recorded for this pattern yet", seen-before from closed anomalies on the same slot or unit, triage history), and `GET /api/anomalies/summary`. Then implement them in `web/api.py` and `views/queries.py`; verify `pytest tests/web tests/views` passes and each call answers in < 500 ms on the synthetic season
- [ ] 7.2 Write failing tests for `POST /api/anomalies/<id>/triage`: the transition table, outcome required and at most 500 characters, refused when bound to the network, refused for a foreign Origin or a non-loopback client, non-JSON or > 4 KB bodies refused, append-only history, and "no longer detected" for orphaned ids. Then implement it reusing `advantagescope.refusal`; verify the tests pass
- [ ] 7.3 Write failing tests that a restart-baseline dismissal spawns a detached `flashpoint detect` (stubbed spawn), reports `restart_pending` until detection consumes it, and the Gearbox change scenario (no new STEP or DRIFT on that metric, other metrics unchanged). Then implement; verify the tests pass
- [ ] 7.4 Extend the Offline and read-only test into Triage is the only write (raw, bronze, silver, and gold byte-identical; one new triage row). Add a backup and restore drill assertion that triage rows come back; verify `pytest tests/web tests/acquire` passes, and the corpus restore drill passes

## 8. Views

- [ ] 8.1 Write failing Playwright tests for the Anomalies view: Queue from the corpus, Sensitivity, Nothing open (with building-baseline count), Link keeps the selection, Evidence point opens Replay, and the subject opening History on the metric. Then build `web/static/views/anomalies.js` to the canvas board with shell tokens and uPlot; verify the browser tests pass
- [ ] 8.2 Write failing Playwright tests for the triage UI: Investigate then resolve, Close without a reason, Hostile outcome text, restart-baseline pending notice, actions absent when shared or static, and keyboard reachability with a visible focus outline. Then implement; verify the browser tests pass
- [ ] 8.3 Write failing Playwright tests for the navigation badge (count, HIGH emphasis, refresh after triage) and the absence of Anomalies in static mode. Then implement the shell badge slot; verify the browser tests pass
- [ ] 8.4 Write failing Playwright tests for Replay badges: Served badge, Resolved anomaly drops its badge (data files unchanged), the battery track for `power`, and Detection does not rebuild Replay. Then implement in `replay.js`; verify the browser tests pass
- [ ] 8.5 Write failing tests for the static snapshot: `report --static` writes `anomalies-snapshot.js` filtered to the exported matches with `taken_at`; offline open shows badges without links; no external host in any file. Then implement in `report/export.py`; verify the report and browser tests pass
- [ ] 8.6 Write failing Playwright and query tests for History: Health from an anomaly (HOT from a HIGH SIBLING), Closed anomalies do not count, and the row link to Anomalies filtered to the unit. Then implement; verify the tests pass and the Season-scale refresh test still runs in < 2 s

## 9. Backtest

- [ ] 9.1 Write failing tests for `flashpoint backtest`: Equals live detection (truncated lake at 20 matches), Triage ignored, Q105 caught with lead time 0, the degradation label missed with "building baseline (3 of 5)", Label outside the lake → not evaluable, empty lake message, byte-identical JSON, FP per method per 10 robot-matches, and the per-sensitivity breakdown. Then implement `anomaly/backtest.py` and the CLI command; verify the tests pass
- [ ] 9.2 Write the two-season backtest budget test (120 × 23: < 60 s, < 1 GB); verify it passes
- [ ] 9.3 [HUMAN] After adding the remaining 2026 logs to `~/flashpoint-lake`, run `flashpoint detect` and `flashpoint backtest`. Add any further known failures to `config/failure-labels.toml`, and review the per-method false-positive rate. Record the results, and any threshold changes, in design.md

## 10. Docs, gate, and hand checks

- [ ] 10.1 Write ADR-0015 (anomaly triage writes; amends 0013 and 0014). Set ADR-0010 to Accepted with amendments (MODEL optional and off by default; no SPEC limit on the residual; NOISE, STUMPY, and River deferred; brownouts from the battery proxy). Update the server docstring about non-GET routes. Verify the ADR index lists 0015
- [ ] 10.2 Update docs/rewrite: 05 §6 D10 row (decided, with the amendments) and 06 §2 P5 (status, what shipped, deferred items: webhook, NOISE, STUMPY/River, spin-test and battery metrics). Update the README command list with `detect` and `backtest`; verify the links resolve
- [ ] 10.3 Run the full gate (unit, corpus, browser, perf) locally, then open a PR into `rewrite` (stacked on PR #40 if it is still open); verify CI is green on all jobs
- [ ] 10.4 [HUMAN] Serve `~/flashpoint-lake`. Check that the Anomalies queue, evidence, and why-text for the Q105 roller reboot read correctly; triage one anomaly through investigate → resolve; dismiss one with restart baseline and confirm the pending notice clears after detection runs. Confirm the Replay and History badges match the queue
