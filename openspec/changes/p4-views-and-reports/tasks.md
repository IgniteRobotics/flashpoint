## 1. Stall threshold check (spec: unit-odometry; motor-physics)

- [ ] 1.1 Corpus spike: distribution of stator/stall-current ratio while |velocity| < 0.5 rps for the intake extension and intake roller on Q7 and E10; record the numbers in design.md's spike table
- [ ] 1.2 Tests first: synthetic stall at the chosen threshold counts; corpus test pins the Q7 stall result (non-zero if the evidence supports it, otherwise a documented zero); adjust `stall_current_fraction` only on evidence

## 2. Unit usage and thermal cycles (spec: unit-odometry)

- [ ] 2.1 Tests first: thermal-cycle state machine covers the spec cases (cool-down, powered off hot, small wobble, no temperature) plus configurable rise and fall thresholds
- [ ] 2.2 Implement thermal cycles in `semantics/usage.py`; add `thermal_rise_c = 10`, `thermal_fall_c = 3` to `PhysicsConfig`
- [ ] 2.3 Expose the enabled intervals (mode runs) from `framing.py`, with tests: practice session with several enable toggles, and a match session
- [ ] 2.4 Tests first: usage on synthetic silver covers powered-on time (with the hold cap and a gap), enabled time, energy, stall, a mid-session swap split, and a non-match session
- [ ] 2.5 Implement `usage.session_usage` and write `gold/unit_usage` (staged rename, like match features); wire it into the derive loop for every session with silver; bump `PIPELINE_VERSION` to 3
- [ ] 2.6 Corpus tests: Q7 usage rows exist for all 17 motors, usage supply energy ≥ match-phase feature energy, cycle counts match the spike (12 of 17 motors); measure added derive time (< 3 s) and peak memory (< 1 GB)

## 3. Lifetime queries (spec: lifetime-trends, unit-odometry)

- [ ] 3.1 Synthetic gold fixture builder: one season (60 matches × 3 phases × 23 slots) of match features and usage, plus slot observations including a swap and a robot move
- [ ] 3.2 Tests first for `views/queries.py`: filters (season, robot, event, match type, phase, subsystem, slot, unit) appear as SQL parameters and return only matching rows; a hostile filter value is treated as data
- [ ] 3.3 Implement the trend queries (per slot, per unit with slot changes marked) and sibling deviation (`(subsystem, model)` median, 25 % default)
- [ ] 3.4 Implement the unit queries: lifetime totals (sums and maxima, filterable by season and robot), slot history with identity source, closest-match lookup for unknown units, and the count of sessions without aligned samples
- [ ] 3.5 Perf test (`perf` marker): each view query on the synthetic season returns in < 2 s

## 4. Report data and envelopes (spec: match-reports)

- [ ] 4.1 Tests first for `report/envelope.py`: about 1000 buckets with width rounded to 10 ms, a 4 ms 150 A spike survives, empty buckets are null, rounding to 3 significant figures, temperature change points
- [ ] 4.2 Implement the envelope over one silver partition in DuckDB (memory limit 256 MB) plus the battery proxy
- [ ] 4.3 Tests first for `report/build.py`: match payload shape (header, per-phase slot table from gold, series, change points, "not logged" markers, alignment, source logs), embedded JSON escaping (`</`, U+2028/2029), 2 MB guard that drops the bucket count without dropping min/max
- [ ] 4.4 Implement the per-match build, writing `data/<match_key>.js` through a temp file and rename
- [ ] 4.5 Tests first for `report/site.py`: `site-state.json` fingerprints, incremental rebuild touching only changed matches, `--event` and match-key selection, an index that lists hoot-only and low-alignment entries with reasons, and a build summary counting sessions without a match key
- [ ] 4.6 Implement the site build: copy static assets, `matches.js` from state, and raw downloads (hard link or copy, `<match_key>__<source>__<name>`, `--no-raw` lists hashes)
- [ ] 4.7 Corpus tests: Q7 data ≤ 2 MB; every bucket's min and max equal the silver min and max over that bucket's range; raw download hashes equal the ledger hashes; build < 5 s and < 500 MB (perf)

## 5. Report front end (spec: match-reports)

- [ ] 5.1 Vendor Plotly 2.35.2 (from `archive/static-site`, with its license) and write `index.html`, `app.css`, and `app.js` with DOM-only rendering, porting the power-tracking UX (chips, compare, stats first, heatmaps, total power), plus the phase selector, slot detail charts with phase bands, hot-limit highlights from `config/report.toml`, the low-alignment banner, and the downloads panel
- [ ] 5.2 URL hash state for matches, phase, and view
- [ ] 5.3 Static lint test: `app.js` has no `innerHTML`, `outerHTML`, `insertAdjacentHTML`, inline `on*=` handlers, or `eval`; no file in the report references a remote host
- [ ] 5.4 Add `pytest-playwright`, the `browser` marker, and a CI job that installs Chromium
- [ ] 5.5 Browser tests (file:// with network blocked): index loads and Q7-like fixture charts render; hostile names show literally with no dialog or console error; URL restore; compare with an absent slot; "not logged" temperature; CSP behaviour recorded (fall back per design if Chromium rejects `'self'` on file://)

## 6. CLI: report, serve, trends

- [ ] 6.1 Tests first: `flashpoint report [--out] [--event] [--match] [--no-raw]` exit codes and summary; `flashpoint serve DIR [--host] [--port]` serves files, rejects traversal, and exits 0 on interrupt
- [ ] 6.2 Implement `report` and `serve`; keep heavy imports deferred (extend `test_acquire_imports.py` so `acquire --watch` RSS doesn't grow)
- [ ] 6.3 Add the `views` extra (marimo, altair); `flashpoint trends [--lake] [--port] [--report-dir]` runs the app, and prints the install hint when the extra is missing (test)

## 7. Lifetime app (spec: lifetime-trends, unit-odometry)

- [ ] 7.1 `apps/lifetime.py` Trends tab: feature, phase, and filter controls, with a per-slot or per-unit toggle; low-alignment markers; slot-change markers on unit lines
- [ ] 7.2 Siblings tab: per-match sibling comparison with deviation highlighting
- [ ] 7.3 Units tab: totals, usage trend, slot history, "not found" with suggestions, and sessions without aligned samples
- [ ] 7.4 Drill-through: link to the report page (or "not built" plus the command) and a raw-file download
- [ ] 7.5 Tests: the app runs headless against the corpus lake and an empty lake (no error, empty-lake message); the lake is byte-identical before and after; a hostile slot role renders as text
- [ ] 7.6 `notebooks/README.md` and one example notebook using `views/queries.py`

## 8. Docs and wrap-up

- [ ] 8.1 Amend ADR-0006 (about 1000-bucket envelopes, script-tag data for file://, W/RPS dropped) and ADR-0007 (apps in the package, `views` extra); update `docs/rewrite/05-target-architecture.md` §7 and the layout
- [ ] 8.2 Update `docs/rewrite/06-roadmap.md` P4 status; README usage for `report`, `serve`, and `trends`
- [ ] 8.3 File GitHub issues: `DeviceEnable` dropped by silver; no battery-voltage signal; rio TalonFX status signals at 4 Hz; hoot-only sessions missing from odometry
- [ ] 8.4 Full gate: ruff, mypy --strict, pytest, corpus e2e, browser tests, and perf; then open the PR into `rewrite`

## 9. [HUMAN] Field check

- [ ] 9.1 Open a built report from a USB stick on a pit laptop with Wi-Fi off; a student finds the hottest motor in Q7 in two clicks
- [ ] 9.2 Run `flashpoint trends` on a mentor laptop; confirm the drill-through opens the report and the downloaded wpilog opens in AdvantageScope
