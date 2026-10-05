## 1. Stall threshold check (spec: unit-odometry; motor-physics)

- [x] 1.1 Corpus spike: distribution of stator/stall-current ratio while |velocity| < 0.5 rps for the intake extension and intake roller on Q7 and E10; record the numbers in design.md's spike table
- [x] 1.2 Tests first: synthetic stall at the chosen threshold counts; corpus test pins the Q7 stall result (non-zero if the evidence supports it, otherwise a documented zero); adjust `stall_current_fraction` only on evidence

## 2. Unit usage and thermal cycles (spec: unit-odometry)

- [x] 2.1 Tests first: thermal-cycle state machine covers the spec cases (cool-down, powered off hot, small wobble, no temperature) plus configurable rise and fall thresholds
- [x] 2.2 Implement thermal cycles in `semantics/usage.py`; add `thermal_rise_c = 10`, `thermal_fall_c = 3` to `PhysicsConfig`
- [x] 2.3 Expose the enabled intervals (mode runs) from `framing.py`, with tests: practice session with several enable toggles, and a match session
- [x] 2.4 Tests first: usage on synthetic silver covers powered-on time (with the hold cap and a gap), enabled time, energy, stall, a mid-session swap split, and a non-match session
- [x] 2.5 Implement `usage.session_usage` and write `gold/unit_usage` (staged rename, like match features); wire it into the derive loop for every session with silver; add `DERIVE_VERSION` 2 to the derive fingerprint (not `PIPELINE_VERSION`, which would re-ingest bronze)
- [x] 2.6 Corpus tests: Q7 usage rows exist for all 17 motors, usage supply energy ≥ match-phase feature energy, cycle counts match the spike (12 of 17 motors); measure added derive time (< 3 s) and peak memory (< 1 GB)

## 3. History queries (spec: lifetime-trends, unit-odometry)

- [x] 3.1 Synthetic gold fixture builder: one season (60 matches × 3 phases × 23 slots) of match features and usage, plus slot observations covering a swap, a robot move, and a gap
- [x] 3.2 Tests first for `views/queries.py`: each filter (range, robot, event, match type, phase, subsystem, slot, unit) binds as a parameter and returns only matching rows; a hostile value is treated as data; the metric whitelist rejects unknown names
- [x] 3.3 Implement the trend queries: per unit with slot changes marked and gaps where the unit was not installed; per slot; low-alignment flag
- [x] 3.4 Implement the device table (current slot, serial, in-service span, latest value, change, sparkline, temperature health) and the summary tiles
- [x] 3.5 Implement the unit queries: odometry totals (filterable by season and robot), lifeline events (first seen, moved, replaced by, last seen) with identity source, closest-match lookup for unknown units, and sessions without aligned samples
- [x] 3.6 Perf test (`perf` marker): each query on the synthetic season returns in < 2 s

## 4. Replay data, envelopes, and markers (spec: match-reports)

- [x] 4.1 Tests first for `report/envelope.py`: about 1000 buckets, width rounded to 10 ms; a 4 ms 150 A spike survives; empty buckets are null; 3-significant-figure rounding; temperature change points; battery proxy (lowest supply voltage as the minimum)
- [x] 4.2 Implement the envelope over one silver partition in DuckDB (memory limit 256 MB)
- [x] 4.3 Tests first for `report/markers.py`: temperature WARN and FAULT, brownout and sag, stall interval, sample gap, quiet match, thresholds from `config/report.toml`
- [x] 4.4 Implement the markers
- [x] 4.5 Tests first for `report/build.py`: payload shape (header, per-phase slot features, series, change points, markers, "not logged" markers, alignment, source logs); embedded JSON escaping (`</`, U+2028/2029); the 2 MB guard lowers the bucket count without losing min or max
- [x] 4.6 Implement the per-match build (`data/<match_key>.js`, temp file then rename) and `site-state.json`. Incremental rebuild, `--event` and match-key selection, and an index listing hoot-only and low-alignment entries with reasons; the build summary counts sessions without a match key
- [x] 4.7 Corpus tests: Q7 data ≤ 2 MB; every bucket's min and max equal the silver min and max over its range; build < 5 s and < 500 MB (perf)

## 5. Server and API (spec: match-reports, lifetime-trends)

- [x] 5.1 Tests first for `web/server.py`: routes (`/`, `/static`, `/data`, `/raw/<sha256>/<name>` for ledger hashes only, `/api`), 404 for everything else including traversal, `--host` warning, clean exit on interrupt, lazy DuckDB (idle RSS < 150 MB)
- [x] 5.2 Implement the server; `serve` runs an incremental report build on start (`--no-build` skips it)
- [x] 5.3 Tests first for `web/api.py`: each endpoint's JSON shape, 400 on unknown parameters, empty-lake message, the lake is byte-identical after exercising every endpoint
- [x] 5.4 Implement the API over `views/queries.py`
- [x] 5.5 Corpus test: the API answers for Q7 and E10, and raw download hashes equal the ledger hashes

## 6. App shell and design tokens (spec: match-reports "Shared app shell")

- [x] 6.1 Vendor uPlot (with its license) and the VT323 and IBM Plex woff2 fonts (OFL), with `@font-face`
- [x] 6.2 `tokens.css` (palette, type, spacing lifted from the canvas) and `shell.css` (header, nav, panels, section headings, chips, tables, tiles, status glyphs, focus outline, scanline toggle)
- [x] 6.3 `shell.js`: view registry (`needsApi` hides views in static mode), hash router and URL state, the `h()` DOM builder, and the `track()` uPlot wrapper with the shell theme
- [x] 6.4 Static lint test: no `innerHTML`, `outerHTML`, `insertAdjacentHTML`, inline `on*=`, `eval`, or `new Function` in `web/static/**/*.js`; no remote host referenced by any app file
- [x] 6.5 Add `pytest-playwright`, the `browser` marker, and a CI job that installs Chromium; a test view registered with the shell renders with shell fonts and colours and no stylesheet of its own; keyboard focus is visible

## 7. Replay view (spec: match-reports)

- [x] 7.1 `views/replay.js`: match list grouped by event with marker counts and low-alignment marks; header; marker strip; default tracks with reference lines and per-track slot/metric pickers; scrub (drag and keyboard); readout (at the cursor, or match maxima hottest-first); "not logged" cells
- [x] 7.2 Compare overlay (dashed series, second readout column, absent slots) and URL state (match, overlay, cursor, tracks)
- [x] 7.3 AdvantageScope downloads panel (served `/raw/` links; static `raw/` links or the "not included" list of hashes)
- [x] 7.4 `report --static OUT [--no-raw]` export, with tests (layout, History absent from the nav, raw hard link or copy)
- [x] 7.5 Browser tests, served and file:// (network blocked): hottest-first readout on a Q7-like fixture, scrub, marker select, compare, URL restore, "not logged" temperature, hostile names show literally with no dialog or console error; record CSP behaviour and apply the design fallback if Chromium rejects `'self'` on file://

## 8. History view (spec: lifetime-trends, unit-odometry)

- [x] 8.1 `views/history.js`: filter bar, tiles, metric picker with reference lines, lines chart (per unit by default, per-slot toggle, emphasised selection, low-alignment markers, gaps), device table
- [x] 8.2 Device panel: model and serial, health, odometry tiles, sessions without aligned samples, lifeline; unit links; "not found" with suggestions
- [x] 8.3 Drill-through to Replay (`#view=replay&m=<key>&track=<slot>`), "not built" plus the command, raw download
- [x] 8.4 Browser tests against the served corpus lake: filters, unit line with a gap, lifeline order, drill-through, empty-lake message, hostile slot role as text

## 9. CLI

- [x] 9.1 Tests first: `flashpoint report [--event] [--match] [--static OUT] [--no-raw]` and `flashpoint serve [--lake] [--host] [--port] [--no-build]` exit codes and summaries
- [x] 9.2 Implement both; keep heavy imports deferred (extend `test_acquire_imports.py` so `acquire --watch` RSS doesn't grow)

## 10. Docs and wrap-up

- [x] 10.1 ADR-0013 "Views: one local app in the canvas shell" (supersedes ADR-0007; mark 0007 Superseded); amend ADR-0006 (Replay static export, about 1000-bucket envelopes, script-tag data, uPlot); update `docs/rewrite/05-target-architecture.md` (D7 row, §7, layout)
- [x] 10.2 `notebooks/README.md` and one example marimo notebook using `views/queries.py`
- [x] 10.3 Update the `docs/rewrite/06-roadmap.md` P4 status and add a Live/Pit follow-up change to the roadmap; README usage for `report` and `serve`
- [x] 10.4 File GitHub issues: `DeviceEnable` dropped by silver; no battery-voltage signal; rio TalonFX status signals at 4 Hz; hoot-only sessions missing from odometry; design metrics without data (faults, firmware, spin test, batteries) — #18–#22
- [ ] 10.5 Full gate: ruff, mypy --strict, pytest, corpus e2e, browser tests, and perf; then open the PR into `rewrite`

## 11. [HUMAN] Field check

- [ ] 11.1 Open a static export from a USB stick on a pit laptop with Wi-Fi off; a student finds the hottest motor in Q7 in two clicks
- [ ] 11.2 Run `flashpoint serve` on a mentor laptop; History drill-through opens Replay, and the downloaded wpilog opens in AdvantageScope
- [ ] 11.3 Josh compares the built Replay and History against the canvas boards and signs off on the look
