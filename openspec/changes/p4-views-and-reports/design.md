## Context

P2 left a lake with silver (one row per mapped sample on the aligned wpilog clock) and gold (`gold/match_features`, one row per match, phase, slot, and unit). Nobody can look at it without writing SQL. The legacy viewers are broken in different ways:
- The Streamlit/PyGWalker app (`viz.py`) runs `SELECT *` and filters in pandas (#35), shares one `gw_config.json` across users (#58), and launches AdvantageScope on the server (#54).
- The power-tracking static site (`utils/site_builder.py` + `utils/templates/index.html` at `archive/static-site`) is the right UX but has several defects:
  - it decimates with `arr[::step]` (#25);
  - it writes about 27 MB per match (#37);
  - it puts names into `innerHTML`/`onclick` (#56);
  - it `fetch()`es its data, so it does not work from a folder (#59).

Decisions: **D6** (static per-match site), **D7** (marimo lifetime app over DuckDB), and **D8** (link to AdvantageScope). All three are followed, so there is no new ADR. ADR-0006 and `05 §7` are amended for the envelope and file-loading details below. **D11** (serial identity) drives the unit page.

### Spike results (2026-10-05, corpus lake, Q7)
| Question | Finding |
|---|---|
| Silver size | 22 slots, 14 metrics, 251 series, 9.28 M rows; session 341.8 s, match window 164.1 s |
| Sample rates | CANivore devices about 246 Hz. Rio TalonFX: currents, temp, and velocity at **4 Hz**, voltage at 100 Hz. Constants at 4 Hz |
| Payload, default set (90 series), about 1000 buckets | **1,045 KB raw** / 269 KB gzip; true max and min kept for all series |
| Payload, fixed 10 ms (05 §7's plan) | 20 MB raw (default set), 58 MB (all), so it fails the budget by 10× |
| LTTB at 1500 points | Small, but **misses peaks** (64/90 kept). Rejected (#25 again) |
| Temperature | °C end to end (owlet `℃`), integer resolution, TalonFX only, present in 2025 and 2026 hoots. 26 distinct values in Q7, so it is ideal for change points |
| Q7 temps | Start 21–25 °C, peak 26–46 °C, cool only 4–9 °C before the log ends |
| `stall_s` | **0.0 for every Q7 row.** Threshold suspect (`stall_current_fraction = 0.4`) |
| Battery voltage | Not logged anywhere. Proxy: the median device `supply_voltage` |
| Powered-on time | Not computed anywhere. Gold only exists for match-keyed sessions |
| Bug found | `DeviceEnable` is a string in bronze, and the silver COALESCE drops it. Filed as an issue, not fixed here |

## Goals / Non-Goals

**Goals:**
- `flashpoint report` builds an offline per-match site, at most 2 MB of data per match, that opens from a folder.
- `flashpoint trends` runs a local marimo app: per-slot and per-unit trends, sibling comparison, and unit pages, with all filters in SQL.
- A per-session `unit_usage` table, so odometry counts practice too.
- A corrected stall threshold, backed by corpus evidence.

**Non-Goals:**
- Pit-server dashboards (cut by Josh, 2026-10-05).
- Zooming below about 0.2 s in the report. That is AdvantageScope's job (D8).
- Hoot-only sessions in odometry. They have no silver (the E10 reboot group, the 2025 Q19 hoots). Their time is not counted; see Risks.
- Fixing `DeviceEnable` in silver, or adding battery-voltage logging on the robot.

## Decisions

### Package layout
```
src/flashpoint/report/      build.py (per-match data), envelope.py, site.py (index + state), serve.py
src/flashpoint/report/static/   index.html, app.js, app.css, plotly-2.35.2.min.js (vendored, MIT)
src/flashpoint/views/       queries.py (parameterised SQL shared by apps and tests)
src/flashpoint/apps/        lifetime.py (marimo app: Trends, Siblings, Units tabs)
src/flashpoint/semantics/usage.py   per-session unit usage + thermal cycles
notebooks/                  README + one example marimo notebook for students (not installed)
```
The proposal said the apps live in `notebooks/`. They move into the package instead, because `pipx install` (D9) only ships the package, and `flashpoint trends` has to find the app. `notebooks/` stays as the place for students' own exploration.

### Report data: about 1000-bucket envelopes, delivered as scripts
- **Window:** match start − 2 s to match end + 2 s, taken from `match_phases`. Bucket width = window / 1000, rounded up to a whole 10 ms. Q7 gives about 170 ms.
- **Per bucket:** min, max, and sample mean, from DuckDB `GROUP BY (t_us - t0) // width` over one session's silver partition. Polars only shapes the result. Empty buckets are emitted as null, so charts show gaps and never draw zero.
- **Series:** supply current, stator current, rotor velocity, and motor voltage per slot, plus a robot "battery proxy" (the median `supply_voltage` across devices per bucket). Temperature goes out as **change points** `[t, value]`. Constants (`motor_kv`, `stall_current`) are dropped. Energy curves are computed client-side from the power envelope means, so no energy arrays are written.
- **Encoding:** columnar arrays, values rounded to 3 significant figures, and a time base of `t0` plus `width` instead of a timestamp per bucket.
- **Delivery (fixes #59):** each match is written as `data/<match_key>.js`, which calls `FP.register(<json>)`. The index is `matches.js`. Pages load these with `<script>` tags, which work over `file://`, where `fetch()` does not. JSON is embedded with `</` escaped as `<\/` and U+2028/2029 escaped.
- **Budget check:** the build fails the match (and reports it) if its data exceeds 2 MB. If a future robot has more slots, the bucket count drops to fit. It never drops spikes.

### Report UX (salvaged from the power-tracking SPA)
- Keep: dark theme, match chips, compare two matches, the Stats table first, the heatmaps tab, and the Total Power tab.
- Drop: the W/RPS tab. It was a noisy derived ratio that nobody has used yet; it can come back later.
- Add:
  - a phase selector;
  - per-slot detail charts with phase bands;
  - the low-alignment banner and "not logged" cells;
  - URL hash state (`#m=2026gacmp_qm7,2026gacmp_e10&phase=auto&view=stats`);
  - the downloads panel.
- **Escaping (fixes #56):** `app.js` builds the DOM with `textContent` and `createElement` only. There is no `innerHTML` with data and no inline handlers; a lint test greps `app.js` for `innerHTML`, `outerHTML`, `insertAdjacentHTML`, `on*=`, and `eval`. A CSP `<meta>` restricts scripts to the folder (`script-src 'self'`). This is verified in the browser test, because file-origin CSP differs between browsers. If Chromium rejects `'self'` on file://, the fallback is a CSP without `script-src`, and the DOM-only rule stays the actual guard.
- **Hot limits:** 65 °C maximum and 55 °C mean, the legacy SPA's values, configurable in `config/report.toml`.

### Incremental builds (fixes #37)
- `site-state.json` in the output folder maps each match key to its derived fingerprint (from `derived_state`), the report version, and an index summary.
- A build recomputes only the keys whose fingerprint or report version changed, then rewrites `matches.js` from the state summaries. It never re-parses data files.
- Each data file is written to a temp file and renamed into place.

### Raw downloads (D8)
- Raw files are hard-linked into `raw/` when the output is on the lake's filesystem; otherwise they are copied. Names follow `<match_key>__<bus or wpilog>__<original name>`.
- `--no-raw` skips them, and the page then lists hashes instead.
- Links use the `download` attribute. Nothing launches AdvantageScope.

### `flashpoint serve`
- Uses the stdlib `ThreadingHTTPServer` on `127.0.0.1:8000` by default. `--host 0.0.0.0` shares it on the pit network.
- Prints the URL and serves only the given folder (directory traversal is rejected by the stdlib handler; a test covers it). Ctrl-C exits with code 0.

### Lifetime app (D7)
- `flashpoint trends [--lake] [--port]` runs `marimo run` on `flashpoint/apps/lifetime.py`.
- marimo and altair become an **optional extra** (`flashpoint[views]`), so acquire-only installs stay small. The command prints the install hint if they are missing.
- Charts are altair through `mo.ui.altair_chart`. Point selection drives the drill-through.
- **Queries:** `views/queries.py` holds plain functions: `(con, filters) -> polars.DataFrame`, with SQL using `?` parameters. Filter values are never formatted into SQL. Each filter becomes a predicate on the hive-partitioned `gold/match_features` and `gold/unit_usage` views, and on meta snapshots (`slot_observations`, `sessions`).
- Read-only: the app opens DuckDB in memory over Parquet only, never the SQLite ledger. A test hashes the lake before and after a run.
- **Drill-through:** a link to `<report dir>/index.html#m=<key>` (report dir from `--report-dir`, default `<lake>/report`), plus `mo.download` of the raw files from the raw store.
- **Siblings:** group by `(subsystem, model)` within a match. A slot is flagged when `|value − median| / median > 0.25` (configurable).

### Unit usage (`gold/unit_usage`, partitioned like match features)
- This is a new derive step after gold, in the same fingerprinted loop, so it rebuilds with silver. It runs for **every session with silver**, whether or not it has a match key. `PIPELINE_VERSION` is bumped to 3, so existing lakes recompute.
- **Powered-on time:** time held of the unit's `supply_voltage` samples, capped at `hold_cap_us` (1 s) so logging gaps don't count. If supply voltage is absent, the time held of any metric.
- **Enabled time:** powered-on time intersected with the robot's enabled intervals. These come from the same mode runs that framing uses, exposed from `framing.py`, not from the auto/teleop phases: practice sessions toggle enable many times.
- **Energy and stall:** reuse `physics.motor_features` over the whole session, not phases.
- **Thermal cycles:** a hysteresis state machine over temperature change points. Arm at `min_since_last + rise` (10 °C). Count when the reading falls `fall` (3 °C) below the peak, or when the session ends while armed. Thresholds live in `PhysicsConfig`. Josh asked for the fall default to sit below 5 °C so a robot powered off soon after a hot run still counts.
- **Units table:** none is materialised. Totals are a SQL `GROUP BY unit_id` over `unit_usage`, and slot history comes from `slot_observations` joined with `sessions`.

### Stall threshold
- Measure the stall candidates in the corpus: intake extension against its hard stop, and the intake roller on a jam. Look at the distribution of stator current / stall current while |velocity| < 0.5 rps.
- Lower `stall_current_fraction` only if the evidence supports it, and record the plot and numbers in this design's spike table. If the corpus has no real stalls, keep 0.4, document that 0 is truthful, and add a synthetic test.

### Budgets
| Operation | Budget | Notes |
|---|---|---|
| Report data, one match | < 5 s, < 500 MB peak | Reads one silver partition; DuckDB `memory_limit` 256 MB |
| Report index rewrite | < 1 s for 200 matches | From `site-state.json` only |
| Trends view refresh | < 2 s, full season | Synthetic 60 × 3 × 23 gold; perf-marked test |
| Usage derive, Q7 | < 3 s added | Derive stays < 1 GB peak (P2 budget) |
| Data file | ≤ 2 MB raw | Q7 measured at about 1.0 MB |

### Testing
- **Unit tests:** envelope (spike, nulls, rounding, bucket math), thermal-cycle cases from the spec, usage on synthetic silver, query predicates, `site-state` incremental logic, and serve traversal.
- **Browser tests** (new `browser` marker, `pytest-playwright` as a dev dependency, Chromium in a new CI job):
  - file:// load with network blocked;
  - hostile-name fixture with no dialog or console error;
  - URL-state restore;
  - compare;
  - "not logged" cells.
- **Corpus tests:** Q7 builds under 2 MB, the envelope equals silver min/max per bucket, raw hashes match the ledger, and usage energy ≥ feature energy.

## Risks / Trade-offs
- **Hoot-only time is missing from odometry** (reboots without a new wpilog, the 2025 hoots). This undercounts real wear. Mitigation: unit pages show "sessions without aligned samples: N" from the ledger. A future change can derive hoot-clock silver.
- **Rio TalonFX at 4 Hz** means currents on rio slots are coarse, and short spikes between samples are invisible regardless of the envelope. This is a robot-side logging setting (SignalLogger rates), so it gets a note in the report header, not a fix here.
- **The marimo/altair dependency surface** is behind an extra. If marimo's app mode changes, only `apps/` breaks.
- **CSP on file://** behaves inconsistently across browsers. The DOM-only rule is the real guard; CSP is defence in depth.
- **A 3-significant-figure rounding loses detail:** 123.4 A shows as 123 A. That is acceptable for a pit view, and the exact values are in gold and AdvantageScope.

## Migration Plan
- New commands only; nothing existing changes behaviour except that derive also writes `gold/unit_usage` (`PIPELINE_VERSION` 3 makes `flashpoint derive` recompute).
- `viz.py` and `gw_config.json` stay until `retire-legacy-code` stage 2d, after P4 is archived.

## Open Questions
- Does the team want the W/RPS tab back? It was dropped for now.
- Should the report default to building every match in the lake, or only the newest event? The proposed default is all, with `--event`.
