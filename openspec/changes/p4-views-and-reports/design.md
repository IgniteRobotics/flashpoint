## Context

P2 left a lake with silver (one row per mapped sample on the aligned wpilog clock) and gold (`gold/match_features`, one row per match, phase, slot, and unit). Nobody can look at it without writing SQL. The legacy viewers are broken in different ways:
- The Streamlit/PyGWalker app (`viz.py`) runs `SELECT *` and filters in pandas (#35). It shares one `gw_config.json` across users (#58) and launches AdvantageScope on the server (#54).
- The power-tracking static site (`utils/site_builder.py` + `utils/templates/index.html` at `archive/static-site`) has several defects:
  - it decimates with `arr[::step]` (#25);
  - it writes about 27 MB per match (#37);
  - it puts names into `innerHTML`/`onclick` (#56);
  - it `fetch()`es its data, so it does not work from a folder (#59).

Josh's design canvas, "Flashpoint UI Prototypes" (claude.ai artifact, 2026-10-05), sets the UX. It has five boards: Live, Replay, Pit, History, and Anomalies. All five share one shell: an amber-on-black CRT look, VT323 and IBM Plex type, and one navigation bar. P4 builds **Replay** and **History**. Live and Pit read the robot, not the lake, so they become a later change. Anomalies is P5.

Decisions:
- **D6** (static per-match site): kept, as Replay's static export mode.
- **D7** (marimo lifetime UI): **superseded** by ADR-0013, one local web app. marimo can't render the designed shell, and a single app gives every view shared navigation.
- **D8** (link to AdvantageScope): followed.
- **D11** (serial identity): drives History's per-unit lines and the device lifeline.

### Spike results (2026-10-05, corpus lake, Q7)
| Question | Finding |
|---|---|
| Silver size | 22 slots, 14 metrics, 251 series, 9.28 M rows. Session 341.8 s, match window 164.1 s |
| Sample rates | CANivore devices: about 246 Hz. Rio TalonFX: currents, temp, and velocity at **4 Hz**, voltage at 100 Hz. Constants: 4 Hz |
| Payload, default set (90 series), about 1000 buckets | **1,045 KB raw** / 269 KB gzip. True max and min kept for all series |
| Payload, fixed 10 ms (05 §7's plan) | 20 MB raw for the default set, 58 MB for all series. Fails the budget by 10× |
| LTTB at 1500 points | Small, but **misses peaks** (64 of 90 kept). Rejected (#25 again) |
| Temperature | °C end to end (owlet `℃`), integer resolution, TalonFX only. Present in both 2025 and 2026 hoots. Only 26 distinct values in Q7, so ideal for change points |
| Q7 temps | Start at 21–25 °C, peak at 26–46 °C, cool only 4–9 °C before the log ends |
| `stall_s` | **0.0 for every Q7 row.** Threshold suspect (`stall_current_fraction = 0.4`) |
| Stall ratio (|stator| / stall current while |velocity| < 0.5 rps), Q7 | Bimodal. Drive and steer transients peak at 0.149 (steer, about 60 A); nothing on any motor falls in 0.15–0.20. The intake extension holds at 0.20–0.25 (about 80 A against its hard stop, 33 s), and the intake rollers jam at 0.20–0.30 (about 84 A, 6.8 s leader, 7.8 s follower). 0.4 (≥ 112 A) can't be reached under the robot's current limits. E10 has no enabled time: every ratio is ≤ 0.003 |
| Stall threshold decision | `stall_current_fraction` 0.4 → **0.2**, in the empty gap between transients and limited stalls. Caveat: a Kraken (476 A stall) limited to 80 A reads 0.17, so it doesn't count. A rule relative to the configured current limit needs limits in robot config (a later change) |
| Battery voltage | Not logged anywhere. Proxy: the lowest or median device `supply_voltage` |
| Powered-on time | Not computed anywhere. Gold only exists for match-keyed sessions |
| Bug found | `DeviceEnable` is a string in bronze, and the silver COALESCE drops it. Issue filed, not fixed here |

## Goals / Non-Goals

**Goals:**
- `flashpoint serve` runs one local, read-only app with Replay and History in the designed shell. A new view is one module plus one registry entry.
- `flashpoint report` precomputes Replay data, at most 2 MB per match. `flashpoint report --static` exports Replay as a folder that opens from file://.
- A per-session `unit_usage` table, so odometry counts practice too.
- A corrected stall threshold, backed by corpus evidence.

**Non-Goals:**
- The Live, Pit, and Anomalies boards (see Context). Pit-server dashboards were cut by Josh on 2026-10-05.
- Design elements with no data source yet:
  - pit spin-test current;
  - faults per match (fault signals aren't in the `health` profile);
  - firmware events;
  - the batteries tab and fleet;
  - CAN-utilisation and loop-overrun markers;
  - current-limit markers (limits aren't in robot config).
- Write actions: add note, log swap, create pit task, mark resolved. Markers carry no cause or fix text.
- Sibling comparison. P5's SIBLING detector owns it.
- Zooming below about 0.2 s in Replay. That is AdvantageScope's job (D8).
- Hoot-only sessions in odometry. They have no silver (the E10 reboot group, the 2025 Q19 hoots). See Risks.
- Fixing `DeviceEnable` in silver, or adding battery-voltage logging on the robot.

## Decisions

### Package layout
```
src/flashpoint/report/      envelope.py, markers.py, build.py (per-match data + index), export.py (static)
src/flashpoint/web/         server.py (stdlib HTTP: routing, static, raw, API), api.py (History endpoints)
src/flashpoint/web/static/  shell: index.html, tokens.css, shell.css, shell.js (nav, router, URL state, DOM helper)
                            views/replay.js, views/history.js; vendor/uplot (MIT); fonts/ (VT323, IBM Plex; OFL)
src/flashpoint/views/       queries.py (parameterised DuckDB SQL behind the History API; testable without HTTP)
src/flashpoint/semantics/usage.py   per-session unit usage + thermal cycles
notebooks/                  README + one example marimo notebook for students (not installed, not a dependency)
```

### App shell and design tokens (the "new views reuse styling" rule)
- **`tokens.css`** holds the canvas palette and type as CSS custom properties. The canvas has no published design system, so the tokens are lifted from its boards:
  - `--fp-bg #0A0806`, `--fp-panel #0D0A07`, `--fp-rule #3D2C10`, `--fp-rule-faint #241A0B`;
  - `--fp-amber #FFB000`, `--fp-amber-hi #FFD27A`, `--fp-amber-mid #C98C1A`, `--fp-amber-dim #A27A36`;
  - `--fp-fault #FF5A1F`;
  - display `VT323`, mono `IBM Plex Mono`, prose `IBM Plex Sans`;
  - spacing 4/8/12/16/24 px, and 44 px touch targets.
- **`shell.css`** holds classes for the header, nav tabs, panels, `## SECTION` headings, chips, tables, tiles, status glyphs, focus outline, and the optional scanline overlay. Status glyphs are ● ok, ▲ warn, ✕ fault, so status never relies on colour alone. Views use these classes only. There are no per-view stylesheets.
- **`shell.js`** provides the shared machinery:
  - Each view registers with `register({id, label, needsApi, mount(el, state), unmount()})`.
  - The nav renders the registered views and hides `needsApi` views in static mode.
  - A hash router keeps all view state in the URL.
  - `h(tag, attrs, ...children)` builds DOM through textContent only.
  - `track(el, opts)` wraps uPlot with the shell theme.

  Live, Pit, and Anomalies later become one file in `views/` plus one `register` call each.
- **No build step:** plain ES modules and vendored files. The front end needs no Node or bun.
- **Fonts:** vendored woff2 (SIL OFL) with `@font-face`. The canvas loads them from Google Fonts, which breaks offline use.

### Charts: uPlot
uPlot (about 50 KB, MIT) is vendored. It replaces the old site's Plotly 2.35.2 (3.5 MB); Josh chose it on 2026-10-05.
- It draws dense time series fast and takes the shell theme.
- It supports a shared cursor across tracks (the Replay scrub) and series overlays (compare).
- Envelopes draw as a min/max band plus a mean line.
- The design has no heatmaps, so nothing needs Plotly.

### Replay data: about 1000-bucket envelopes, delivered as scripts
- **Window:** match start − 2 s to match end + 2 s, from `match_phases`. Bucket width = window / 1000, rounded up to whole 10 ms (Q7 gives about 170 ms).
- **Per bucket:** min, max, and sample mean, from DuckDB `GROUP BY (t_us - t0) // width` over one session's silver partition. Polars only shapes the result. Empty buckets go out as null, so charts show gaps and never draw zero.
- **Series:**
  - per slot: supply current, stator current, rotor velocity, and motor voltage;
  - the robot **battery proxy**: the lowest `supply_voltage` across devices per bucket for the minimum, and the median for the mean;
  - temperature as **change points** `[t, value]`.

  Constants (`motor_kv`, `stall_current`) are dropped. The client computes energy from the power envelope means, so no energy arrays are written.
- **Also embedded:** the per-phase slot features from gold (for the "match maxima" readout) and the rule markers.
- **Encoding:** columnar arrays, 3 significant figures, and a time base of `t0` plus `width` instead of a timestamp per bucket.
- **Delivery (fixes #59):** each match is written as `data/<match_key>.js`, which calls `FP.register(<json>)`, and the index is `data/matches.js`. Both modes load them with `<script>` tags, which work over `file://` where `fetch()` does not. Embedded JSON escapes `</` as `<\/` and escapes U+2028/2029.
- **Budget check:** a match over 2 MB is reported, and its bucket count is lowered until it fits. Spikes are never dropped.

### Replay view
- **Layout** follows the canvas:
  - left: the match list, grouped by event, with marker counts;
  - main: the title, marker strip, tracks, and scrub slider;
  - aside: the selected marker's details and the readout at the cursor.
- **Default tracks:** each has a picker to swap in any slot and metric.
  - battery proxy V, with a brownout reference line;
  - total supply current;
  - the hottest slot's temperature, with a 65 °C reference line;
  - the highest-current slot's current.
- **The readout** shows each slot's match maxima, hottest first, when no cursor is placed. This keeps the old SPA's "hot motor in two clicks" answer.
- **Compare** overlays a second match on every track as a dashed series, and adds a second readout column.
- **Rule markers** (`report/markers.py`) are computed at build time into the match data. Thresholds live in `config/report.toml`. Markers state facts only, with no cause or fix text.
  - temperature: WARN at 65 °C, FAULT at 75 °C;
  - battery proxy: FAULT below 6.75 V held ≥ 20 ms, WARN below 8.0 V;
  - stall intervals from physics;
  - sample gaps over 1 s during the match.
- **Escaping (fixes #56):** views build the DOM only through `h()`. A lint test greps `web/static/**/*.js` for `innerHTML`, `outerHTML`, `insertAdjacentHTML`, inline `on*=`, `eval`, and `new Function`. A CSP `<meta>` limits scripts to the app (`script-src 'self'`), which the browser test verifies. File-origin CSP differs between browsers: if Chromium rejects `'self'` on file://, the static export drops `script-src`, and the `h()` rule stays the actual guard.

### Incremental builds (fixes #37)
- `<lake>/report/site-state.json` maps each match key to:
  - its derived fingerprint (from `derived_state`);
  - the report version;
  - an index summary.
- A build recomputes only keys whose fingerprint or version changed, then rewrites `matches.js` from the state summaries. It never re-parses data files.
- Each data file is written to a temp file and renamed into place.
- `report --static OUT` copies the shell, the Replay view, and the built data into `OUT`.

### Raw downloads (D8)
- **Served mode:** `/raw/<sha256>/<name>` streams from the raw store, for ledger hashes only.
- **Static export:** raw files are hard-linked into `raw/` when on the same filesystem and copied otherwise. `--no-raw` skips them, and the page then lists hashes.
- **Download names:** `<match_key>__<bus or wpilog>__<original name>`, sent with the `download` attribute. Nothing launches AdvantageScope.

### `flashpoint serve`
- Runs the stdlib `ThreadingHTTPServer` on `127.0.0.1:8000` by default. `--host 0.0.0.0` shares it on the pit network and prints a warning.
- Routes:
  - `/` and `/static/*` from the package;
  - `/data/*` from `<lake>/report/data/`;
  - `/raw/<sha256>/<name>`;
  - `/api/*`.

  Every other path returns 404, and a test covers traversal.
- `serve` runs an incremental `report` build on start; `--no-build` skips it.
- Ctrl-C exits with code 0. No web-framework dependency.

### History view and query API (ADR-0013 supersedes D7)
- **Layout** follows the canvas, top to bottom:
  - the filter bar: range (each season, plus all time), robot, event, phase, subsystem;
  - stat tiles;
  - a metric picker (peak temp, min voltage, supply Wh, stall s, current P95), each metric with its reference line;
  - the lines chart: per unit by default, with a per-slot toggle;
  - the device table;
  - the device panel aside, with odometry tiles and the lifeline.
- **API** (`web/api.py`) is thin JSON over `views/queries.py`:
  - `GET /api/filters`: the available filter values;
  - `GET /api/trend?metric=&by=unit|slot&…filters`: `[{series, match_key, t, value, alignment}]`;
  - `GET /api/devices?metric=&…`: table rows with health and sparkline points;
  - `GET /api/unit/<id>`: odometry totals and the lifeline;
  - `GET /api/summary?…`: the tiles.

  Unknown parameters return 400. Values bind as `?` parameters. Metric names map to columns through a fixed whitelist.
- **Data access:** DuckDB in memory over `gold/match_features`, `gold/unit_usage`, and the meta Parquet snapshots. The connection opens lazily on the first API call. It never opens the SQLite ledger, and a test hashes the lake before and after.
- **Health (P4):** OK, WARN, or HOT, from the temperature limits only. P5 adds anomaly states to the same column.
- **Lifeline:** derived in SQL from `slot_observations` in time order: first seen, moved (slot or robot changed), replaced by X, and last seen.
- **Drill-through:** links to `#view=replay&m=<key>&track=<slot>`, plus the `/raw/` download.

### Unit usage (`gold/unit_usage`, partitioned like match features)
- A new derive step after gold, in the same fingerprinted loop, so it rebuilds with silver. It runs for **every session with silver**, with or without a match key. A new `DERIVE_VERSION` (2) joins the derive fingerprint, so existing lakes recompute silver and gold. `PIPELINE_VERSION` stays at 2: ingest keys off it too, and bumping it would re-convert every raw file although bronze is unchanged.
- **Powered-on time:** the time held of the unit's `supply_voltage` samples, capped at `hold_cap_us` (1 s) so logging gaps don't count. If supply voltage is absent, the time held of any metric is used instead.
- **Enabled time:** powered-on time intersected with the robot's enabled intervals. These come from the same mode runs framing uses, exposed from `framing.py`, not from the auto/teleop phases, because practice sessions toggle enable many times.
- **Energy and stall:** reuse `physics.motor_features` over the whole session, not phases.
- **Thermal cycles:** a hysteresis state machine over temperature change points, with thresholds in `PhysicsConfig`:
  - it arms at `min_since_last + rise` (10 °C);
  - it counts when the reading falls `fall` (3 °C) below the peak, or when the session ends while armed.

  Josh asked for the fall to be below 5 °C, so a robot powered off soon after a hot run still counts.
- **Units table:** none is materialised. Totals are a SQL `GROUP BY unit_id` over `unit_usage`. The lifeline comes from `slot_observations` joined with `sessions`.

### Stall threshold
- Measure stall candidates in the corpus: the intake extension against its hard stop, and the intake roller on a jam. Plot the distribution of stator current over stall current while |velocity| < 0.5 rps.
- Lower `stall_current_fraction` only if the evidence supports it, and record the numbers in the spike table.
- If the corpus has no real stalls, keep 0.4, document that 0 is truthful, and add a synthetic test.

### Budgets
| Operation | Budget | Notes |
|---|---|---|
| Replay data, one match | < 5 s, < 500 MB peak | Reads one silver partition; DuckDB `memory_limit` 256 MB |
| Match index rewrite | < 1 s for 200 matches | From `site-state.json` only |
| History API call | < 2 s, full season | Synthetic 60 × 3 × 23 gold; perf-marked test |
| Usage derive, Q7 | < 3 s added | Derive stays < 1 GB peak (P2 budget) |
| Replay data file | ≤ 2 MB raw | Q7 measured at about 1.0 MB |
| Server idle RSS | < 150 MB | DuckDB opened lazily |

### Testing
- **Unit tests:**
  - envelope: spike, nulls, rounding, bucket math;
  - markers: each rule plus a quiet match;
  - thermal cycles: the cases from the spec;
  - usage on synthetic silver;
  - queries: predicates and the whitelist;
  - API status codes;
  - server routes and traversal;
  - `site-state` incremental logic.
- **Browser tests** use a new `browser` marker, `pytest-playwright` as a dev dependency, and Chromium in a new CI job. They run against both the served app and the file:// static export:
  - a test view registered with the shell renders styled;
  - Replay: hottest-first readout, scrub, marker select, compare overlay, URL restore, "not logged" cells, keyboard focus;
  - History: filters, unit line with a gap, device lifeline, drill-through to Replay;
  - the hostile-name fixture causes no dialog and no console error;
  - with the network blocked, no remote requests are made.
- **Corpus tests:**
  - Q7 data ≤ 2 MB;
  - the envelope equals silver min/max per bucket;
  - raw hashes match the ledger;
  - usage energy ≥ feature energy;
  - the History API answers for Q7 and E10.

## Risks / Trade-offs
- **Hoot-only time is missing from odometry** (reboots without a new wpilog, and the 2025 hoots). This undercounts real wear. Mitigation: the device panel shows "sessions without aligned samples: N" from the ledger. A future change can derive hoot-clock silver.
- **Rio TalonFX log at 4 Hz.** Rio-slot currents are coarse, and spikes between samples are invisible whatever the envelope. This is a robot-side SignalLogger setting: it gets a Replay header note and an issue.
- **Plain-JS front end**, the area Josh likes least. It is kept small: no framework, no build step, one DOM helper, and uPlot for charts.
- **The design is ahead of the data** (see Non-Goals). The shell and the API whitelist make each missing metric a data task, not a UI rewrite.
- **`--host 0.0.0.0` exposes read-only lake data and raw logs to the pit network**, with no auth (a proposal non-goal). The default binding is local, and the flag prints a warning.
- **CSP on file://** behaves inconsistently across browsers. The `h()`-only rule is the real guard; CSP is defence in depth.
- **3-significant-figure rounding**: 123.4 A shows as 123 A. That is acceptable for a pit view; exact values are in gold and AdvantageScope.

## Migration Plan
- New commands only. Nothing existing changes behaviour, except that derive also writes `gold/unit_usage` and stalls use the 0.2 threshold. `DERIVE_VERSION` 2 makes `flashpoint derive` recompute; bronze is not re-ingested.
- New ADR-0013 ("Views: one local app in the canvas shell") supersedes ADR-0007.
- ADR-0006 is amended: static export of Replay, ~1000-bucket envelopes, script-tag data, and uPlot.
- `viz.py` and `gw_config.json` stay until `retire-legacy-code` stage 2d, after P4 is archived.

## Open Questions
- `report --static` default scope: every match, or only the newest event? Proposed: all, with `--event`.
- The canvas header shows "ROBOT 10.68.29.2 ● TETHERED". In P4 the header shows the lake path and robot names instead, until the Live change exists.
