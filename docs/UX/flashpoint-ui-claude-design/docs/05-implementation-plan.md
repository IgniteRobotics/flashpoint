# 05 · Implementation plan

Build in order. Each phase ends with its acceptance criteria passing and a short report (done / stubbed / open questions, also logged in `docs/DECISIONS.md`). The UI runs on MSW mocks of `data-contracts/examples` until the API phases land.

Before Phase 0: **inventory what exists** (robot logging, topics, log locations, any current Flashpoint code) and update the **(assumed)** sections of `docs/04`.

---

## Phase 0 · Scaffold

- Vite + React + TS strict, React Router, Tailwind with `design/tailwind.preset.js`, global `tokens.css` + `base.css`, fonts.
- `pnpm gen:types` from `data-contracts/*.schema.json`.
- MSW handlers serving the example fixtures for every endpoint in `docs/04`.
- Vitest, Testing Library, Playwright + axe. CI: typecheck, lint, unit, e2e.

**Done when:** shell renders on warm black; tests run green; types generate.

## Phase 1 · Shell and kit

- `AppHeader` (tabs, Anomalies count, robot pill states, CRT toggle on `ignite.scanlines`, DEMO badge when mocked), `CommandPalette` (cmdk) with all groups from `docs/03`.
- Components from `docs/02`: Status, Pill, Readout, Vital, MechCard, PhaseBar, Alert, EventLog, Track, Marker, Verdict, CheckRow, Fact, Lifeline, AnomalyItem, plus shared Button/Tab/Badge/Label/SectionHeading/Card/Callout/Table.
- `<TimeSeries>` wrapper around uPlot implementing the chart grammar (primary, selected, dim, peer, temp overlay, threshold, band, out-of-band points, cursor, phase shading, segments).
- `/kit` dev route showing every component in every state.

**Done when:** `/kit` matches the prototype side by side; axe clean on `/kit`; status renders identically in grayscale (glyph + word + border); reduced motion disables blink and scanlines.

## Phase 2 · Replay (on mocks)

First real screen: static data, exercises tracks, markers, cursor, URL state.

- Match list, tracks with shared cursor and scrubber, markers, event panel with cause/fix, readout table, COPY LINK, EXPORT CSV.

**Done when:** `?t=&ev=` round-trips; clicking a marker moves the cursor and selects the event; keyboard: scrubber arrows, markers tabbable; NO LOG and parse-error states render.

## Phase 3 · Live (on the simulator)

- `pnpm dev:sim`: NT4 server replaying a WPILog fixture (1× / 4×).
- NT4 client (or relay) → ring buffer → 2 Hz `live-snapshot` render loop.
- Match band, vitals, mechanism cards (in-place updates), focus rail, alerts with ack, event log, footer stats, connection-lost state.

**Done when:** runs a full simulated match without memory growth; keyboard focus on a mechanism card survives 60 s of ticks; alerts don't steal focus; connection loss shows the banner and recovers; CPU stays reasonable on a low-end laptop (record the number).

## Phase 4 · Pit (on mocks, then probes)

- Verdict band and rule, check rows with streamed results, acknowledge/undo, clear sticky faults (disabled unless robot disabled), battery fleet with grade and selection.

**Done when:** verdict logic has unit tests for every combination; results stream in order; selecting a battery re-evaluates instantly; actions are logged with author.

## Phase 5 · Log pipeline + device registry (API)

- FastAPI service; log pull/upload, WPILog parse (RobotPy wpiutil), raw storage, downsampled tracks, event extraction and rule-table analysis, ingest progress over SSE.
- Registry tables, LOG SWAP, notes, battery tests, spin tests (and the robot test-mode routine that produces them).
- Replay and Pit switch from mocks to the API.

**Done when:** a real match log appears in Replay within 3 minutes of pulling it; every event has cause/fix text from the rule table (or "no rule matched"); a swap logged in the pit appears in History lifelines.

## Phase 6 · History

- Metrics extraction per device per match (`docs/04` table) into DuckDB.
- History screen: range, type, metric, multi-device chart with selected highlight and gaps, segment labels, table with own-range trends and health, device rail with lifeline.

**Done when:** a device moved between slots shows one continuous line under its serial; retired devices appear in the right ranges; numbers in the table equal the chart's last points.

## Phase 7 · Anomaly detection

- Detector job after each ingest implementing SPEC, DRIFT, SIBLING, STEP, NOISE with sensitivity profiles; confidence; similar-case matching; evidence series.
- Anomalies screen against the API; state changes with required dismissal reason and optional spec update; pit tasks.

**Done when:** on historical data, the detector finds known past problems (e.g., a bearing that was replaced) without flooding at NORMAL sensitivity; dismissing with "spec updated" stops repeat flags; every finding renders its evidence chart.

## Phase 8 · Local-first, sync, harden

- Package the laptop stack (one command to start everything); optional cloud sync with audit trail; access roles.
- Error and loading states everywhere; axe clean on every route; keyboard walkthrough of every flow; test at 1280×720 and on a phone; test with the robot on a real field network at an event or scrimmage.

**Done when:** a full competition day runs on the pit laptop with no internet, then syncs cleanly that night.

---

## v1.1 candidates

Full-rate zoom in Replay · match overlay compare · QR labels on motors for one-scan swaps · auto-generated pit task lists per event · push alerts to a phone in the stands · shared component library with Mnemosyne as a package.
