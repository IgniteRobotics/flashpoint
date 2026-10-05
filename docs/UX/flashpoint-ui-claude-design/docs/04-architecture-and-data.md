# 04 · Architecture and data

Anything about Flashpoint's existing code, the robot program or the team's logging setup is marked **(assumed)**. Reconcile these with the repo before building; record differences in `docs/DECISIONS.md`.

## The constraint that shapes everything: competitions are offline

FRC venues have unreliable or no internet, and the robot radio network is for the robot. So Flashpoint is **local-first**:

- The whole stack (UI, API, database, detector) runs on **the pit laptop**.
- Live connects to the robot over the robot network (or tethered in the pit).
- Logs are pulled from the robot after each match and processed on the laptop.
- When internet is available (hotel, home), the laptop **syncs** logs, metrics and registry changes to a shared store so mentors can view History and Anomalies from anywhere. Conflicts are rare (one pit laptop at a time) and resolved last-writer-wins per record, with an audit trail.

## System shape

```
 ROBOT (roboRIO)                       PIT LAPTOP (runs everything offline)                  OPTIONAL CLOUD
 ┌──────────────────────┐   NT4 ws    ┌─────────────────────────────────────────────┐  sync  ┌──────────────────┐
 │ robot code publishes │ ──────────▶ │ Browser UI (React, uPlot)                    │ ─────▶ │ same API + store │
 │ telemetry topics      │             │   Live ◀── NT4 client (or relay)            │        │ read-mostly view │
 │ DataLog → .wpilog     │  pull log   │ FastAPI                                      │        │ for mentors      │
 │ (+ Phoenix .hoot)     │ ──────────▶ │   log ingester → metrics extractor → DuckDB  │        └──────────────────┘
 └──────────────────────┘   after match│   anomaly detector (runs after each ingest)  │
                                      │   pit-check probes (NT4 / diagnostics)        │
                                      └─────────────────────────────────────────────┘
```

## Live data path

**Robot side (assumed):** robot code publishes per-device telemetry to NetworkTables 4 and records everything to a WPILog via WPILib's DataLogManager (which also captures NT). Teams using AdvantageKit already have structured logging; adapt the topic map to it.

**Proposed topic convention** (adjust to what exists):

```
/Flashpoint/meta/{matchType, matchNumber, alliance, station, phase, matchTime}
/Flashpoint/power/{batteryVoltage, totalCurrent, brownout}
/Flashpoint/can/{utilization, busOffCount}
/Flashpoint/loop/{periodMs, overrun}
/Flashpoint/devices/<slot>/{serial, supplyCurrent, statorCurrent, tempC, faults, stickyFaults, firmware}
/Flashpoint/events  (string array or struct log of {time, level, source, message})
```

`serial` per slot is what links live data to the device registry. **(assumed)** Verify how to read a device's serial at runtime with the vendor library in use; if it isn't exposed, maintain the slot → serial mapping in the registry (updated by LOG SWAP and verified at Pit check via firmware/version info).

**Browser side:** one NT4 connection per laptop. If more than one viewer needs Live (stands + pit), run a tiny **relay** on the laptop that holds the single NT4 subscription and fans out over WebSocket/SSE. Robot network bandwidth is limited; don't let five tablets each subscribe to everything.

**Rendering:** ingest at full rate into a ring buffer (last 30 s), render at ~2 Hz (see `docs/03`). The `live-snapshot` contract is what one render tick consumes.

**Simulator:** a log-replay NT4 server that plays a WPILog back in real time (or at 4×). Required for development, demos and testing Live without a robot.

## Log pipeline (after each match)

1. **Pull:** copy the match's `.wpilog` from the roboRIO (USB stick or over the network) **(assumed: confirm where logs land today)**. If Phoenix 6 signal logging (`.hoot`) is used, convert with CTRE's conversion tool to WPILog/MCAP first.
2. **Parse:** RobotPy `wpiutil` DataLog reader (Python). Identify the match by FMS metadata in the log.
3. **Store raw:** keep the original file (it is the source of truth) in `logs/<event>/<match>.wpilog`.
4. **Downsample tracks:** 2–10 Hz overview series for Replay; full-rate available on demand.
5. **Extract events:** brownouts, faults, current-limit hits, loop overruns, CAN peaks, plus robot-published events.
6. **Analyze events:** attach heuristic `cause` and `fix` text from a rule table (e.g., brownout + simultaneous high drive and shooter current → "spin-up overlapped acceleration"). Keep rules in data, not code, so leads can edit them.
7. **Extract per-device metrics** (next section) and write them to the metrics store.
8. **Run the anomaly detector.**
9. **Notify:** Replay shows the match; Anomalies tab count updates.

Target: Replay available within 3 minutes of the log being pulled.

## Device registry

Hardware is tracked by **serial number**. Tables (DuckDB/SQLite):

| Table | Key columns |
|---|---|
| `device` | serial (PK), kind (motor/battery/…), model, commissioned_at, retired_at, notes |
| `install` | serial, robot, slot, from_match, to_match (null = current) |
| `life_event` | serial, match_ref, kind (install/move/repair/firmware/threshold/anomaly/retire/note), text, author |
| `battery_test` | serial, tested_at, rest_v, ir_milliohm, analyzer |
| `spin_test` | serial, tested_at, duty, current_a, slot, robot |

LOG SWAP writes `install` + `life_event` atomically. History queries join metrics to devices through `install` spans.

## Per-match device metrics

Computed per device per match (`device-metrics.schema.json`):

| Metric | Definition |
|---|---|
| `peakTempC` | max temperature |
| `tempRisePer100As` | temperature rise ÷ integrated current (A·s) × 100; normalizes heating by work done, so sibling comparison is fair |
| `meanCurrentA`, `peakCurrentA` | supply current stats while enabled |
| `limitHits` | count of current-limit engagements |
| `faultCount` | live + sticky faults raised |
| `idleCurrentStdA` | std-dev of current while commanded neutral (electrical noise indicator) |
| `minBatteryV`, `brownouts` | match-level, attached to the battery in use |

### The pit spin test (strongly recommended)

Before each event day: robot on blocks, each mechanism run at a fixed duty cycle (e.g., 50%) for a few seconds, steady-state current recorded per device. Same conditions every time means friction changes show up cleanly. Match data alone mixes in driving style, defense and game state. Automate it as a robot "test mode" routine that writes `spin_test` rows.

## Anomaly detection

Runs server-side after each ingest. Each finding records method, metric, actual, expected band, evidence window, confidence and severity. The UI never computes these.

| Method | What it catches | Proposed algorithm |
|---|---|---|
| **SPEC** | Value outside a fixed limit | Compare to limits from registry/device type (e.g., IR > 15 mΩ, peak climb current > spec). Severity by margin. |
| **DRIFT** | Slow move away from own baseline | Baseline = median of the device's first N matches (or last known-good window). Flag when an EWMA of the metric exceeds baseline by k × MAD for M consecutive matches. CUSUM is a good alternative. |
| **SIBLING** | Different from identical devices doing the same job | Sibling groups from config (four drive modules; left/right shooter). Robust z-score of the device vs the group median per match; flag when |z| > k for M of the last W matches. Use normalized metrics (`tempRisePer100As`, spin-test current) so load differences don't trigger it. |
| **STEP** | Sudden change after a specific match | Two-window test on per-match values (e.g., mean of last W vs previous W with a minimum effect size), or a change-point method (PELT, e.g. the `ruptures` library). Report the change match. |
| **NOISE** | Erratic when it should be steady | `idleCurrentStdA` vs device baseline; flag at a multiple of baseline for M matches. |

**Sensitivity** maps to thresholds: RELAXED (k=4, M=5, hide low severity), NORMAL (k=3, M=3), STRICT (k=2.5, M=2, include watch-level findings). Starting values; tune against the team's history.

**Confidence** combines effect size and number of matches (few matches → low).

**Seen this before:** match new findings to past anomalies with the same device, slot or metric+method; show the outcome text.

**Feedback:** DISMISS AS EXPECTED requires a reason and can update the spec or mark a change point as intentional (e.g., gearbox ratio change), which resets the baseline for DRIFT/STEP from that match.

## API (FastAPI, on the laptop)

| Method & path | Returns |
|---|---|
| `GET /api/robot` | config: mechanisms, slots, limits, phases, brownout V |
| `GET /api/live/stream` | SSE/WebSocket of `live-snapshot` (relay mode) |
| `GET /api/matches?event=` | match list with status and fault counts |
| `GET /api/matches/:id` | `match-log` (tracks downsampled, events with analysis) |
| `GET /api/matches/:id/signal?name=&from=&to=` | full-rate slice (v1.1) |
| `POST /api/logs` | upload/pull a log → `{ingestId}`; SSE progress at `/api/ingests/:id/events` |
| `POST /api/pit-checks` | run checks → `{runId}`; SSE of check results; `GET /api/pit-checks/:id` |
| `PATCH /api/pit-checks/:id/checks/:checkId` | acknowledge a warning |
| `POST /api/robot/clear-sticky-faults` | explicit action, logged with author |
| `GET /api/batteries` | fleet with latest test |
| `GET /api/devices?kind=` · `GET /api/devices/:serial` | registry, installs, life events |
| `POST /api/devices/:serial/swaps` · `POST /api/devices/:serial/notes` | LOG SWAP, ADD NOTE |
| `GET /api/metrics?kind=&metric=&from=&to=` | per-match aggregates for History |
| `GET /api/anomalies?state=&sens=` · `GET /api/anomalies/:id` | findings with evidence series |
| `PATCH /api/anomalies/:id` | state change `{state, reason?, updateSpec?}` |
| `POST /api/tasks` | pit task from an event or anomaly |
| `POST /api/sync` | push/pull with the cloud store when online |

## Data contracts

JSON Schemas in `data-contracts/`; generate TypeScript with `json-schema-to-typescript` and validate Pydantic models in tests. Examples in `data-contracts/examples/` are generated from the prototype's `data.js` by `scripts/export-fixtures.mjs` and checked by `scripts/validate-fixtures.py`.

| Schema | Describes |
|---|---|
| `live-snapshot` | One render tick: match state, vitals, per-mechanism values and status, recent events |
| `match-log` | Match metadata, downsampled tracks, events with cause/fix |
| `pit-check` | A run: checks with state/detail/action, acknowledgments, verdict |
| `device` | Registry entry, install spans, life events |
| `device-metrics` | Per-match metric rows for one device |
| `anomaly` | Finding with method, values, band, evidence series, causes, similar, state history |

## Security and safety

- Read-only toward the robot except **Clear sticky faults**, which is an explicit, logged action available only when the robot is disabled.
- The laptop API binds to localhost (and the LAN only when the relay is enabled for other viewers).
- Cloud sync uses team accounts; students can view, leads can change registry and anomaly state.
