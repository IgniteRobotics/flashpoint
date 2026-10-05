# 03 · Screen specs

Reference implementation for each screen is in `prototype/`. **[prototype shortcut]** marks behavior the prototype fakes that production must do for real.

## Routes and URL state

| Route | Screen | URL state |
|---|---|---|
| `/` | Redirect to `/live` when a robot is connected, else `/replay` | |
| `/live` | Live | `?sel=<mechanismId>` |
| `/replay/:matchId` | Replay | `?t=<seconds>&ev=<eventId>` |
| `/pit` | Pit check | `?battery=<id>` (a run is stored server-side; `/pit/runs/:id` reopens one) |
| `/history` | History | `?type=motors|batteries&range=<season|all>&metric=<key>&device=<id>` |
| `/anomalies` | Anomalies | `?id=<anomalyId>&tab=open|resolved|dismissed&sens=relaxed|normal|strict` |

Prototype uses query params on static files (`replay.html?match=q42&t=97&ev=brownout`).

## Global

**Header:** wordmark · tabs (Anomalies tab shows the open count in hot) · palette trigger · robot link pill (`LINKED` / `TETHERED` / `NO ROBOT`) · CRT toggle · DEMO badge when mocked. One row at ≥1280px.

**Command palette (Ctrl/⌘ K, or `/` outside inputs):** views; mechanisms (→ Live focus); match logs (→ Replay); devices by name or serial (→ History); anomalies (→ Anomalies); actions (Run pit checks, Toggle CRT). ↑↓ Enter Esc.

**Robot connection states (all screens):** connected (normal), connecting (pill shows `CONNECTING ▌`), lost (pill turns hot `NO ROBOT`; Live freezes on the last values with a hot banner "Connection lost at T+… · showing last values" and keeps recording locally if possible).

---

## Live (`/live`)

**Job:** what's wrong right now, at a glance, during a match.

```
┌ match band: QUAL 42 · BLUE 2   0:53 TELEOP   [AUTO|TELEOP━━━━|ENDGAME]   PAUSE 1× 4× RESTART   ● REC ┐
├ main ─────────────────────────────────────────────────────────┬ rail ───────────────────┤
│ [BATTERY 8.56 V] [TOTAL 331 A] [CAN 62 %] [LOOP 12.1 ms]      │ FOCUS  Shooter L  ▲ WARN│
│ MECHANISMS · 9                                                │  current/temp/limit plot │
│ [Drive FL][Drive FR][Drive BL][Drive BR][Intake]              │  NOW · PEAK · TEMP · RATE│
│ [Indexer][Shooter L][Shooter R][Climber]                      │ ALERTS · 2 unacknowledged│
│ EVENT LOG (newest first)                                      │  [✕ FAULT power …][ACK]  │
└───────────────────────────────────────────────────────────────┴──────────────────────────┘
 footer: nt4 ws://10.68.29.2:5810 · 50 Hz in · 2 Hz render · rx 3.1 ms · dropped 0 · logging Q42.wpilog
```

**Data:** live snapshot per render tick (`live-snapshot.schema.json`) built from NT4 topics, plus FMS match state (match number, alliance, phase, time remaining).

**Match band:** match label, clock showing time remaining in the current FRC period (auto counts 0:15 → 0:00, teleop 2:15 → 0:00), phase name, phase bar from robot config, recording pill. **[prototype shortcut]** PAUSE / speed / RESTART exist only for the demo clock; in production they apply to the log-replay simulator only and are hidden for a real robot.

**Vitals (4):** battery voltage (threshold = brownout voltage from config; footnote: min this match), total current (footnote: peak in window), CAN utilization (threshold 70%), loop time (threshold = loop period; footnote: overruns this match). Each goes FAULT/WARN per its rule; battery vital stays `.is-fault` for the rest of the match once a brownout occurs.

**Mechanism cards:** one per configured mechanism. Status rules (from device registry, per device type, not hardcoded): FAULT if temp ≥ fault limit or a fault flag is active; WARN if temp ≥ warn limit or current > 85% of supply limit for the window; else OK. Clicking focuses it (URL `?sel=`).

**Focus rail:** device name + status, controller type, CAN ID, supply limit, installed device serial with a link to History. 20 s plot of current (solid), temperature (dashed) and limit (hot dashed). Tiles: now, 20 s peak, temperature, temperature rate (°C/min).

**Alerts:** the four most recent WARN/FAULT events, newest first, with ACKNOWLEDGE and OPEN IN REPLAY (deep link to that event). Header shows unacknowledged count. Acknowledgments sync across viewers (production) so the pit sees what the stands acknowledged.

**Event log:** newest first, nine visible, scroll for more.

**Render rules:** render at ~2 Hz; mechanism cards and vitals are created once and updated in place (focus must survive); alerts re-render only when the set or ack state changes; sparkline windows are the last 20 s.

**Keyboard:** Tab through mechanism cards, Enter/Space focuses. Alerts' buttons in order.

---

## Replay (`/replay/:matchId`)

**Job:** what happened, when, and why.

**Left rail:** match logs for the current event (label, result, battery, fault count or CLEAN or NO LOG). Compare: "+ overlay a match" **[prototype shortcut: inert]** overlays a second match's series on every track in `--amber-dim` dashed.

**Main:** title + meta (alliance, result, battery, duration, warning/fault count), EXPORT CSV, COPY LINK (URL with `t` and `ev`). Event markers row above four synchronized tracks: battery voltage, total current, hottest-trending motor temperature, and the mechanism with the most faults (production: tracks are chosen by the match's events; user can add/remove tracks from any logged signal). Shared cursor; scrubber (`<input type=range>`, 0.5 s steps; arrow keys work natively). Auto and endgame shaded.

**Right rail:** selected event: level, source, time, message, **Likely cause**, **Suggested fix** (with the heuristic disclaimer), CREATE PIT TASK, MARK RESOLVED. Below: readout table of every mechanism's current, temperature and status at the cursor.

**Data:** `match-log.schema.json`. Tracks are downsampled server-side (2–10 Hz) for the overview; zooming fetches full-rate data for the visible window **(v1.1)**.

**States:** NO LOG for unplayed matches ("Not played yet. The log appears here when the match ends."); parsing ("Parsing Q42.wpilog ▌" with progress); parse error (hot callout with filename and reason, RETRY).

---

## Pit (`/pit`)

**Job:** can we queue?

**Verdict band:** 96px verdict, next match line, explanation sentence, RUN CHECKS / CHECKING… / RUN AGAIN.

**Checks (v1 set):** CAN bus (devices responding vs expected, bus-off events); firmware (all devices of a type on the same version); sticky faults (any → FAULT, with CLEAR FAULTS after review); battery (selected battery's resting voltage and internal resistance vs spec); motor temperatures (all below queue limit); radio and driver station link; vision (cameras and pipelines up); mechanism homing. Results reveal in order as each probe completes.

**Verdict rule:** any FAULT → **NO-GO**; else any unacknowledged WARN → **DECIDE**; else **GO**. Before running: **NOT CHECKED**. Warnings are acknowledged per run (and logged with who acknowledged them).

**Battery fleet:** table of batteries (ID, resting V, internal resistance, cycles, USE / IN ROBOT). Grades: ≤ 15 mΩ healthy, 16–20 worn (WARN), > 20 retire (FAULT); resting voltage < 12.6 V is WARN **(team-configurable thresholds)**. Selecting a battery re-evaluates the battery check immediately.

**Data:** `pit-check.schema.json`. **[prototype shortcut]** results are canned; production probes over NT4/diagnostics and reads battery tests from the registry (entered from a battery analyzer or imported).

---

## History (`/history`)

**Job:** how is each piece of hardware aging, across robots and seasons?

**Band:** RANGE (each season, ALL TIME), DEVICES (MOTORS / BATTERIES), stats (devices tracked, matches, motor faults, retired in range).

**Chart:** metric selector. Motors: pit spin-test current, peak temperature, faults per match. Batteries: internal resistance, lowest voltage under load. X = matches in order, with event segments labeled (narrow segments use short labels, full label on hover). One line per **physical device**; gaps where it wasn't installed; the selected device in `--amber-hi`, others in `--line-strong`; spec line labeled.

**Table:** device, serial, in service (first → retired), matches/cycles, faults/latest, trend sparkline (own range), health (GOOD / WATCH / RETIRE / RETIRED). Row button selects the device.

**Device rail:** model + serial, name, health, plain-language summary, tiles (matches in service, runtime or cycles, current value, change across range), **lifeline** (installs, moves between slots and robots, repairs, firmware updates, threshold crossings, anomalies, retirement), OPEN ANOMALY when one exists, ADD NOTE, LOG SWAP.

**LOG SWAP** opens a form: device out, device in, slot, match, reason. This is how the registry stays truthful when hardware moves. **(Production must make this fast: it gets done in a loud pit in 30 seconds or not at all. Consider scanning a QR label on each motor.)**

**Data:** `device.schema.json` (registry + installs + life events), `device-metrics.schema.json` (per-match aggregates).

---

## Anomalies (`/anomalies`)

**Job:** catch hardware trending toward failure, with enough evidence to act.

**Band:** open count (hot if any high), SHOW tabs (OPEN · n / RESOLVED · n / DISMISSED · n), SENSITIVITY (RELAXED hides low; NORMAL; STRICT adds watch-level findings).

**Queue (left):** sorted by severity. Each item: severity · device, method, title, first seen, state (NEW / INVESTIGATING / RESOLVED / DISMISSED, plus PIT TASK if one exists). Below: the five methods explained in one line each.

**Detail (main):** severity · method · state; device name; model, serial, link to device history; actions (CREATE PIT TASK, INVESTIGATE or MARK RESOLVED, DISMISS AS EXPECTED; REOPEN when closed); title as a sentence; fact tiles (actual, expected, first seen, matches affected, confidence, method); **evidence chart** (expected band, actual series, sibling series when the method compares, out-of-band points, x-range labels); **Why it was flagged**; **Likely causes**; **Seen this before** (links to past anomalies on the same device or slot); **Outcome** for closed items; disclaimer.

**Dismiss as expected** requires a reason (production) and offers to update the spec, so the same thing doesn't flag again.

**Data:** `anomaly.schema.json`. State changes PATCH the anomaly; history of state changes is kept.
