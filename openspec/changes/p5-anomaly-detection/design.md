## Context

See proposal.md for the motivation. This section covers the state of the code that shapes the approach.

- **Gold is small and per match.** `gold/match_features` has one row per (match, phase, slot, unit). It holds current means and P95s, temperatures, rise rate, energy, `stall_s`, and `residual_p95_a`. `gold/can_health` adds reboots, isolated gaps, missed frames per minute, and the longest gap per (match, phase, slot, unit). One season is about 60 matches × 23 slots, which is tens of kilobytes.
- **Unit ids** are CTRE serials or `legacy:<robot>:<slot>:<epoch>`. Josh's lake has only legacy ids today, because no robot has logged `/Flashpoint/CANInventory` yet. Baselines key on `unit_id` either way, so a declared swap starts a new epoch and a new baseline (D11).
- **Brownouts** exist only as Replay markers. `report/markers.power_markers` computes them from the silver battery proxy (the lowest device supply voltage) using `config/report.toml` thresholds. No gold table counts them.
- **Derive** runs as a subprocess (`flashpoint derive`) after ingest, in the CLI and in the `acquire` cycle. It writes gold per session and bumps `DERIVE_VERSION` (currently 4) when gold semantics change.
- **Serve** is stdlib HTTP plus a DuckDB JSON API (ADR-0013). The only non-GET route is the guarded `POST /api/launch` (ADR-0014). Its guard (`advantagescope.refusal`) checks the local bind, a loopback client, and the Origin and Host headers.
- **The ledger** is `lake/meta/flashpoint.sqlite` (WAL). `backup` snapshots the SQLite image to `<remote>/meta/latest`, and `restore` brings it back.
- **Data volume.** Josh's lake has 4 Johnson matches, and the corpus adds GACMP Q7 and a few 2025 logs. Most units cannot leave the "building baseline" state until more 2026 logs are ingested. Josh will add them.

## Goals / Non-Goals

**Goals:**
- One detection pass that is deterministic, has no lookahead, and recomputes the whole anomalies table from gold, configuration, and triage in seconds.
- Anomaly ids that are stable across reruns, so triage stays attached.
- The first writable surface in the app: triage only, local only, append-only.
- A backtest that costs one detection run, not one run per match.

**Non-Goals:**
- Incremental detection. A full recompute is cheap at this data size.
- Writing back to gold from the server. The server writes only triage rows, and may start a `detect` subprocess.
- Any network call during detection or backtest.

## Decisions

### D1. Detection is a separate step that runs after derive
`flashpoint detect` reads gold, `can_health`, the brownout cache, configuration, and triage. It writes `gold/anomalies/anomalies.parquet` and `gold/anomalies/summary.json` atomically, via the existing temp-then-rename helper. `derive` calls detection in-process as its last step when gold changed. The acquire cycle also runs `detect` when a baseline restart is pending (see D7). Detection takes its own lock, `meta/detect.lock`, using the `AcquireLock` class. It cannot take the acquire lock, because the watch holds that lock for its whole run, including the derive subprocess.

A detection fingerprint is stored in `summary.json`. It is a hash of the gold file list with sizes and mtimes, the configuration file hashes, the brownout cache, and the triage restart revision. Detection exits early when nothing has changed.

- *Alternative:* fold detection into derive's per-session loop. Rejected: detection needs the whole history of each unit, and derive is per session.
- *Alternative:* compute anomalies at query time in the server. Rejected: the work would repeat on every request, and the backtest and CLI need the same results without a server.
- **D10:** followed. Rules, a robust z-score against per-unit history, and the residual as a baseline metric.

### D2. One time-ordered frame, judged per series with numpy
Detection builds one frame of whole-match rows (`phase = 'match'`). It joins `match_features` and `can_health` on (match_key, slot_id, unit_id) and orders them by `session_start` from `gold/unit_usage`, with `match_key` breaking ties. Each (unit, metric) series becomes a numpy array, and is judged in a loop at index *i* using only indices before *i*.

- **Baseline at *i*:** take the previous `window` values (default 20), starting no earlier than the unit's baseline-restart match for that metric. Center = median. Spread = max(1.4826 × MAD, floor[metric]).
- **Floors:** 1 °C for temperatures, 0.5 °C/min for rise rate, 2 A for current P95, 5 A for residual P95 (the residual is noisy; one Q76 drive row has a teleop P95 of 115 A), 0.5 s for stall, 1 for gap counts, and 0.5 for missed frames per minute.
- **z** = (x − center) / spread.
- **STEP:** the last `step_persist` values (default 3) all have |z| ≥ `step_z` with the same sign.
- **DRIFT:** take the median of the last 5 values against the baseline of the window before them. The drift statistic is that difference divided by the spread, and DRIFT is flagged when it is ≥ `drift_z` and STEP is not.
- **Fewer than `min_baseline` (5) prior values:** no flag. The unit counts toward "building baseline (n of 5)" in `summary.json`.

Per-match flags are kept as a boolean array per (subject, method, metric), together with the z-score array. Episodes are runs of flagged indices, joined across gaps of up to 2 unflagged matches; three in a row ends the episode. The id is `sha1(subject|method|metric|onset_match_key)[:16]`.

- *Alternative:* EWMA or CUSUM. Rejected for v1: median/MAD is what ADR-0010 names, students can explain it, and it is robust to the single bad matches the corpus contains.
- *Alternative:* polars rolling expressions. Rejected: the series are short, and an explicit loop makes the no-lookahead rule obvious and testable.

### D3. Sensitivity tags one set of episodes
Detection runs the per-match tests once with the **strict** thresholds (normal z minus 1.0, persistence 2), which give a superset. It then tags each episode with the lowest sensitivity at which it appears:
- `normal`, if any match in it passes the normal thresholds;
- otherwise `strict`;
- LOW episodes are hidden at `relaxed` by the view.

So an anomaly keeps one id whatever the sensitivity, and its onset is the strict onset. This matches the canvas, where sensitivity is a filter, not a recompute.

### D4. SPEC rules
SPEC rules are evaluated per match from the same frame, with the limits from `config/anomaly.toml [spec]`:
- temperature 65/75 °C maximum and 55 °C mean;
- stall 5 s (3 s before the 9.3 tuning);
- reboots (`reboots_enabled > 0` → HIGH, other reboots → MED);
- longest isolated gap 0.25 s / 1 s.

The SPEC temperature limits default to the `report.toml` marker thresholds, but are their own keys, so they can be tuned without moving Replay markers. Brownouts are the exception: the count comes from the same marker logic, so the two always agree (spec: Brownouts agree with Replay).

### D5. Brownout cache from the Replay marker logic
Detection reuses `report.markers.power_markers` to count brownout FAULT intervals per match. Results go to `gold/anomalies/power_counts.parquet`, one row per match_key. The row holds the count, the lowest proxy voltage, the silver partition fingerprint (mtime and size), and a hash of the brownout thresholds. Only matches whose fingerprint or thresholds changed are recomputed, one match's silver at a time.

Measured cost on Q7 (task 2.4): 0.10 s and +69 MB peak RSS uncached. The budget is ≤ 0.5 s per match.

The span and primary session come from `report.build.match_span`, extracted from Replay's match payload so both use one code path (same primary session, same window).

- *Alternative:* a new gold `match_power` table written by derive (`DERIVE_VERSION` 5). Rejected: derive would then depend on `report.toml` thresholds, every lake would need a full re-derive, and two copies of the brownout rule could disagree.

### D6. Sibling comparison
Groups come from `[[siblings]]` in the robot TOML. The shipped groups are:
- `drive` (4 slots) and `steer` (4 slots);
- `flywheel`: flywheel-left-leader, flywheel-right, and flywheel-left-follower;
- `intake-roller`: leader and follower;
- `intake-extension`: leader and follower. That pair is the Q105 case.

For each group, metric, and match where every member has a row, each member gets d = x − median(others). Each member's d series then gets the D2 baseline treatment, with its own median and MAD of past d, `sibling_z` = 3.0, and a rule of 3 of the last 5 group matches. A group of two produces a d of equal size and opposite sign for each member. The anomaly goes to the member whose own z (against its own baseline) is larger, and the evidence series carries the peer's values.

### D7. Triage storage and baseline restarts
Triage lives in a new ledger table, `anomaly_triage`, which is append-only. Its columns are `seq INTEGER PRIMARY KEY`, `anomaly_id`, `at` (UTC ISO), `from_state`, `to_state`, `outcome`, `restart_baseline`, plus copies of `subject`, `metric`, and `onset_match_key`, so a restart can be applied after the anomaly row is gone. The current state is the last row per `anomaly_id`.

- The ledger's schema migration adds the table with `CREATE TABLE IF NOT EXISTS`.
- `rebuild` and a `PIPELINE_VERSION` change must not touch it. Task 1.4 tests that.
- Backup carries it (the SQLite image), and the restore drill verifies it.

A restart applies to one (subject, metric): detection reads the latest restart rows, and those set the earliest baseline index for that series. `summary.json` records the highest `seq` it consumed. When a newer restart row exists, the API reports `restart_pending`.

After writing a restart, the server spawns `python -m flashpoint detect --lake <lake>` detached. If `detect.lock` is held, that run exits with `EXIT_LOCKED`, and the acquire cycle picks it up, because `run_cycle` runs detect whenever `restart_pending` is true.

- *Alternative:* a separate `triage.sqlite`. Rejected: backup and restore already carry the ledger image, and a second database is a second thing to lose.
- *Alternative:* a mutable state column. Rejected: the spec requires a readable history, and append-only rows make concurrent writers harmless.

### D8. API and guard
- `GET /api/anomalies?sens=&tab=&unit=&match=` returns queue items, counts, `scanned_through`, `building_baseline`, and `restart_pending`.
- `GET /api/anomalies/<id>` returns the detail. That includes evidence, causes, seen-before, and triage history. Causes and seen-before are resolved at read time, so catalogue edits and new outcomes show without a detect run.
- `GET /api/anomalies/summary` returns the navigation badge.
- `POST /api/anomalies/<id>/triage` takes JSON of at most 4 KB: `{to, outcome, restart_baseline}`. It reuses `advantagescope.refusal` (local bind, loopback client, Origin and Host match). It also checks the content type (JSON) and validates the transition table and the outcome length.

Because served Replay reads badges from `GET /api/anomalies?match=<key>`, `REPORT_VERSION` does not change. Server docstrings and ADR-0013/0014 currently say the launch route is the only non-GET route; a new **ADR-0015** (anomaly triage writes) records the exception.

### D9. Views
- `web/static/views/anomalies.js` follows the canvas board. It registers through `FP.view` with no new CSS beyond the shell tokens, and is a classic script like the others.
- The evidence chart is uPlot (already vendored), with the band drawn as a fill series and the flagged matches as points.
- The shell gains a badge slot on navigation items.
- Replay shows a match-list glyph and track badges. History's device table gains anomaly counts and health.
- `report --static` writes `anomalies-snapshot.js` (`FP.anomalySnapshot = {...}`), filtered to the exported matches and carrying its `taken_at` time.

### D10. Optional model detector
Optional dependency group `ml` holds scikit-learn. When `[model] enabled = true`, detection imports it lazily. It trains an `IsolationForest(random_state=0, n_estimators=100)` per subsystem, on all unit-match feature vectors from earlier **events**. Each feature is robust-scaled per unit, and the model is refit once per event, not per match, to keep the cost bounded. It scores the event's matches and flags the top `contamination` (1 %).

MODEL anomalies are capped at MED and tagged `normal`. They are explained by the three features with the largest per-unit |z|. The model is excluded from the detection budget. The no-lookahead rule holds, because training uses only earlier events.

The proposal deferred NOISE, STUMPY, and River.

### D11. Backtest as one detection run
Every per-match flag already uses only earlier data (D2 and D10), and triage is ignored, so a single detection run with restarts disabled gives the as-of state at every match: truncate the flag arrays at *t*. The backtest builds the frame once and scores each label from the per-match flags within the label's 5-match window. It writes `lake/report/backtest/<UTC>.md` and `.json`. The JSON is canonical, with sorted keys and no timestamps inside, so two runs are byte-identical.

The "equals live detection" test runs `detect` on a lake truncated to the first 20 matches and compares the results.

### D12. Configuration files
- `config/anomaly.toml`, with sections `[spec]`, `[baseline]`, `[sibling]`, `[strict]`, and `[model]`;
- `config/anomaly-causes.toml`, with `[[cause]] method, metric, subsystem?, text`, seeded from the canvas's examples where real data supports them;
- `config/failure-labels.toml`, with `[[label]]`;
- `[[siblings]]` in the robot TOMLs.

Each file is loaded with `tomllib` and strict key validation, following `robot_config.py`.

### Budgets
| Run | Time | Peak RSS | Notes |
|---|---|---|---|
| `detect`, 1 season (60 × 23), warm brownout cache, model off | < 5 s | < 400 MB | gold read via DuckDB, `threads=2`; measured (5.6): 1.6 s, 222 MB |
| brownout count, one uncached match | ≤ 0.5 s | one match's silver | Q7 measured (2.4, macOS M-series, 2026-10-11): 0.10 s, +69 MB peak RSS |
| `backtest`, 2 seasons (120 × 23) | < 60 s | < 1 GB | one detection pass; measured (9.2): 3.2 s, 256 MB |
| `GET /api/anomalies` | < 500 ms | — | reads one Parquet file |
| derive + detect on Q7 (perf test) | stays under the 30 s / 1 GB ingest budget | | measured (5.6): ingest + derive + detect 14.6 s, derive child 700 MB |

## Risks / Trade-offs

- **Too little history, so detection is silent** → "building baseline" is reported in the view and the backtest. The Q105 degradation label is expected to be missed until more 2026 logs are ingested. Josh is adding them, and task 9.2 re-runs the backtest then.
- **False-positive flood** (the residual is noisy, and the stall default is a guess, already raised from 3 s to 5 s) → MAD floors, persistence, sensitivity tags, and a backtest FP report per method before anyone relies on it. Thresholds are in config, not code.
- **Ids shift when configuration changes onsets** → triage is kept and shown as "no longer detected". Students re-triage the new id. This is acceptable, because configuration changes are rare.
- **First write route** → the same guard as the launch, append-only rows, a 4 KB body cap, JSON-only, and no write in shared or static mode. ADR-0015 records it.
- **The server and the watch both touch SQLite** → WAL and busy timeout are already in the ledger. Triage inserts are single-row transactions.
- **The detached detect after a restart races the watch's derive** → both take `detect.lock`. The loser exits `EXIT_LOCKED`, and the cycle picks up `restart_pending`.
- **Legacy unit ids hide swaps that were never declared** → an undeclared swap looks like a STEP on one "unit". That is a true positive about the data, and the explanation's causes catalogue includes "unit swapped without a recorded swap".

## Migration Plan

- `PIPELINE_VERSION` and `DERIVE_VERSION` are unchanged. The first `derive` or `detect` after upgrading creates `gold/anomalies/`, and the ledger migration adds `anomaly_triage`.
- `REPORT_VERSION` is unchanged, and static exports gain a snapshot file.
- **Rollback:** delete `gold/anomalies/`. The triage table is inert to older code.

## Open Questions

- Default stall-time and residual floors are expected to move after the first real backtest (task 9.2). That changes configuration values only.
- Whether the model detector earns its keep. Decide after a season of backtests, with no structural impact.

## Implementation notes (2026-10-11)

Decisions and deviations found while implementing; the specs are unchanged unless stated.

- **STEP needs a jump; a STEP suppresses DRIFT (refines D2).** With the D2 rules as written, a slow drift becomes a STEP once 3 values pass `step_z`, and after a real step the new level reads as a DRIFT while it enters the baseline window. That breaks the Sudden step and Slow drift scenarios. So a STEP also needs its run's median to be `step_z` spreads from the `persist` values just before the run. A DRIFT is suppressed while any STEP flag (at the same sensitivity) sits in its baseline window or run. Both are in ADR-0010's amendments.
- **Strict and normal flags are unioned.** Episodes come from the union of the strict and normal per-match flags, so the strict onset is kept even where suppression differs between sensitivities. The sensitivity tag is as D3 describes.
- **Event changes are quiet (added 2026-10-12, refines D2 and D3).** With GADAL ingested, half the new HIGH anomalies were STEPs at the first match of the next event: followers enabled, current limits raised, a roller rebuilt. They were real but deliberate, and a manual "restart baseline" per unit after every event would never happen. A STEP whose onset is the unit's first match at a new event, after an earlier event, is now LOW and tagged strict, with a title naming both events. It still suppresses DRIFT. The alternative, restarting every baseline at each event, would leave each unit blind for its first 5 matches of every event. A failure that starts exactly at an event boundary is visible only at strict; that is the trade-off. `[baseline] quiet_event_steps = false` turns it off. Steps that start later in an event (the drive current-limit change at GACMP qm33) are unaffected. `DETECT_VERSION` is 2.
- **Restarts keep earlier judgements.** Matches before a restart point are still judged against their full history. Only matches from the onset onward use the restarted baseline, so older anomalies on that metric stay. The restarted anomaly itself becomes "no longer detected", and its triage is kept.
- **Sibling groups are left out of the derive and Replay fingerprints.** Declaring them does not re-derive or rebuild anything.
- **One span for Replay and brownouts.** `report.build.match_span` was extracted from Replay's match payload. Detection's brownout count uses the same primary session and window, so the counts always agree. Q7 has 6 brownouts.
- **Restart checks are stdlib-only.** `anomaly/restarts.py` uses only sqlite3 and json, so the acquire watch can check `restart_pending` every cycle without loading polars or numpy. Exit 3 (lock held) is not an error for the cycle.
- **SPEC confidence** uses the subject's prior match count, capped at `window`, as its baseline size. A one-match SPEC finding is Low, as the spec says.
- **Reopen returns to `new`.**
- **Module layout.** The anomaly read and write service is `views/anomalies.py`, not `views/queries.py`. `HistoryQueries` takes an optional `AnomalyQueries` for health. A served app always has the service (`FlashpointServer` defaults it), so the Anomalies view and its badge never 404.
- **Static snapshot** goes to `OUT/data/anomalies-snapshot.js` as `FP.anomalySnapshot = {...}`, next to the match data.
- **Resolved anomaly drops its badge (corpus).** Real Q7 also has three MED SPEC stall anomalies on the intake (the stalls that coincide with its brownouts). Resolving the brownout removes its badge, and Q7's glyph drops from HIGH to MED rather than disappearing. The browser test asserts that.
- **No lookahead (spec scenario).** An anomaly still running at the cutoff keeps its id and onset; one finished by the cutoff is identical. The test checks both directions.
- **Backtest "caught".** An anomaly about the label's unit or slot (and metric, when given) counts when it was detected at or before the label's match and has a flagged match within the 5 before it. Lead time is the label's match index minus the detection match index, in that robot's match order. A date label resolves to the robot's last match on or before that date.
- **The corpus has gold for one match (Q7).** Its anomalies are 1 HIGH SPEC brownout (6 brownouts) and 3 MED SPEC intake stalls. Josh's Johnson lake gives the HIGH Q105 roller reboot, the MED 820 ms gap, stall MEDs, and brownouts in all four matches (one HIGH episode).

## Tuning, first pass (9.3, 2026-10-11)

Lake: Johnson plus GACMP (19 matches with gold). Josh changed four values, which are now the code defaults and the shipped config:

| Setting | Was | Now |
|---|---|---|
| `anomaly.toml` `[spec] stall_med_s` | 3.0 | 5.0 |
| `anomaly.toml` `[baseline.floors] isolated_gaps` | 1.0 | 2.5 |
| `anomaly.toml` `[baseline.floors] missed_per_min` | 0.5 | 1.0 |
| `report.toml` `brownout_hold_s` | 0.02 | 0.5 |

Results at normal sensitivity: 165 → 136 anomalies (30 HIGH, 47 MED, 59 LOW). Both evaluable labels (Q105 roller reboot, GACMP Q7 brownouts) are still caught with lead 0. The follower label is still missed (building baseline, 3 of 5). False positives per 10 robot-matches:

| Method | Before | After |
|---|---|---|
| DRIFT | 24.2 | 18.4 |
| SPEC | 7.4 | 6.8 |
| STEP | 6.8 | 5.3 |
| SIBLING | 1.1 | 0 |

Still loud: DRIFT `missed_per_min` (10), SPEC stall (10), DRIFT temperatures (12, plausibly the venue change), and STEP residual and stator current (4 HIGH each). The longer brownout hold did not change the count: one episode still spans 16 matches with 25 brownouts, which points at the 6.75 V proxy threshold rather than the hold. 9.3 stays open for the next pass.

With the hold at 0.5 s (corrected from 0.05 s): 137 anomalies (31 HIGH, 47 MED, 59 LOW), SPEC 7.37 per 10 robot-matches, labels unchanged. The brownout episode splits into Q7 alone and qm33 → Johnson qm105. 14 matches still hold below 6.75 V for 0.5 s or more, with lows of 5.6–6.5 V.
