## Context

See proposal.md for the motivation. The facts that shape the design were measured on 2026-10-10, against the Johnson lake (`~/flashpoint-lake`, Q15/Q53/Q76/Q105) and the corpus lake:

- **Per-device frame clock.** Every Phoenix 6 device in bronze has a `Phoenix6/<Model>-<id>/Timestamp` signal, one sample per status frame received. Its median interval is 8–9 ms on the rio and about 4 ms on the CANivore (9.8 ms for CANcoders). The 2025 hoots (compliancy 13) have it too.
- **Reboot signals.** `ResetCount` is logged on change only, as an integer: Q105 TalonFX 2 has two samples, 0 at 0.42 s and 1 at 344.96 s. `Fault_BootDuringEnable` is sampled at 4 Hz and turns on at 345.16 s. The 2026 hoots carry both. The 2025 hoots carry `Fault_BootDuringEnable` but no `ResetCount`.
- **`ResetCount` and `Timestamp` are not in `HEALTH_PATTERN`.** They still reach bronze under the `health` profile: all 46 Johnson hoots were exported with `health`. owlet's `-s` selection evidently pulls them in with something else. We depend on that, but it isn't documented anywhere.
- **The sibling rule separates the cases.** A gap is an interval of at least 2.5× the device's median. A gap is bus-wide when at least half of the device's siblings have an overlapping gap. While enabled:

  | Case | Isolated gaps | Longest isolated | Bus-wide gaps |
  |---|---|---|---|
  | CANcoders, CANivore, all 5 matches | 0 (1 in Q76) | ≤ 30 ms | 59–79 each |
  | Other CANivore devices, all 5 matches | 0–13 | 12–20 ms | 97–126 each |
  | rio TalonFX 14, Q15 / Q53 / Q76 / **Q105** | 13 / 46 / 14 / **101** | ≤ 27 ms | 1–9 |
  | rio TalonFX 2, Q105 | 3 | **820 ms** | 9 |
  | rio TalonFX 8, all matches | 11–52 | ≤ 57 ms | 1–10 |
  | Corpus Q7, any device | ≤ 11 | ≤ 57 ms | n/a |

  The only isolated gap over 59 ms in the whole sample is Q105 TalonFX 2's 820 ms.

  **As implemented (task 3.x, 2026-10-10).** The table above came from the exploration script, which took the median over the whole log. The spec takes it over enabled time only, which gives 9–10 ms on the rio, because the bus is busier while enabled. That raises the rio threshold to 22.5–25 ms and drops the 20–22 ms micro-gaps. Results with the real module:

  | Case | Q15 | Q53 | Q76 | Q105 |
  |---|---|---|---|---|
  | rio TalonFX 14, isolated | 7 | 20 | 1 | **42** |
  | rio TalonFX 10, isolated | 6 | 24 | 3 | 3 |
  | rio TalonFX 8, isolated | 7 | 5 | 6 | 7 |

  - Q105 TalonFX 14 is still the highest count by about 2×. The other devices are less noisy than before.
  - Q105 also gives TalonFX 2 one reboot (reset count 0 → 1, enabled) and an isolated gap of 820 ms.
  - On the CANivore, every CANcoder has 0 isolated gaps, and the Talons and Pigeon have 3–7, none over 13 ms.
  - Corpus Q7: CANivore devices have at most 4 isolated gaps, at most 13 ms long; on the rio the worst is 57 ms. There are no reboots. The 2025 hoots report `reset_count_logged = false`.
  - Each hoot takes 0.24–0.52 s to analyse.
- **Derive is the right place.** `Deriver.run` already writes per-session gold tables through `gold.write_session(..., table, schema)` (`match_features`, `unit_usage`). It recomputes stale sessions by fingerprint, and the fingerprint includes `DERIVE_VERSION`.
- **Markers today.** Replay markers come from `report/markers.py`. The existing `gap_markers` rule ("no silver samples for > `sample_gap_s`") fires on logger gaps and the end of a log, not on device faults.
- **The bus-status logger (Robot-2026#108)** writes DataLog entries `/Flashpoint/CANBus/{rio,DriveTrain}/{Utilization,BusOffCount,TxFullCount,ReceiveErrorCount,TransmitErrorCount,Status}`. These are plain wpilog entries, not NetworkTables, so no `NT:` prefix. They are written on change at 10 Hz.

## Goals / Non-Goals

**Goals:**
- One new derive step, per session, that writes reboot events, gap events and per-match counts to gold, and reads only bronze, the ledger and configuration.
- Replay markers and History metrics read those gold tables. Neither recomputes anything.
- Keep the thresholds in configuration, with defaults taken from the table above.

**Non-Goals:**
- No per-frame silver table. Gaps are summarised into events and counts; the frames stay in bronze.
- No change to History's health status (still temperature only). Scoring CAN health is P5 Tier 2.
- No Replay track for bus utilisation or error counts. Gold carries them; a view can come later.

## Decisions

### D-a. The frame stream is the device's `Timestamp` signal
Each device's frames come from its `Timestamp` signal in bronze, one row per received frame across all of the device's status groups.
- *Alternative:* the fastest value signal (such as `TorqueCurrent` at 10 ms). Rejected: it misses frames from the other groups, and which signal is fastest depends on how the robot code configures update rates.
- *Gotcha:* we make the `health` profile list `Timestamp` and `ResetCount` explicitly (see D-h).

### D-b. Nominal period = median interval while enabled; gap ≥ 2.5×
These defaults come from the measurements in Context. With them, normal jitter (p99 is 12–13 ms on an 8 ms rio stream) never counts as a gap, and a single missed frame on a 4 ms stream does. Thresholds are module constants (`GAP_FACTOR = 2.5`, `STARTUP_SKIP_S = 5.0`), not user configuration. Changing them changes gold, so it bumps `DERIVE_VERSION`.

### D-c. Enabled windows come from the hoot itself; phases come from framing
- **Enabled intervals** come from the hoot's own `RobotEnable` signal. Each hoot already carries it, so the gap and reboot math needs no alignment, and unframed sessions (practice, nofms) still get events with an `enabled` flag.
- **Phase rows** (`auto`, `teleop`, `match`) cut those events by the session's framing, shifted onto the hoot clock by `session_hoots.offset_us`. This is the mapping the exploration used.
- A hoot with no `offset_us` gets events in lake time and no phase rows.

### D-d. Sibling classification by interval overlap, per hoot
The steps, per hoot:
1. One DuckDB query computes each device's frame intervals: `lag` over `ts_us`, partitioned by signal.
2. A filter keeps the gaps (≥ 2.5× the device's median). This leaves hundreds of rows per match, not millions.
3. Gaps are self-joined on `a.g0 < b.g1 AND b.g0 < a.g1 AND a.dev <> b.dev`.
4. Overlapping devices are counted against the number of devices measured on that hoot, minus one.

Overlap is interval arithmetic, so no step joins on equal timestamps, which keeps clear of pitfalls #14 and #38.
- *Alternative:* bucket gaps into 20 ms bins, as the exploration script did. Rejected: a gap that straddles a bin edge splits into two events.
- *Siblings are per hoot, not per configured bus.* A hoot is one bus, and unmapped devices still count as siblings.

### D-e. Three gold tables, written through the existing per-session writer
| Table | Key | Holds |
|---|---|---|
| `gold/can_health` | match, phase, slot, unit | `reboots`, `reboots_enabled`, `isolated_gaps`, `missed_per_min`, `longest_gap_ms`, `reset_count_logged`, `bus`, `robot`, `alignment`, `source_logs` |
| `gold/can_bus_health` | match, phase, bus | `logger_gaps`, `logger_gap_s`, `longest_logger_gap_ms`, `devices`, `util_max`, `util_mean`, `bus_off_delta`, `tx_full_delta`, `rx_err_max`, `tx_err_max` (all bus-status columns nullable) |
| `gold/can_events` | session, bus, device, t_us | `kind` (`reboot` / `dropout` / `bus_gap`), `slot_id`, `unit_id`, `match_t_s`, `enabled`, `duration_ms`, `reset_before`, `reset_after` |

`can_events` holds every reboot. It holds gaps only when they are at least `EVENT_FLOOR_S = 0.1` long, so it stays small: Q7 has under 20 bus gaps of that length.

The new module is `src/flashpoint/semantics/can_health.py`. `Deriver.run` calls it for each stale session, next to `usage.session_usage`, and writes with `gold.write_session(..., table, schema)`. Unit attribution reuses `slot_observations` (`from_ts_us` / `to_ts_us`), the same way silver does, so a mid-session swap splits correctly. `DERIVE_VERSION` goes from 3 to 4.

- *Alternative:* add the slot columns to `match_features`. Rejected: that table is built from silver, which has no frame timing, and bus rows don't fit its key.

### D-f. Markers: the CAN rules replace the silver sample-gap rule
`gap_markers` (silver silence > 1 s) is removed. Three rules take its place, all reading `gold/can_events` for the session:
- `reboot`: FAULT, on the slot.
- `dropout`: WARN on the slot, for isolated gaps ≥ `dropout_warn_s` (default 0.25 s).
- `bus_gap`: WARN on `bus:<name>`, for bus-wide gaps ≥ `sample_gap_s` (default 1 s).

`sample_gap_s` keeps its name, so existing `report.toml` files still load. Its comment changes to "whole bus silent this long". `dropout_warn_s` is new, and a value below `EVENT_FLOOR_S` is rejected when the configuration loads.

We drop the old rule because it measured the same thing badly. Silver gaps are hoot frame gaps, but the old rule had no sibling test, so it charged logger and log-end silence to slots. `REPORT_VERSION` goes from 2 to 3.

### D-g. History: metrics name their table
`Metric` gains `table` (default `match_features`). Four CAN metrics are added with `table = "can_health"`: `reboots`, `isolated_gaps`, `missed_per_min` and `longest_gap_ms`. The trend and device queries read `gold/{metric.table}`. They keep the same filter predicates, because both tables carry match, phase, slot, unit, robot and the event fields. A match with no row is simply absent from the series, so the line has a gap there and never shows a zero.

### D-h. `health` profile names the signals we rely on
`HEALTH_PATTERN` gains `ResetCount|Timestamp`. Task 1.2 re-ingests the corpus and compares bronze row for row.
- If bronze is unchanged, which is expected since owlet already emits both, `PIPELINE_VERSION` stays at 2.
- If it changes, `PIPELINE_VERSION` goes to 3, and the release notes say to run `flashpoint rebuild`.

**Result (task 1.2, 2026-10-10):** unchanged. The golden corpus ingested with the old and the new pattern gives the same bronze: 25,447,616 rows each, and zero rows differ (compared on log, signal, ts_us and values). `PIPELINE_VERSION` stays at 2.

### D-i. Bus-status signals: a `flashpoint` season root plus named buses
- **The season root.** `SeasonRoots` gains an optional `flashpoint` root. The 2026 season sets it to `/Flashpoint/`, and `root_names()` leaves out roots that are unset. The existing check rejects a signal under an undefined root (`robot_config.py:179`).
- **The 2026 signals.** The 2026 robot declares 10 signals: `CANBus/<bus>/<Metric>` for `rio` and `DriveTrain`. `Status` is skipped, because it is an enum code, not a measure.
- **Named buses.** Robot configuration gains `[[bus]] name = "DriveTrain" hoot = "canivore"` (and `rio` → `rio`). The `hoot` field is validated like `Slot.bus`. The bus row joins silver signal samples whose `component` is the bus name.

Not every robot has to declare buses. A robot without `[[bus]]` gets empty bus-status columns.

### Decisions D1–D11
Followed, with no deviation and no new ADR:
- D1/D2: DuckDB SQL over Parquet; Polars only to hand rows to the writer.
- D6/D7: Replay and History read gold.
- D10: this change is the rules tier, and supplies the counts that P5's per-unit baselines will use.
- D11: rows are per unit, with serials where an inventory exists.

## Budgets

The step touches full logs, since it reads every `Timestamp` row of a match.
- **Rows.** Q7 has about 3 M `Timestamp` rows: 9 rio devices at 8 ms and 13 CANivore devices at 4 ms, over ~2.5 min enabled plus the disabled time.
- **Query.** One `lag` window over two columns (`ts_us` int64, signal dictionary), with the connection's existing `memory_limit = 512MB` and `threads = 2`.
- **Target.** At most 3 s and at most 150 MB of extra peak RSS (VmHWM on Linux) per match, as the spec requires. Task 5.3 measures it, and the corpus perf job asserts it.
- **Season scale.** A full season (~60 matches) adds under 3 min to `derive --force`. The History CAN queries read tables with one row per match, phase and slot, so they stay well inside the existing 2 s refresh budget.

## Risks / Trade-offs

- **[Risk] The thresholds come from 5 matches on one robot.** → They are constants with the measurement table above as their rationale. Task 7.1 re-runs the Johnson field check, and P5 Tier 2 baselines will catch drift. A rio device with a high, steady isolated count (TalonFX 8) reads as its own baseline, not a fault, because History shows trends per unit.
- **[Risk] A real bus fault that silences more than half the bus counts as a logger gap, not against any device.** → Whole-bus silence of ≥ 1 s still gets a bus marker. Error counters from the bus-status logger will separate the two once Robot-2026#108 runs on the robot.
- **[Risk] owlet's tail loss (#17) can cut a hoot's end.** → The hoot-end rule (D-c) treats the last frame of any device as the end of the log, so a short export never produces gaps. Lost reboot events in a dropped tail are possible. That is already a known #17 limitation.
- **[Risk] `ResetCount` resets at power-on.** A robot power cycle between logs looks like a "baseline" in the next log. → Only increases within one hoot count, as the spec requires. A power cycle is not a device reboot.
- **[Trade-off] Removing the silver gap marker** means a slot whose wpilog-only data stops is no longer flagged. Today every slot comes from hoots, so nothing is lost.

## Migration Plan

1. Merge, then run `flashpoint derive`. `DERIVE_VERSION` 4 recomputes every session, and gold gains the three tables.
2. Run `flashpoint report`. `REPORT_VERSION` 3 rebuilds Replay data with the new markers.
3. If D-h bumped `PIPELINE_VERSION`, run `flashpoint rebuild` first.

Rollback means reverting the commit and re-running derive and report. The old tables are untouched, and the new ones become orphaned. `derive` doesn't read them, and they can be deleted.

## Open Questions

- Are rio TalonFX 2 and TalonFX 14 adjacent on the rio chain, and what did the pit find after Q105? This is useful for the labelled-case notes, but changes no rule here. Chain order is deferred to 2027.
