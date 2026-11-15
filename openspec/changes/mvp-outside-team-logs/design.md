## Context

See proposal.md for why. The current state, verified against the code on `development` (c027d6d):

- **Identity.**
  - `semantics/derive.py` `derive_sessions()` calls `identify()` with only the wpilog's FMS fields and `FRC_` filename when the session has a wpilog. The hoot group's `event`/`match` label is used only in the `else` (hoot-only) branch.
  - `meta/extract.py` reads FMS only from entries named `…/FMSInfo/<field>`. `_WPILOG_NAME` only matches `FRC_\d{8}_\d{6}…`.
  - `git grep -i -e akit -e advantagekit src tests` finds nothing.
- **Anchor.** `_anchor()` uses `systemTime`, otherwise the filename time. A filename anchor gives `utc_start` but no `utc_offset_us`, so `utc_end` stays empty. `sessions.group_sessions()` needs both, so such a wpilog never pairs with its hoots, and they become a hoot-only session. AdvantageKit writes its own wpilog, not through `DataLogManager`, and we believe it writes no `systemTime` (unverified). So for AdvantageKit, pairing is the real blocker, not only identity.
- **Pro.**
  - `hoot_logs.pro_licensed` is recorded per hoot (`readers/hoot.py` `check_pro`).
  - The only consumer is `report/build.py` `_temperature_reason()`, which is all-or-nothing per match.
  - `views/queries.py` `_health()` returns `NO-DATA` whenever `temp_max` is None.
  - `setup/checks.py` has no Pro check, so the roadmap's §6 risk row ("`doctor` flags any log that isn't") is false.
  - Only TalonFX has `DeviceTemp`. `owlet --scan` on Ignite's raw CANivore hoots (reviewed 2026-11-14) lists no temperature signal for CANcoders, but it does list `Pigeon2-10/Temperature` (the IMU's own temperature). The health profile (`readers/hoot.py` `HEALTH_PATTERN`) doesn't export it and `semantics/silver.py` doesn't map it, so bronze never holds it. Flashpoint tracks no Pigeon 2 temperature. It doesn't follow that the device has no sensor.
  - `NO_TEMPERATURE_MODELS` and `PRO_ONLY_SIGNALS` (used below) are new constants. Neither exists today.
- **Anomalies.**
  - `anomaly/settings.py` `DEFAULT_FLOORS` sets `isolated_gaps` 2.5 and `missed_per_min` 1.0, whatever the rate.
  - On the Ignite lake (38 matches, 289 episodes), 65 are CAN-gap findings (22%). 41 of them are on 9 CANivore slots, with a median longest gap of 12.2 ms against a ~4 ms nominal period. That is jitter. Their |z| runs from about 2 to 8.5.
  - Median frame intervals measured from bronze `…/Timestamp` on 2026 lake hoots (reviewed 2026-11-14):
    - CANivore Talon FX and Pigeon 2: 3.99 ms (250 Hz).
    - CANivore CANcoders: 9.4–9.8 ms (about 105 Hz).
    - rio Talon FX: 9–10 ms (100–111 Hz).

    The rio is not a slow bus. `can_health.py` `GAP_FACTOR` is 2.5.
  - `anomaly/power.py` already stores which proxy a brownout used (`proxy`: the slot, or "median of N devices"). Severity ignores it.
  - `defaults/anomaly.toml:50` and `anomaly/model.py:21` give Poetry commands.
- **Corpus.** Every corpus-v1 file is Ignite's. All hoots are Pro (`test_ingest_corpus.py` asserts it), and the robot runs CTRE swerve on a CANivore.
- **Ledger.** `lake/ledger.py` `_migrate()` returns early once `files.attempts` exists, so new columns need their own `PRAGMA table_info` check. Appending an `ALTER` to the current body won't work.

**05 decisions this change touches:**
- **D5 (temperature source).** Amended: non-Pro hoots become an expected input (ADR-0005 amendment, task 7.2).
- **D10 (anomaly v1).** Refined, not replaced: the brownout cap (§6) and rate-relative floors (§7) go in an ADR-0010 amendment.
- **D3 (wpilog reader).** Unchanged: the new entry names go through the same reader.
- **D6 (Replay payload).** Unchanged: per-bus reasons add a few strings, well inside 2 MB.
- **D11 (device identity).** Unchanged: `temp_status` is per unit, and model comes from the slot.
- **No ADR.** The identity ranks in §1 refine the config.yaml order and don't replace it.

## Goals / Non-Goals

**Goals:**
- Follow the config.yaml identity order (in-log FMS, then filename, then TBA) for AdvantageKit and hoot-labelled sessions, without an ADR.
- Decide "why is temperature missing" in one place, so History, detection's summary and Replay agree.
- Keep CAN-gap judgement measured per device. Change how a measurement is judged, never the measurement.
- Make everything testable on synthetic fixtures today. Real outside logs then confirm it, or correct it if they don't match.

**Non-Goals:**
- Enable-edge alignment from `/DriverStation/Enabled`, and AdvantageKit's `RealOutputs`/replay structure.
- A per-model temperature capability database beyond the CTRE devices in the Phoenix 6 device list.
- Changing the battery proxy rule itself (the battery-proxy change owns it).

## Decisions

### 1. Identity order: where `/DriverStation` and the hoot label slot in (no ADR)
config.yaml's order is in-log FMSInfo, then the filename, then TBA. This change keeps it and fills in the ranks:

1. **In-log FMS.**
   - FMSInfo when it has a match number, else AdvantageKit's `/DriverStation/*`, taken as a whole set, never mixed per field.
   - Both are the driver station's own FMS data, recorded in the log, so `/DriverStation` is in-log FMS data, not a new source. The ledger records `fms_origin` (`fms-info` | `driver-station`) for provenance.
2. **Filenames.**
   - 2a: the wpilog name, `FRC_` or `akit_`.
   - 2b: the hoot group label (`EVENT_Q7_…`), which CTRE's signal logger takes from the same FMS data at log start.
   - The hoot label ranks below the wpilog name because a hoot group can restart mid-session (corpus E10).
   - `match_source` gains `hoot-filename`, also used for hoot-only sessions, which were labelled `filename`.
3. **TBA.** Unchanged: optional and online only.

This refines the order without reordering it, so no ADR is needed. Task 7.2 adds a dated note to `docs/rewrite/05-target-architecture.md` §4. The config.yaml domain rule still reads true, so it stays as it is.

Alternatives:
- Use the hoot label even when the wpilog has identity, and warn on conflict. Rejected for now: it changes nothing for correct logs, and it adds warnings to corpus sessions whose hoots restarted.
- Merge FMSInfo and `/DriverStation` per field. Rejected: a mixed record can name an event from one source and a match from the other.

**AdvantageKit names, to confirm on a real log (task 2.1):**
- Fields: `/DriverStation/EventName`, `MatchNumber`, `MatchType`, `ReplayNumber`, `AllianceStation`.
- Filename: `akit_yy-MM-dd_HH-mm-ss[_EVENT_{p|q|e}n].wpilog`, taken as UTC (the roboRIO runs UTC).
- Epoch entry: `/SystemStats/EpochTimeMicros`.

They live in one constant block in `meta/extract.py`. A real log that differs is fixed there, and the spec scenarios still hold.

**MatchType:** accept 1/2/3 and the names `Practice`/`Qualification`/`Elimination` (case-insensitive). Anything else is absent.

### 2. AdvantageKit wall-clock anchor
`_anchor()` tries `systemTime`, then the AdvantageKit epoch entry, then the filename. It uses the same >2020 plausibility filter and the median offset. With an offset, `utc_end` exists, and `group_sessions()` pairs the log with its hoots unchanged.

Alignment then falls to `wall-clock` (very low). AdvantageKit's CTRE swerve template doesn't publish `DriveState/Pose` to NetworkTables, and enable edges read `DS:enabled`. Framing still works, because it prefers hoot `RobotMode` (`framing_for`). Replay shows the existing low-alignment warning. That is honest for M2. The one-line `/DriverStation/Enabled` alias for enable edges stays post-MVP, per the brief; see Open questions.

Alternative: estimate a filename-anchored `utc_end` as the start plus the log span. Rejected: AdvantageKit renames its file when the clock syncs, not at log start, so the window would be wrong by an unknown amount.

### 3. Already-ingested wpilogs
Metadata is extracted at ingest, and the ledger skips known files. A lake from 0.2.0 holding AdvantageKit logs would never pick up the new fields. The ledger gains `logs.meta_version`. Derive re-extracts metadata from the raw store only for wpilogs below the current version that have no FMS match number or no UTC end. On Ignite's lake that is the non-FMS and practice logs, a handful. This is one metadata-subset read per log. If the raw file is missing, or fails to read, derive keeps the old metadata, logs one warning, and carries on. It never re-tries that log every derive, because `meta_version` is set either way.

Alternative: tell users to re-ingest with a flag. Rejected: they won't know they need to.

### 4. Temperature status, classified once in derive
Gold features gain `temp_status`: `logged`, `needs-pro`, `not-tracked` or `missing`.

- Derive knows each unit's model and hoot bus, and the ledger's `pro_licensed` for that session's hoot.
- **Not tracked:** models in the new `NO_TEMPERATURE_MODELS` are `not-tracked`. It starts with the two checked on real hoots:
  - CANcoder, which has no temperature signal;
  - Pigeon 2, whose `Temperature` signal Flashpoint doesn't export or map.

  CANrange, CANdi and CANdle go in only after task 6.2 checks an `owlet --scan` or CTRE's signal list. Until then they default to temperature-capable. The label says "not tracked", not "no sensor", because the Pigeon 2 has a sensor. Mapping Pigeon 2 `Temperature` is a follow-up issue, not M2.
- **Needs Pro:** a temperature-capable model with no value on a non-Pro hoot is `needs-pro`.
- **Missing:** otherwise `missing`.

Consumers:
- **History** health reads `temp_status` with the range's temperature maximum. N/A, NEEDS PRO and NO DATA rank below OK, then anomalies raise them (spec).
- **Detection** already skips missing values. It counts `needs-pro` skips separately in its summary.
- **Replay** reads silver and the ledger, not gold, so it applies the same rule per bus through a shared helper, `semantics/temperature.py`. Its payload gains `temperature.missing_by_bus` and a per-slot `temp_reason`.

Alternative: work it out in each view from the ledger. Rejected: three copies of one rule drift, and History would need a ledger join per refresh.

**Fault_DeviceTemp** is in the free set and could give non-Pro teams an over-temperature marker. It is out of scope (Non-goals) and gets a follow-up issue.

### 5. Non-Pro conversion
`select_signals` picks from the signals the hoot lists. We don't know whether owlet lists Pro-only signals for a non-Pro hoot, or fails exporting them. Conversion therefore retries once without `PRO_ONLY_SIGNALS` (`DeviceTemp`, `ProcessorTemp`, `AncillaryDeviceTemp`) when owlet fails on a hoot whose `--check-pro` is false. It records them in `hoot_logs.dropped_signals`. A fake owlet covers both cases. A real non-Pro hoot settles which one happens (task 8.2), and the unused path stays as a guard.

### 6. Brownout cap (refines D10)
In `spec_rules`, a brownout row whose `proxy` is the median fallback gets MED, whatever the count. The why text says the fallback can read wiring drops as brownouts, and that declaring `battery_proxy` fixes it. The thresholds hash and brownout cache are unchanged, because the counts are unchanged. Anomaly ids are stable, because severity isn't part of the id.

Alternative: hide fallback brownouts below normal sensitivity. Rejected: a real brownout on an undeclared robot should still show.

### 7. Rate-relative CAN-gap floors (refines D10)
- **Derive.** CAN-health device rows gain `frame_rate_hz` = 1e6 / `nominal_us`, already computed in `can_health.py`. This costs nothing to compute, but it is a new gold column, so `DERIVE_VERSION` +1.
- **Detection.** The floor is the configured floor × max(1, rate ÷ `CAN_FLOOR_REFERENCE_HZ`), with the reference fixed at 50 Hz. The rate is the median rate over the baseline window plus the judged match. It applies wherever a floor is used: `detect.py` `_rules()`, `siblings.py` (`floor = cfg.baseline.floors[metric]`) and `model.py` (`judge(..., cfg.baseline.floors[metric], ...)`).
- **Reference rate: 50 Hz, and rio floors rise too.**
  - Ignite's measured rates (Context) put the rio at 100–111 Hz, not below 50 Hz, so a 50 Hz reference doubles the rio floors as well (×2.0–2.2, and ×2.1 for CANivore CANcoders).
  - A reference high enough to leave the rio alone (≥ 111 Hz) would scale 250 Hz devices by only ×2.25. That would leave CANivore jitter at |z| up to about 3.8, still a normal-sensitivity DRIFT (2.5), so #77's acceptance would fail.
  - 50 Hz is chosen because it meets #77, and it's the rate the shipped floors were reasoned about. The cost is that weak rio CAN-gap trends get quieter. Ignite's real rio stalls are at |z| 17–93 and stay HIGH. The two rio DRIFT findings at normal sensitivity (|z| ≈ 3.9) fall to about 1.8 and show at strict only. Task 4.4 lists every rio finding that moves, and Josh decides (Open questions).
  - This is a constant, not a config key: one fewer knob for new teams. A team can still raise `floors` in `anomaly.toml`.
- **Expected effect on a 250 Hz device:** floors become 12.5 gaps and 5 missed/min. The jitter case (9 gaps, 3.7/min) is z < 1. Ignite's 41 CANivore findings scale from |z| ≤ 8.5 to about ≤ 1.7, which is below every normal threshold. Only an occasional strict DRIFT (1.5) remains possible, and task 4.4 counts it. A 1 s dropout per match is ~100 missed/min, z ≈ 20, so still a HIGH STEP. The SPEC gap limit (250 ms / 1 s) is untouched, so a single real dropout is still caught.
- **Alternatives:**
  - A minimum gap length in derive (ignore gaps under ~25 ms). Rejected: it changes the measurement, the Replay markers and the can-health spec. Ignite's starved-rio stalls of 20–50 ms are real.
  - A "% frames missed" metric. Rejected: it means a new metric, History mapping and display, for the same effect.
  - Higher fixed floors. Rejected: they blind slow (4 Hz) devices, where one missed frame matters.

### 8. Pro coverage check
A new entry in the `setup/checks.py` registry runs one ledger query over `hoot_logs` grouped by bus and `pro_licensed`. It is a warning, never a problem, and has no fix. It is ADDED as its own requirement, so it doesn't collide with `mvp-install-and-first-run`'s edits to "Setup checks".

### 9. Beta label and guide
- `anomalies.js` adds a BETA tag and a note to the header. Its link comes from the server's `GET /api/docs` map, and the Pro check's link (§8) from `docs_url()` in `flashpoint/docs_links.py`; `mvp-install-and-first-run` (D-b, #72, its tasks 2.3–2.4, due 12-05) adds both. The shipped JS holds no address (static lint, ADR-0013), and `mvp-public-release` forbids `blob/development` links, so there is no interim constant: if #72 hasn't merged, the note names the page by title and the link is added when it lands.
- `mvp-documentation` owns the pages. This change supplies their content, and its tests check the anchors (task 7.1):
  - the Anomalies beta note and the "make findings trustworthy" text go on `reference/views/anomalies.md`;
  - `battery_proxy` and `[[siblings]]` go on `reference/config/robots.md`;
  - the non-Pro wording goes on `robot-side/pro-and-temperature.md`;
  - the AdvantageKit wording goes on `robot-side/advantagekit.md`.
- The support-matrix rows for non-Pro and AdvantageKit belong to `mvp-documentation` (#84, `docs/guide/support-matrix.toml`). This change supplies their cell values and evidence (task 7.3).

### 10. Outside logs: handling and tests
- **Storage.** Logs live outside the repo (`$FLASHPOINT_OUTSIDE_LOGS`, default `~/flashpoint-outside-logs/<team>/`), with a private `expect.toml` per team: match keys, alignment method and floor, and the Pro flag. They never go into the corpus, issues, CI or screenshots.
- **Permission.** Written permission is kept with them. A team may ask for its logs to be deleted.
- **Tests.** A new pytest marker `outside`, registered for strict markers, is skipped when the folder is absent. The committed tests read their expectations from `expect.toml`, so no team data is committed.
- **Findings** are filed as issues without log excerpts or team numbers unless the team agrees.

## Budgets

The reference laptop, as in the other specs. Measured by task 9.1 and written here:

| Step | Budget | Effect of this change |
|---|---|---|
| Ingest Q7 + derive + detect | 30 s / 1 GB (5.6 perf test) | Metadata reads 6 more entry names: < 0.1 s |
| Derive, CI perf (~3 s; 3.2 s seen) | unchanged | Identity fallback O(sessions); frame rate already computed; `temp_status` needs one ledger lookup per session: < 0.2 s on Q7 |
| CAN-health step | +3 s / 150 MB per match | One extra column, no new pass |
| One-off metadata re-extract (§3) | ≤ 2 s per affected log, once | Only logs with no FMS number or no UTC end |
| Detect season, CI perf (~5 s; 5.3 s seen) | 5 s / 400 MB | Floor scaling is a vector op per unit-metric: < 0.1 s |
| History refresh | < 2 s | Reads `temp_status` from gold; no ledger join |
| Replay build per match | 5 s / 500 MB | Per-bus reason: one ledger read |
| Setup checks total | < 2 s | One grouped ledger query: < 50 ms |

AdvantageKit wpilogs are often larger than ours, because they log every input at 50 Hz. Their ingest time is recorded in task 8.3 as an observation, not a gate. A real log over the Q7 budget becomes a filed issue.

## If outside logs arrive vs. not

| | Logs by 2026-11-28 | No logs |
|---|---|---|
| AdvantageKit names (§1, §2) | Confirm or correct the constants. Add the real log to `outside` tests | Ship the names as designed. The matrix says "AdvantageKit: identity from hoot names; not tested on real logs" |
| Non-Pro conversion (§5) | Settle which owlet path happens. Real-hoot scenario passes | The fake-owlet tests only. Try a bench hoot from an unlicensed Talon FX on Ignite's rio bus (task 8.2) |
| Anomaly volume | Count default-view findings per outside robot. No HIGH brownout or CAN-gap jitter findings expected | Recount on Ignite's lake only |
| Beta (2026-12-14) | Outside mentor uses their own logs | Outside mentor uses corpus-v1 and the hosted demo |

## Risks / Trade-offs

- [AdvantageKit names guessed wrong] → They sit in one constant block, and spec scenarios use synthetic logs built from that block. A real log in task 8.3 is the check. If none arrives, the matrix says "not tested".
- [Hoot label wrong after a mid-match logger restart] → It is used only when the wpilog has nothing. The earliest group wins, and a conflict is warned.
- [Rate scaling hides a real slow degradation on a 250 Hz device] → The SPEC gap limit still catches any gap ≥ 250 ms. A 1 s/match dropout still makes a HIGH STEP. The scaling is shown in the why text, so a mentor can see it.
- [The 50 Hz reference quiets weak rio CAN-gap trends (rio runs at 100–111 Hz)] → Real rio stalls (|z| 17–93) stay HIGH. Task 4.4 recounts on the Ignite lake and lists every rio finding that moves severity or sensitivity, for Josh to accept or to move the reference.
- [N/A list misses a model] → An unknown model defaults to temperature-capable, so it reads NEEDS PRO or NO DATA rather than hiding a real problem. A model goes on the list only after a scan shows Flashpoint tracks no temperature for it.
- [Health ranks change for Ignite] → The four CANcoders and the Pigeon 2 move from NO DATA to N/A. There is no other change on Pro lakes.
- [Conflicts with sibling MVP changes] → `team-setup` and `match-reports` are ADDED-only here. `project-documentation` has no delta. Page content goes onto `mvp-documentation`'s pages (§9).
- [Two Replay rules on missing temperature] → The main `Replay view` requirement already says non-Pro cells read "not logged" and that the header says why. The ADDED per-bus requirement narrows it without contradicting it: "not logged (needs Phoenix Pro)" still contains "not logged". At archive, fold the old sentence into the new requirement.

## Migration Plan

- `DERIVE_VERSION` 5 → 6, for the gold `temp_status` and CAN-health `frame_rate_hz` columns and the identity sources. That means one re-derive on upgrade, which is automatic.
- `REPORT_VERSION` 4 → 5, for the per-bus temperature reasons: one Replay rebuild.
- The ledger gains `logs.fms_origin`, `logs.meta_version` and `hoot_logs.dropped_signals` through `_migrate()` (`ALTER TABLE … ADD COLUMN`, defaulting to NULL). Each column gets its own `PRAGMA table_info` check, because today's `_migrate()` returns early once `files.attempts` exists.
- Detection re-runs after derive as usual.
- **Rollback:** reinstall the previous version. Its lower `DERIVE_VERSION` re-derives, and the extra ledger columns are ignored.

## Open Questions

- AdvantageKit's exact entry and filename names (§1, §2), to settle on the first real log. They don't change the specs.
- Should the `/DriverStation/Enabled` enable-edge alias come into M2 if a real AdvantageKit log arrives by 11-28? It is about S and lifts AdvantageKit alignment from very low to low. The brief says post-MVP, so it is left out of the tasks.
- Whether owlet fails or skips Pro-only signals on a non-Pro hoot (§5). Both paths are built.
- **Rio floors (§7).** A 50 Hz reference also doubles the floors on Ignite's 100–111 Hz rio devices. Real rio stalls stay HIGH, but weak rio CAN-gap drifts move from normal to strict. Is that acceptable, or should rio-class rates (≤ 125 Hz) keep today's floors, with only faster devices scaling? The second option needs a piecewise rule and a re-check of the CANivore jitter count in 4.4.
- **Pro check level (§8).** A non-Pro team, the common case, would see a permanent `doctor` warning it can't fix without buying Pro. That is the same trap the M1 change removed for backup. Should the check be ok with an informational message instead?
- **AdvantageKit simulation fallback (task 2.1).** AdvantageKit's template writes no wpilog in SIM mode unless a `WPILOGWriter` is added. A sim filename uses the laptop's time zone, not UTC. So a sim log confirms entry names, but not the filename time zone or the `SystemStats` epoch values.
