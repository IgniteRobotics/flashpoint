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

**Checked 2026-11-25 against AdvantageKit v26.0.2's source** (`LoggedDriverStation`, `LoggedSystemStats`, `WPILOGWriter`, `LogTable` on GitHub; no outside log or simulation log yet, task 2.1):
- The root table prefix is `/`, so the entries are `/DriverStation/<field>` and `/SystemStats/<field>`. Confirmed.
- `EventName` is a string (trimmed). `MatchNumber`, `ReplayNumber`, `MatchType` and `AllianceStation` are integers (`int64` in the wpilog). `MatchType` is 0 none, 1 practice, 2 qualification, 3 elimination. `AllianceStation` is the HAL's 1–3 red, 4–6 blue, 0 unknown, so it maps to the alliance and station fields. The names `Practice`/`Qualification`/`Elimination` are still accepted, but v26 doesn't write them.
- The epoch entry is `/SystemStats/EpochTimeMicros` (int64). `/SystemStats/SystemTimeValid` exists too; the >2020 filter does the same job.
- Filename: `akit_` + `yy-MM-dd_HH-mm-ss` (else a random hex id until the DS attaches with a valid clock) + `_<event>` **lower-cased** + `_<p|q|e><n>`. Each part is optional. Extraction upper-cases the event. The time is `LocalDateTime.now()`, the JVM's local zone: UTC on a roboRIO (not confirmed on a real log), the laptop's zone in simulation.
- Not confirmed: real epoch values and the UTC filename rule (both need a real log), and the on-disk types (the source says int64; a sim or real log would show it).
- **Correction to task 2.4's check:** on the corpus, only `2025-nofms` is read again. `2026-gacmp-p2` is an FMS practice match with a match number and a UTC end, so the rule skips it.

**MatchType:** accept 1/2/3 and the names `Practice`/`Qualification`/`Elimination` (case-insensitive). Anything else is absent.

### 2. AdvantageKit wall-clock anchor
`_anchor()` tries `systemTime`, then the AdvantageKit epoch entry, then the filename. It uses the same >2020 plausibility filter and the median offset. With an offset, `utc_end` exists, and `group_sessions()` pairs the log with its hoots unchanged.

Alignment then falls to `wall-clock` (very low). AdvantageKit's CTRE swerve template doesn't publish `DriveState/Pose` to NetworkTables, and enable edges read `DS:enabled`. Framing still works, because it prefers hoot `RobotMode` (`framing_for`). Replay shows the existing low-alignment warning. That is honest for M2. The `/DriverStation/Enabled` alias for enable edges (size S) comes into M2 only if a real AdvantageKit log arrives by 2026-11-28 (Josh, 2026-11-15): it lifts AdvantageKit alignment from very low to low. The `session-grouping` delta carries it; task 2.5 drops that delta if no real log arrives, and the alias goes to a post-MVP issue.

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

**Applied 2026-11-25 (task 6.2):**
- `NO_TEMPERATURE_MODELS` is CANcoder and Pigeon2 only. No Ignite or corpus hoot has a CANrange, CANdi or CANdle, so there was nothing to `owlet --scan`, and CTRE's signal list was not checked. They stay temperature-capable: an outside log with one reads NEEDS PRO or NO DATA until a scan says otherwise.
- `temp_status` is written to `match_features` (motors) **and** `can_health` (every mapped device). Sensors never get a `match_features` row, because gold features need stator current. History lists the gyro and CANcoders only on CAN metrics, so it reads their status from `can_health`. A `can_health` row takes its motor's status from the session's gold row. A sensor's status comes from its model and its bus's Pro flag.
- A slot's Pro flag is its bus's (`pro_by_bus` over the session's hoots): Pro if any hoot on that bus is, non-Pro if every known one isn't, unknown otherwise. Unknown reads as `missing`, never `needs-pro`.

Alternative: work it out in each view from the ledger. Rejected: three copies of one rule drift, and History would need a ledger join per refresh.

**Fault_DeviceTemp** is in the free set and could give non-Pro teams an over-temperature marker. It is out of scope (Non-goals) and gets a follow-up issue.

### 5. Non-Pro conversion
`select_signals` picks from the signals the hoot lists. We don't know whether owlet lists Pro-only signals for a non-Pro hoot, or fails exporting them. Conversion therefore retries once without `PRO_ONLY_SIGNALS` (`DeviceTemp`, `ProcessorTemp`, `AncillaryDeviceTemp`) when owlet fails on a hoot whose `--check-pro` is false. It records them in `hoot_logs.dropped_signals`. A fake owlet covers both cases. A real non-Pro hoot settles which one happens (task 8.2), and the unused path stays as a guard.

### 6. Brownout cap (refines D10)
In `spec_rules`, a brownout row whose `proxy` is the median fallback gets MED, whatever the count. The why text says the fallback can read wiring drops as brownouts, and that declaring `battery_proxy` fixes it. The thresholds hash and brownout cache are unchanged, because the counts are unchanged. Anomaly ids are stable, because severity isn't part of the id.

Alternative: hide fallback brownouts below normal sensitivity. Rejected: a real brownout on an undeclared robot should still show.

### 7. Rate-relative CAN-gap floors (refines D10)
- **Derive.** CAN-health device rows gain `frame_rate_hz` = 1e6 / `nominal_us`, already computed in `can_health.py`. This costs nothing to compute, but it is a new gold column, so `DERIVE_VERSION` +1.
- **Detection.** The floor is the configured floor when the rate is at most `CAN_FLOOR_RIO_MAX_HZ` (125 Hz), and the configured floor × rate ÷ `CAN_FLOOR_REFERENCE_HZ` (50 Hz) above it. The rate is the median rate over the baseline window plus the judged match. It applies wherever a floor is used: `detect.py` `_rules()`, `siblings.py` (`floor = cfg.baseline.floors[metric]`) and `model.py` (`judge(..., cfg.baseline.floors[metric], ...)`).
- **Piecewise rule: rio rates keep today's floors (Josh, 2026-11-15).**
  - Ignite's measured rates (Context) put the rio at 100–111 Hz and CANivore CANcoders at about 105 Hz. A plain ×max(1, rate ÷ 50) would double their floors and move the two rio DRIFT findings at normal sensitivity (|z| ≈ 3.9) to strict. Josh chose to keep today's behaviour there.
  - Above 125 Hz the scale is rate ÷ 50, so a 250 Hz CANivore device gets ×5, which #77 needs. A single higher reference (≥ 111 Hz) would scale 250 Hz by only ×2.25 and leave CANivore jitter at |z| up to about 3.8, a normal-sensitivity DRIFT.
  - The step at 125 Hz (×1 to ×2.5) is deliberate. Phoenix status frames sit at the default rates (rio ~100 Hz, CANivore 250 Hz), so few devices land near it. A team that configures a device just above 125 Hz gets the scaled floors, and the why text states the rate and floor so a mentor can see it.
  - This is a constant, not a config key: one fewer knob for new teams. A team can still raise `floors` in `anomaly.toml`.
- **Expected effect on rio-rate devices (≤ 125 Hz):** none; floors and findings are unchanged, including the CANivore CANcoders at ~105 Hz. Task 4.4 checks that their jitter doesn't add findings.
- **Expected effect on a 250 Hz device:** floors become 12.5 gaps and 5 missed/min. The jitter case (9 gaps, 3.7/min) is z < 1. Ignite's 41 CANivore findings scale from |z| ≤ 8.5 to about ≤ 1.7, which is below every normal threshold. Only an occasional strict DRIFT (1.5) remains possible, and task 4.4 counts it. A 1 s dropout per match is ~100 missed/min, z ≈ 20, so still a HIGH STEP. The SPEC gap limit (250 ms / 1 s) is untouched, so a single real dropout is still caught.
- **Corpus Q7 rates, checked 2026-11-25 (task 4.1):** CANivore Talon FX 250.4 Hz, Pigeon 2 251.2 Hz, CANcoders 100.8–104.9 Hz; rio Talon FX 100.0–111.1 Hz. This matches the lake review above. Q7's isolated-gap counts per slot are unchanged by recording the rate. The truncated E10 copy (`2026-gacmp-e10-truncated`) ends before the robot is enabled, so its devices have no nominal period and an empty rate, as "No frames while enabled" says.
- **Ignite lake recount, 2026-11-25 (task 4.4).** Run on an APFS clone of `~/flashpoint-lake` (74 sessions, 38 matches), with the repo `config/`. "Before" is `development` (3892985); "after" is this branch (a full re-derive for `DERIVE_VERSION` 7, then detect). The lake itself was not touched.

  | CAN-gap anomalies | HIGH | MED | LOW | Total |
  |---|---|---|---|---|
  | CANivore, before | 4 | 12 | 25 | 41 |
  | CANivore, after | 0 | 0 | 1 | 1 |
  | rio, before | 11 | 2 | 12 | 25 |
  | rio, after | 11 | 2 | 12 | 25 |

  - Before, the CANivore findings were DRIFT (2 normal, 13 relaxed, 8 strict), SIBLING (16 strict) and STEP (2 strict). After, one strict LOW DRIFT remains, on the gyro's frames missed (onset `2026gacmp_qm46`). It is the "occasional strict DRIFT" expected above.
  - No CANivore Talon FX or Pigeon 2 DRIFT, STEP or SIBLING remains at normal or relaxed sensitivity.
  - Every rio CAN-gap finding is identical: same id, method, severity, sensitivity and onset.
  - CANivore CANcoders: 0 findings before and after.
  - The baseline was 66 CAN-gap anomalies, not 65: the extra one is a rio SPEC finding on the longest gap. Anomalies overall went from 289 to 249.
  - Devices between 110 and 150 Hz: the ten rio Talon FX slots at 111.1 Hz (flywheels, hood, indexers, intake). All are below 125 Hz, so they are unscaled. No CANivore device is in that range.
- **Versions:** `DERIVE_VERSION` was already 6 when this change was applied (silver-device-enable took 5 → 6), so this change takes it to 7.
- **Alternatives:**
  - A minimum gap length in derive (ignore gaps under ~25 ms). Rejected: it changes the measurement, the Replay markers and the can-health spec. Ignite's starved-rio stalls of 20–50 ms are real.
  - A "% frames missed" metric. Rejected: it means a new metric, History mapping and display, for the same effect.
  - Higher fixed floors. Rejected: they blind slow (4 Hz) devices, where one missed frame matters.

### 8. Pro coverage check
A new entry in the `setup/checks.py` registry runs one ledger query over `hoot_logs` grouped by bus and `pro_licensed`. It is always ok, with an informational message: a non-Pro team, the common case, can't clear it without buying Pro, so a warning would be the same permanent-warning trap the M1 change removes for backup (Josh, 2026-11-15). It has no fix. It is ADDED as its own requirement, so it doesn't collide with `mvp-install-and-first-run`'s edits to "Setup checks".

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

**Measured 2026-11-25 (task 9.1)**, `pytest -m perf -s` on Josh's development MacBook, on the branch head. Every budget held, and the run exited 0:

| Step | Measured | Budget |
|---|---|---|
| Ingest Q7 + derive + detect | 19.1 s, 816 MB (children 695 MB) | 30 s / 1 GB |
| Detect season | 1.83 s, 233 MB | 5 s / 400 MB |
| Backtest | 3.58 s, 267 MB | (P5 budget) |
| CAN-health step | 0.38 s, 71 MB | +3 s / 150 MB |
| Replay build (Q7) | 0.62 s, 410 MB | 5 s / 500 MB |
| Usage step | 1.13 s, 677 MB | (P2 budget) |
| History refresh (season) | under 2 s (`test_season_queries_under_two_seconds`) | < 2 s |
| Setup checks (corpus lake) | under 2 s (`test_checks_are_fast_on_the_corpus_lake`) | < 2 s |

The CI perf job's numbers (a slower runner) are its own; this table is the laptop.

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
- [Devices configured just above 125 Hz jump from ×1 to ×2.5] → Rare at default Phoenix rates. The why text shows the rate and floor. Task 4.4 lists any device between 110 and 150 Hz on the Ignite lake.
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

Resolved by Josh on 2026-11-15: the Pro coverage check is ok with an informational message (§8); rio-rate devices keep today's CAN-gap floors (§7); an AdvantageKit simulation log is enough to confirm entry names when no real log arrives, with the filename time zone and epoch values recorded "not confirmed" and the matrix saying "not tested on real logs" (task 2.1); the `/DriverStation/Enabled` alias comes into M2 if a real AdvantageKit log arrives by 11-28 (task 2.5).

- AdvantageKit's exact entry and filename names (§1, §2), to settle on the first real log. They don't change the specs.
- Whether owlet fails or skips Pro-only signals on a non-Pro hoot (§5). Both paths are built.
