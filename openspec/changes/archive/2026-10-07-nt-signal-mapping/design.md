## Context

- **Bronze already has the data.** Ingest writes every wpilog entry to bronze unfiltered, with columns `signal, type, ts_us, v_f64, v_i64, v_bool, v_str, v_bytes`. Nothing new has to be read from raw files.
- **Silver is slot-keyed.** `semantics/silver.py` only reads hoot logs and `Phoenix6/%` signals. Derive reads only a few wpilog signals (`DS:*`, FMS, time anchors).
- **Robot config is strict.** `semantics/robot_config.py` uses frozen pydantic models with `extra="forbid"`, and the derive fingerprint includes the robot configs. There is no season config yet; `config/seasons/` was planned in 05 but never built.
- **The corpus has both seasons.** It includes 2025 wpilogs (`2025-gadal-q30`, `2025-nofms`) and 2026 wpilogs (`2026-gacmp-q7`, `-e10`, and the wpilog-only `-p2`).
- **The legacy map is out of date.** Matched against `2025-gadal-q30`, all 36 vision rows match, but only 6 of the 24 metrics rows do.

Motivation and scope: see proposal.md. Requirements: see `specs/`.

## Goals / Non-Goals

**Goals:**
- Declaring a new signal is a one-line TOML edit, and needs no code change.
- Slot consumers (gold, Replay, History) stay byte-for-byte unchanged.
- Signals add little to the ingest budget (see Budgets).

**Non-Goals:**
- A generic NT schema browser, or auto-discovery of entries into config.
- Struct, protobuf, or array decoding.

## Decisions

### 1. A separate silver dataset, not new columns on `silver/samples`
Signal samples go to `silver/signals/season=<s>/session_id=<id>/part-N.parquet`, with columns `session_id, signal_id, subsystem, component, metric, t_us, match_time_us, phase, value`.
- *Alternative:* add a nullable `slot_id` plus a `signal_id` to `silver/samples`. Rejected: every slot consumer (gold's stator-current filter, Replay device lists, History) would have to learn to skip slotless rows, and that touches code that P4 just signed off.
- Keeping it separate follows D1 (Parquet + DuckDB, store long) and the domain rule to store data long and as-of join later. P5 can join signals and slots on match time with a tolerance.

### 2. Season roots in `config/seasons/<year>.toml`; signals in the robot file
- **Roots are per season.** They change when the code base or the PhotonVision layout changes, which is the gap behind #66.
- **Signals are per robot,** because they come from that robot's code. Example:

  ```toml
  [[signal]]
  id = "intake-at-extension-setpoint"
  root = "robot"
  entry = "Intake/At Extension Setpoint"
  subsystem = "intake"
  metric = "at_setpoint"
  ```

- **Roots are spelled out in full** (`NT:Robot/m_robotContainer/` and `NT:/photonvision/`), because the two forms really differ in the logs. Matching is exact string concatenation, with no normalization. This avoids #10-style prefix bugs.
- **A robot with no signals needs no season file.** Existing configs keep loading, and the season file is only required once signals are declared.
- *Alternative:* full entry names in the robot file, with no season file. Rejected: every entry would repeat the root, and the roadmap gate needs `config2025.json` covered.

### 3. Labels: subsystem, optional component, metric
- **Legacy metrics rows:** `assembly/subassembly/component` collapse into one slugged `component` (as the migration already does for slot ids).
- **Vision rows:** `subsystem = "vision"` and `component = <camera>`. An empty legacy `metric` becomes the snake-cased key (for example `targetPitch` becomes `target_pitch`).
- The metric is free text, validated as snake_case. No enum: P5 will tell us which metrics matter.

### 4. Derive: one DuckDB query per session over bronze
For each session whose robot declares signals, derive runs one query:
- **Read:** bronze rows for the session's wpilog, filtered to the declared names (`signal IN (...)`).
- **Join:** the declared-signals table, plus an as-of join to the session's framing for match time and phase. Framing is the same source silver uses.
- **Match framing:** match time and phase are set only for a match session, meaning it has a match identity and a match start. A practice session such as `2025-nofms` is DS-framed but has no match key, so both stay empty. (Slot silver keeps its own phases.)
- **Value:** `COALESCE(v_f64, v_i64, v_bool::int)`. Rows of any other type are excluded and the signal is reported as `non-numeric`.
- **Missing signals:** found as declared names absent from the session's distinct signals in bronze. They are written to a new ledger table, `missing_signals(session_id, signal_id, reason)`, which is exposed as a meta table.
- **Staging:** writes go through the same staging-then-rename path as silver (atomic visibility).
- This follows D2 (Polars + DuckDB SQL). Signal declarations, and their season roots, join the derive fingerprint, so editing either re-derives only the affected sessions.

### 5. Migration checks against a reference log
`tools/migrate-legacy-config.py --reference-log <wpilog>` loads the log's entry names and types through the existing wpilog reader (D3), then puts each legacy row through these checks in order:
1. A motor voltage, current, temperature, position, or velocity row of a subsystem that has CAN slots (the slot's Phoenix 6 signals carry it) becomes *superseded by CAN slot `<id>`*.
2. An entry absent from the log becomes *not in reference log*.
3. A non-numeric entry becomes *non-numeric in reference log*.
4. Anything else is mapped.

The report is printed and also written into the head comment of `2025-comp.toml`, so the record survives once the legacy files are deleted. The tool keeps reading legacy files from the `legacy-2025` tag. This is a one-time tool; once `datamaps/` is gone it only runs against the tag.

### 6. The 2026 mapping is curated, not exhaustive
- `2026-comp.toml` declares the numeric and boolean entries a pit crew or P5 would act on:
  - the Epilogue motor `Value` objects for Intake, Shooter, Indexer, and Hunter;
  - setpoints and at-setpoint flags;
  - drivetrain PID errors;
  - per camera, `hasTarget`, `latencyMillis`, `fps`, and the target yaw, pitch, and area.
- The candidate list is generated from corpus Q7, and Josh signs off on it before it is merged (task 5.2).

No D1–D11 decision is changed. D5's "robot-side NT logging (fallback only)" stays as it is: temperature still comes from hoots.

## Budgets
- **Time:** signals add at most **2 s** to the Q7 ingest (today 15.5 s locally and 25.1 s on CI, against a 30 s budget). That is one filtered DuckDB scan of the wpilog's bronze partition, a few hundred thousand rows.
- **Memory:** DuckDB streams the query, so peak RSS rises by at most **150 MB** (budget 1 GB).
- `tests/test_ingest_perf.py` is extended to assert both, and silver signals stay inside the existing Q7 budget test.

## Risks / Trade-offs
- **[Robot code renames entries mid-season, and signals vanish]** → The `missing_signals` table plus a derive warning; the Import status view will surface it.
- **[High-rate entries (e.g. 50 Hz PID errors) bloat silver]** → Signals are opt-in per entry. The Q7 partition size is measured in task 6.2 and a size check is added to the perf test.
- **[Curated 2026 list misses something P5 wants]** → Adding it later is a TOML line plus `derive`.
- **[The 2025 metrics map has only 6 live rows]** → That is accepted (decided 2026-10-06). The stale rows are recorded with reasons rather than silently dropped.

## Migration Plan
1. Ship the code with an empty `[[signal]]` list. Behavior is unchanged.
2. Add `config/seasons/2025.toml`, `config/seasons/2026.toml`, and the signals, then run `flashpoint derive --all` on existing lakes. It is rebuildable from bronze, with no re-ingest.
3. **Rollback:** delete the `[[signal]]` tables and re-derive. `silver/signals/` partitions for affected sessions are removed by the normal re-derive.
