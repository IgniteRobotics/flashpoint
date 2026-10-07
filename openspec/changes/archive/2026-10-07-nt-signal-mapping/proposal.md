## Why

P2 mapped CAN devices into robot configuration. It deliberately left out signals that exist only in NetworkTables: subsystem telemetry that the robot code publishes (setpoints, at-setpoint flags, beam breaks, servo positions) and PhotonVision. They sit unlabeled in bronze, but P5's rule-based checks need them. The only description of them is still the legacy `datamaps/` and `log_configs/`, which is the last thing blocking `retire-legacy-code` stage 3b. The legacy 2025 metrics map is also out of date: 18 of its 24 rows name entries that don't exist in a real 2025 log, and none of the 2025 maps match the 2026 robot.

Roadmap: **NetworkTables signal mapping** (`docs/rewrite/06-roadmap.md`, follow-up to P2). It implements the `config/seasons/` layout planned in `05-target-architecture.md` (principle 4, "Configuration is data"). Pitfalls: #10 (CameraPublisher entries never selected), #15 (year and vision map hard-coded), #66 (a season bump means hand-editing JSON and CSVs).

## What Changes

- **Season configuration:** a new `config/seasons/<year>.toml` names the NetworkTables roots for that season (robot telemetry, PhotonVision, CameraPublisher, Preferences, FMS). Both NT spellings (`NT:Robot/...`, `NT:/photonvision/...`) are written out, never guessed.
- **Signal declarations in robot configuration:** `[[signal]]` tables give an NT entry (relative to a season root) a stable id plus subsystem, component, and metric labels.
- **Silver signal samples:** derive writes mapped NT samples to a new silver dataset (`silver/signals/`). Each sample carries session, signal id, labels, wpilog time, match time, phase, and numeric value (booleans become 0/1). Slot-keyed `silver/samples/` is unchanged, so gold, Replay, and History are untouched.
- **Missing signals reported:** for each session, declared signals that are absent from its wpilog are listed in the ledger. A renamed entry in robot code shows up instead of quietly vanishing.
- **2025 migration:** `tools/migrate-legacy-config.py` also emits `config/seasons/2025.toml` and `[[signal]]` tables for `2025-comp`. Every legacy row ends up either mapped or listed with a reason:
  - the 18 motor rows (17 voltage, current, and temperature rows plus the wrist motor position) are listed as *superseded by CAN slot `<id>`*;
  - non-numeric vision keys (pose and raw-bytes values) are listed as *non-numeric, stays in bronze*.
- **2026 mapping:** a hand-written `config/seasons/2026.toml` and `[[signal]]` tables in `2026-comp.toml` for the current robot. They cover the Intake, Shooter, Indexer, Hunter, and Drivetrain telemetry plus the three PhotonVision cameras, chosen from the entries in corpus `2026-gacmp-q7`.

## Capabilities

### New Capabilities
- `nt-signals`: mapping NetworkTables entries to labeled signals: matching entries through season roots, writing silver signal samples, and reporting missing declared signals.

### Modified Capabilities
- `robot-config`: adds a season configuration file requirement; the robot configuration file can also declare signals; legacy migration covers the NT maps and log configs, with a reason for every row it doesn't map.
- `telemetry-lake`: the silver layer also holds signal samples, and season configs are part of what a rebuild depends on.

## Impact

- **Code:**
  - `semantics/robot_config.py`: a `Signal` model and a season loader;
  - `semantics/derive.py` and a new `semantics/signals.py`: the silver writer, missing-signal report, and derive fingerprint;
  - `semantics/legacy_migration.py` and `tools/migrate-legacy-config.py`;
  - a new ledger table for missing signals.
- **Config:** new `config/seasons/2025.toml` and `config/seasons/2026.toml`, and `[[signal]]` tables in both robot files.
- **Data:** new `silver/signals/` partitions. Existing lakes pick them up on the next `flashpoint derive --all`.
- **Unblocks:** `retire-legacy-code` 3b (delete `datamaps/` and `log_configs/`).

## Non-goals

- Showing signals in Replay or History, or adding them to gold features. Those come with Replay UX or P5.
- 2024. There is no 2024 robot config; its maps stay recoverable from the `legacy-2025` tag.
- Mapping every NT entry. Only declared signals reach silver; everything else stays queryable in bronze.
- String, struct, and array values in silver, and SmartDashboard, Shuffleboard, and PathPlanner entries.
