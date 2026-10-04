## Context

P1 delivered bronze samples, per-log metadata, and a ledger, all in `rewrite`. This change adds meaning: which robot, which match, which phase, which role (slot), which physical motor (unit), and physics features. Decisions: D10 (anomaly v1 relies on these features), D11 (serial identity, accepted).

### Spike results (2026-10-04, corpus Q7 and E10)
| Question | Finding |
|---|---|
| Are hoot and wpilog clocks the same? | **No.** Offset about 19.56 s on Q7 |
| Best alignment anchor | CTRE swerve telemetry writes `DriveState/Pose` (`struct:Pose2d`) to **both** logs. **15,039 / 15,039** wpilog poses match a hoot pose byte-for-byte: offset 19.5635 s, IQR 0.076 ms, drift 0.1 ms |
| Enable edges as anchor | Offsets 19.59, 19.51, 20.09 s: 30–500 ms error. **Fallback only** |
| Do rio and CANivore hoots share a clock? | **Yes.** `RobotEnable` edges are identical to the millisecond |
| Is the motor model known? | **Yes.** `ConnectedMotor` per device (KrakenX60, KrakenX44, Falcon500), plus device constants `MotorKT`, `MotorKV`, `MotorStallCurrent` |
| Temperature available? | **Yes** (Pro). Q7 maximum 46 °C on intake TalonFX-1 |
| 2026 robot layout | rio: TalonFX-1..8 and 10 (intake, indexer, shooter, hood; **10 not in `motor-ids.txt`**). CANivore: 11/12, 21/22, 31/32, 41/42 (drive Kraken X60, steer Falcon 500) |
| Restarts | E10 has **one** wpilog but **two** hoot groups (`19-26-19`, `19-29-15`). *Corrected during implementation:* the wpilog **ends at 19:28:10 UTC**, so the second group (19:29:15) is outside it, probably a reboot whose new wpilog isn't in the corpus. It becomes a hoot-only session. A single wpilog enable edge also mis-paired into a bogus 127.8 s offset (the wall clock says about 196 s), so enable-edge alignment now requires at least 2 pairs, a spread under 1 s, and wall-clock agreement within 10 s |

## Goals / Non-Goals

**Goals:**
- Every Q7-like session: robot, match key, phases, aligned clocks, slots and units, then silver and gold.
- Identity and alignment carry an explicit confidence everywhere downstream.
- Derived layers rebuild from bronze plus configuration (no raw re-read, no network).

**Non-Goals:**
- Mapping NT-only signals (wpilog subsystem telemetry, vision) into slots. Bronze still holds them; a later change can map them.
- Distinguishing practice and competition robots that run identical code. That needs the CAN inventory (serial sets) or a configured hint; the first version assumes one robot per project per season.
- Struct decoding beyond what alignment needs (byte-equality, no field decode).

## Decisions

### Robot configuration (`config/robots/<robot-id>.toml`, validated with pydantic)
```toml
robot = "2026-comp"
season = 2026
project = "Robot-2026"            # matched against the log's MetaData "Project Name"
[[slot]]
id = "drive-fl"                   # stable slot id within the robot
bus = "canivore"                  # "rio" or "canivore" (the CANivore serial matches by session)
model = "TalonFX"
can_id = 11
subsystem = "drivetrain"
role = "FL drive"
gear_ratio = 6.12                 # optional
swaps = ["2026-03-20"]            # optional; legacy unit epochs
```
- `bus = "canivore"` matches any 32-hex CANivore id. A specific id can be pinned later if a robot has two.
- pydantic is a new runtime dependency. It's familiar to students and gives good error messages (D-config).
- **Seed:** `2026-comp.toml` comes from Robot-2026 `development` (`TunerConstants`, subsystem constants) plus the Q7 `ConnectedMotor` scan. 23 slots: 18 motors, plus the Pigeon2 gyro (10) and 4 CANcoders (13/23/33/43), which the first corpus run reported as unmapped. TalonFX-10 is the intake roller follower. TalonFX-14 (intake extension follower) was added after GACMP, so it doesn't appear in the corpus. Swerve gear ratios: drive 6.746, steer 21.43.
- **Migration:** `tools/migrate-legacy-config.py` converts the 2025 device CSVs into `2025-comp.toml`, fixes `Pheonix6`, and reports NT-only rows as unmapped (non-goal above).

### Match identity
- The key format is `<season><event lowercase>_<pm|qm|e><n>[r<replay>]`.
- FMS `MatchType`: 1 → `pm`, 2 → `qm`, 3 → `e`.
- The filename fallback uses the P1 parser. TBA enrichment is behind an optional `FLASHPOINT_TBA_KEY` and is never required.

### Sessions and clock alignment
- **Grouping:**
  - Hoots group by filename session stamp.
  - A hoot group joins the wpilog whose anchored UTC span contains its stamp, allowing it to start up to 60 s before the wpilog. Event and match labels must agree when both have one.
  - Otherwise it forms a hoot-only session.
- **`payload-match` alignment:**
  - Find non-`Phoenix6/` hoot signals whose name equals a wpilog signal's name with its `NT:/` prefix removed, and whose types are equal.
  - Join samples on exact payload bytes (struct) or exact value (double), keeping only values that occur exactly once in each log.
  - The offset is the median of (wpilog ts − hoot ts), and the spread is the IQR.
  - Accept with at least 50 matches and a spread under 5 ms.
- **Fallbacks:** `enable-edges` (`DS:enabled` vs `RobotEnable` transitions, nearest pairing, median), then `wall-clock`.
- **One offset per hoot group:** the spike shows the buses share a clock. The other bus's offset is checked anyway and a disagreement over 1 ms is flagged.

### Match framing
`DS:enabled` and `DS:autonomous` (written by `DriverStation.startDataLog`) give the phases. Match start is the first enable. `gap` is disabled time between auto and teleop.

### Silver
- One row per mapped hoot device sample: `session_id, slot_id, unit_id, metric, t_us, match_time_us, phase, value`.
- The `t_us` column is on the wpilog clock.
- Metrics normalize Phoenix names: `SupplyCurrent → supply_current`, `StatorCurrent → stator_current`, `MotorVoltage → motor_voltage`, `SupplyVoltage → supply_voltage`, `RotorVelocity → rotor_velocity_rps`, `Velocity → velocity`, `Position → position`, `DeviceTemp → temp_c`, `TorqueCurrent → torque_current`.
- Path: `silver/samples/season=/session_id=/part-0.parquet`, written per session with the P1 atomic writer.

### Physics (Polars)
- **Hold durations:** the next sample of the same (slot, metric), capped at 1 s, so log gaps don't inflate weights.
- **Power:** motor and supply power as-of joined within a slot (tolerance 20 ms); energy in Wh.
- **Residual** from device constants: expected stator current I ≈ (V − ω/Kv) · I_stall / 12 V, with ω the rotor speed in the units Kv uses (confirmed from the device signal values at implementation, against WPILib DCMotor for Kraken X60). Absent if the constants are missing.
- **Stall time:** stator current above 0.4 × stall current while |rotor velocity| is below 0.5 rps (configurable).

### Gold
`gold/match_features/season=/part-*.parquet`, one row per (match key, phase in {auto, teleop, match}, slot, unit), with the columns in the spec. It's built with DuckDB SQL over silver plus Polars for time-weighted stats.

### Commands and versions
- `flashpoint ingest` runs `derive` for sessions it touched.
- `flashpoint derive [--all]` rebuilds silver, gold, and identity from bronze plus configuration.
- `PIPELINE_VERSION` 1 → 2, because the health profile changes, so `rebuild` reconverts the hoots from raw.

## Risks / Trade-offs

- **Implementation findings (2026-10-04):**
  - **Framing source:** the aligned hoot's `RobotMode` is preferred. On Q7 the wpilog stops at 213 s while teleop ran to 291.6 s; the DS signals are the fallback.
  - **Memory:**
    - DuckDB is capped at 512 MB and spills to `<lake>/tmp`.
    - Silver isn't globally sorted, because sorting 9.3 M rows pushed derive past 1 GB.
    - Gold reads one motor at a time.
    - Derive runs in a fresh process after ingest. Measured: 870 MB (ingest) and 593 MB (derive), where the combined in-process figure was 1,155 MB.
  - **The residual is only meaningful relative to a baseline.** The DC-motor model ignores stator current limits and FOC torque control, so Q7's residual P95 is about 65 A on drive and 105 A on steer. It's a per-unit trend feature for P5, not an absolute fault threshold.

- **Payload match needs shared telemetry.** Robots that don't publish the same struct to both logs get low-confidence alignment. Mitigation: the fallbacks, plus a recommendation to log the pose to both (Robot-2026 and Phoenix-2026 already do through CTRE `Telemetry.java`).
- **One robot per project per season** until the inventory logger is deployed: practice-bot logs would map onto the competition robot. Inventory serial sets resolve this later.
- **Gear ratios aren't in any log.** They're optional, and only needed for mechanism-level features (not in P2).
