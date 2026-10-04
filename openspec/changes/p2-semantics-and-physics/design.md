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
| Restarts | E10 has **one** wpilog but **two** hoot groups (`19-26-19`, `19-29-15`): the hoot logger restarted mid-match |

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
- **Seed:** `2026-comp.toml` comes from `Robot-2026/motor-ids.txt` plus the Q7 `ConnectedMotor` scan. TalonFX-10 gets `role = "TBD"` and a TODO; the robot team fills it in.
- **Migration:** `tools/migrate-legacy-config.py` converts the 2025 device CSVs into `2025-comp.toml`, fixes `Pheonix6`, and reports NT-only rows as unmapped (non-goal above).

### Match identity
- The key format is `<season><event lowercase>_<pm|qm|e><n>[r<replay>]`.
- FMS `MatchType`: 1 → `pm`, 2 → `qm`, 3 → `e`.
- The filename fallback uses the P1 parser. TBA enrichment is behind an optional `FLASHPOINT_TBA_KEY` and is never required.

### Sessions and clock alignment
- **Grouping:**
  - Hoots group by filename session stamp.
  - A hoot group joins the wpilog whose anchored UTC span contains its stamp within ±120 s, with the same event and match prefix when both have one.
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

- **Payload match needs shared telemetry.** Robots that don't publish the same struct to both logs get low-confidence alignment. Mitigation: the fallbacks, plus a recommendation to log the pose to both (Robot-2026 and Phoenix-2026 already do through CTRE `Telemetry.java`).
- **One robot per project per season** until the inventory logger is deployed: practice-bot logs would map onto the competition robot. Inventory serial sets resolve this later.
- **Unknown slot (TalonFX-10):** mapped as role `TBD`; the robot team must fill it in.
- **Gear ratios aren't in any log.** They're optional, and only needed for mechanism-level features (not in P2).
