## Why

Raw signals mean nothing until they are tied to a robot, a match, a role, and a physical unit. Today's mapping uses per-season CSVs full of typos (#11) and hard-coded years (#15, #66). Its match framing is wrong (#13), match IDs are parsed by inconsistent regexes (#20, #21), and the physics on power-tracking has averaging bugs (#23, #26, #27). Roadmap phase **P2**; design in 05 §3, §4, §4a.

## What Changes

- **Robot and season config** (TOML, validated): slots, motor models, gear ratios, declared swap dates. A one-off migration from `datamaps/*.csv` and `utils/motors.toml`
- **Match identity**: in-log FMSInfo, then the filename, then an optional TBA key. One parser, practice-aware
- **Session grouping**: pair a wpilog with its hoots by wall-clock overlap, not by filename substring
- **Device identity**: resolve CANInventory into slot and unit observations, detect mid-session swaps, assign legacy epochs, report unmapped serials
- **Match framing**: auto, teleop, and disabled phases from DS state
- **Motor physics**: time-weighted stats, power, energy, signed velocity, thermal, stall time, DCMotor current residuals. Ported from power-tracking's analyzer and tests
- **Gold features**: one row per (match, slot, unit)

## Capabilities

### New Capabilities
- `robot-config`: season and robot configuration schema, validation, swap declarations
- `match-identity`: canonical match key resolution and its precedence rules
- `session-grouping`: grouping the logs from one robot power-on and match
- `device-identity`: slot/unit model, serial-based unit tracking, legacy epochs
- `match-framing`: match phase detection and trimming
- `motor-physics`: derived electrical, mechanical, and thermal quantities, plus residuals
- `match-features`: the per-match, per-slot, per-unit feature table contract

### Modified Capabilities
- `telemetry-lake`: adds the silver and gold layers and the unit registry tables

## Non-goals

- UI (P4); anomaly scoring (P5)
- REV device serial identity

## Impact

New `src/flashpoint/{config,identity,physics}`, `config/seasons/`, `config/robots/`. Migrates `datamaps/`, `log_configs/`, and `utils/motors.toml`. The legacy files are removed by `retire-legacy-code` stage 2b.
