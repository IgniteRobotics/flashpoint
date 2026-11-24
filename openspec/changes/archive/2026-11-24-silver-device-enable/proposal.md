## Why

Phoenix 6 `DeviceEnable` reaches bronze as a string (`v_str` = `Enabled` / `Disabled`). The silver query builds `value` only from `v_f64`, `v_bool`, and `v_i64`, so every `device_enable` row is NULL and gets dropped. Silver therefore has no per-device enable state, even though `METRICS` maps the signal. The real lake today holds about 51 M such rows (13.0 M `Enabled`, 38.0 M `Disabled`), and silver keeps none of them. Found in the P4 spike (`p4-views-and-reports` design.md, then tracked as private #18) and filed as issue #5.

This is a P2 (Semantics & physics) defect in the silver layer described in `docs/rewrite/05-target-architecture.md` §4 (data model) and `06-roadmap.md` P2. It fixes no numbered pitfall from `03-pitfalls-and-performance.md`. It is a correctness gap in our own rewrite, not a legacy defect.

## What Changes

- Silver maps Phoenix 6 `DeviceEnable` strings to numbers: `Enabled` → 1.0, `Disabled` → 0.0. Any other string stays NULL and is dropped, as today.
- Numeric and boolean device values keep their current precedence (`v_f64`, then `v_bool`, then `v_i64`). Only `DeviceEnable` reads `v_str`.
- `DERIVE_VERSION` goes from 5 to 6, so the next `flashpoint derive` rebuilds every session's silver and gold, and existing lakes pick up the metric without `rebuild`.
- A corpus test on GACMP Q7 shows that `device_enable` rows exist for TalonFX slots, take only the values 0 and 1, and are 1 during the enabled match phases.

## Non-goals

- Switching `gold/unit_usage.enabled_s` (or any other gold feature) from robot enable intervals to per-device enable. That changes published numbers and needs its own spec. It becomes possible after this change.
- Showing `device_enable` in Replay, History, or reports. Their metric lists are fixed and stay so.
- Decoding other string-typed hoot signals (`ConnectedMotor`, `Version`, faults) into silver.
- Changing the NT signal path (`semantics/signals.py`), which has a similar numeric-only `COALESCE` but its own spec.
- Changing the hoot export profile or bronze schema.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `telemetry-lake`: adds a requirement next to "Silver and gold layers" that silver keeps device enable state as a 0/1 metric, decoded from the strings Phoenix 6 logs. Unknown strings are dropped, and existing lakes pick the metric up on the next derive.

## Impact

- Code: `src/flashpoint/semantics/silver.py` (value expression in the silver query) and `src/flashpoint/config.py` (`DERIVE_VERSION`).
- Tests: new unit cases in `tests/semantics/test_silver.py` (string-valued `DeviceEnable`, unknown string dropped) and a new Q7 corpus assertion. The existing bool-typed `DeviceEnable` case in `test_silver.py` must still pass.
- Data: silver grows by about 8 % in rows (51 M on the current lake, against about 600 M). The values are long runs of 0 or 1, so zstd keeps the disk cost small. Every lake re-derives once on upgrade.
- Gold: the gold per-motor load (`gold.py`) reads every metric for a unit, so it now also loads `device_enable` rows. No feature reads them. The Q7 ingest budget (30 s, 1 GB, ingest plus derive) has already been overrun in CI (33–40 s seen), so the design measures what this change adds.
- Usage: `usage._powered` builds hold windows from `supply_voltage` timestamps. It falls back to every metric's timestamps only for slots with no `supply_voltage`. TalonFX slots have `supply_voltage`, so `powered_s` and `enabled_s` should not change. The corpus test pins this.
- No CLI, API, config file, or view changes. No new dependencies.
