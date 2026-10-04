## 1. Profile change and pipeline version

- [x] 1.1 Tests first: the `health` profile selects `DriveState/Pose`, `RotorVelocity`, `MotorKT`, `MotorKV`, and `MotorStallCurrent`; the corpus test confirms they're present in the Q7 CANivore output
- [x] 1.2 Extend `HEALTH_PATTERN`; bump `PIPELINE_VERSION` to 2; check that `flashpoint rebuild` reconverts hoots from raw
- [x] 1.3 Re-measure the Q7 budget with the wider profile and record it

## 2. Robot configuration (spec: robot-config)

- [x] 2.1 Add pydantic. Tests first for the config model: valid load, unknown key, duplicate slot, bad date, `canivore` bus matching
- [x] 2.2 Loader: `config/robots/*.toml`; robot selection by project and season; `robot = unknown` fallback
- [x] 2.3 Seed `config/robots/2026-comp.toml` from Robot-2026 `development` constants and the Q7 `ConnectedMotor` scan (23 slots: 18 motors and 5 sensors; 22 present in Q7)
- [x] 2.4 `tools/migrate-legacy-config.py`: the 2025 device CSVs become `2025-comp.toml` (`Pheonix6` fixed); the report lists NT-only rows as unmapped
- [x] 2.5 `flashpoint doctor` reports each robot's config and its TBD or unmapped slots

## 3. Match identity (spec: match-identity)

- [x] 3.1 Tests first: key format (qm, pm, e, replay), FMS-over-filename precedence with conflict warning, non-match classification
- [x] 3.2 Implement. Optional TBA enrichment (spec: MAY) is **deferred**: no network dependency in P2; revisit with P4 views

## 4. Sessions and clock alignment (spec: session-grouping)

- [x] 4.1 Tests first (synthetic): hoot-group-by-stamp, wpilog overlap join, hoot-only sessions, restart with two hoot groups
- [x] 4.2 `payload-match` alignment (struct bytes and unique doubles, median and IQR, at least 50 matches, spread under 5 ms), with synthetic tests
- [x] 4.3 `enable-edges` and `wall-clock` fallbacks with confidence; cross-bus agreement check
- [x] 4.4 Corpus tests: Q7 aligns by `payload-match` at about 19.56 s with spread under 5 ms; buses agree under 1 ms; E10 gives one session with two hoot-group offsets

## 5. Device identity (spec: device-identity)

- [x] 5.1 Tests first: slot resolution (bus, model, CAN id); legacy epochs from swap dates; inventory serial units; mid-session swap split; unmapped device report
- [x] 5.2 Implement the slots, units, and observations tables (ledger and snapshots)

## 6. Match framing (spec: match-framing)

- [x] 6.1 Tests first: pre/auto/gap/teleop/post from synthetic `DS:enabled` and `DS:autonomous`; never-enabled session; match-time origin
- [x] 6.2 Implement; corpus test: Q7 has exactly one auto and one teleop period

## 7. Silver (spec: telemetry-lake)

- [x] 7.1 Tests first: metric normalization, clock shift onto the wpilog clock, phase and match time per sample, atomic per-session write
- [x] 7.2 Implement the `silver/samples` writer (streamed per hoot window, bounded memory)

## 8. Physics (spec: motor-physics)

- [x] 8.1 Port the power-tracking analyzer tests (time-weighted, no zero-fill, signed velocity), plus the spec scenarios (19 A mean, 2.0 Wh, 2 s stall)
- [x] 8.2 Time-weighted stats, power and energy, thermal rise rate, stall time
- [x] 8.3 Residual from device constants. First confirm the units of `MotorKV` and `MotorStallCurrent` against the Q7 values and WPILib's Kraken X60 model; near-zero-residual test on a free-spin segment; `no-motor-constants` path

## 9. Gold and commands (spec: match-features)

- [x] 9.1 `gold/match_features` writer; rows per (match, phase, slot, unit) with alignment confidence and source logs
- [x] 9.2 `flashpoint derive [--all]`; ingest runs derive for touched sessions; `rebuild` covers derived layers
- [x] 9.3 Corpus e2e: Q7 produces 17 slots × 3 phases of rows; a season maximum-temperature query runs under 1 s; the second derive is a no-op
- [x] 9.4 Budget: Q7 ingest plus derive stays under 30 s and 1 GB

## 10. Docs and wrap-up

- [x] 10.1 ADR-0012 (clock alignment by shared payload); update ADR-0011 with the slot and unit implementation notes
- [x] 10.2 Update `docs/rewrite/05` (§3 pipeline, §4 data model) and the `06` P2 status
- [x] 10.3 README: config files, `derive`, and example lifetime queries
- [ ] 10.4 Start `retire-legacy-code` stage 2b after archive (separate PR): `datamaps/` and `log_configs/` once the migration covers them
