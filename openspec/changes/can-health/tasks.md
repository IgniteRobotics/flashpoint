## 1. Inputs: health profile and configuration

- [x] 1.1 Write failing tests in `tests/readers/test_hoot.py` asserting `HEALTH_PATTERN` matches `Phoenix6/TalonFX-2/ResetCount` and `Phoenix6/TalonFX-2/Timestamp`; add `ResetCount|Timestamp` to the pattern; verify the tests pass
- [x] 1.2 Re-ingest the golden corpus into a scratch lake and compare bronze with a pre-change lake row for row (signal, ts_us, values); if identical, leave `PIPELINE_VERSION` at 2 and record "unchanged" in design.md D-h; if not, bump it to 3 with a comment, and verify with `pytest -m corpus`
- [x] 1.3 Write failing tests for an optional `flashpoint` season root: `root_names()` omits unset roots, a 2025 robot declaring a `flashpoint` signal fails to load naming the signal and root (nt-signals "Root not defined for the season"); implement in `robot_config.py`; verify tests pass
- [x] 1.4 Write failing tests for `[[bus]]` in robot configuration (`name`, `hoot` validated like `Slot.bus`; duplicate names rejected); implement; verify tests pass
- [x] 1.5 Set `flashpoint = "/Flashpoint/"` in `config/seasons/2026.toml`; declare the 10 bus-status signals (subsystem `can`, component = bus name, snake_case metrics) and `[[bus]]` rio → `rio`, DriveTrain → `canivore` in `config/robots/2026-comp.toml`; update the corpus test for "2026 declarations match the corpus" to exclude the `flashpoint` root and assert the 10 are listed `missing` for Q7; verify `pytest tests/semantics -k signals` and `pytest -m corpus -k signals` pass
- [x] 1.6 Write a synthetic-wpilog test for "Bus status mapped" (silver rows with subsystem `can`, components `DriveTrain`/`rio`); verify it passes with no derive code changes

## 2. Synthetic fixtures

- [x] 2.1 Add a test helper that builds a bronze hoot partition from a declarative spec (devices, period, silences, `RobotEnable` intervals, `ResetCount`/`Fault_BootDuringEnable` samples); verify with a self-test that frame counts and intervals match the spec
- [x] 2.2 Build fixtures: `q105_like` (10 rio devices at 8 ms; device 2 silent 820 ms, reset 0→1 at 344.96 s, boot-during-enable at 345.16 s), `bus_wide` (6 of 10 silent ~60 ms, starts up to 5 ms apart), `whole_bus` (all silent 1.4 s), `stops_early` (one device stops 3 s before teleop end), `no_reset_count`; verify each fixture's self-test passes

## 3. Detection core (`semantics/can_health.py`)

- [x] 3.1 Write failing tests for enabled intervals from the hoot's `RobotEnable` and the 5 s start-up skip; implement; verify tests pass
- [x] 3.2 Write failing tests for nominal period and gap detection ("Gap against the device's own period": 40 ms on 8 ms → 4 missed; "Normal jitter is not a gap"; "Disabled time ignored"); implement the per-hoot DuckDB interval query; verify tests pass
- [x] 3.3 Write failing tests for trailing silence and hoot end ("Device stops before the window ends", "Truncated hoot"); implement; verify tests pass
- [x] 3.4 Write failing tests for sibling classification by interval overlap ("One device drops out", "Overlapping, not equal", unmapped devices counted as siblings); implement the overlap self-join; verify tests pass, and grep the module to confirm no equality join on timestamps
- [x] 3.5 Write failing tests for reboot events ("Reboot while enabled" merged within 2 s, "Baseline is not a reboot", `no_reset_count` → boot-during-enable only and `reset_count_logged = false`); implement; verify tests pass

## 4. Gold tables and derive wiring

- [x] 4.1 Write failing tests for `can_health` rows: per (match, phase, slot, unit), phase cuts via framing shifted by `offset_us`, `missed_per_min` over enabled minutes, mid-session swap attribution ("Mid-session swap"); implement; verify tests pass
- [x] 4.2 Write failing tests for `can_bus_health` rows (logger gap counts/longest/total, device count; bus-status summary from silver signals via `[[bus]]`; empty columns when not logged) covering "Bus status logged" and "Older robot code"; implement; verify tests pass
- [x] 4.3 Write failing tests for `can_events` (all reboots; gaps ≥ 0.1 s only; unframed sessions keep lake time with empty match time); implement; verify tests pass
- [x] 4.4 Wire `can_health` into `Deriver.run` for stale sessions via `gold.write_session(..., table, schema)`; bump `DERIVE_VERSION` to 4 with a comment; write a test that a hoot-less session (`2026-gacmp-p2`-like) and an unframed session derive without error and without slot rows; verify `pytest tests/semantics` passes
- [ ] 4.5 Add corpus tests: Q7 has zero reboots, a `match`/`auto`/`teleop` row per mapped hoot device, rio and CANivore bus rows with logger gaps, no CANcoder isolated gap, no CANivore device > 5 isolated gaps or > 50 ms; `2025-gacmp-q19` (unaligned 2025 hoots) has no rows and no reboot event, and its events carry hoot time only; `2026-gacmp-e10-truncated` has no gap past its hoot's last frame; `2026-gacmp-p2` has no rows; two `--force` derives give identical rows; verify `pytest -m corpus` passes

## 5. Budgets

- [x] 5.1 Add the Q7 CAN-health timing and peak-memory assertion (< 3 s, < 150 MB over derive without the step, VmHWM on Linux) to the corpus perf tests; verify it passes locally and record the measured numbers in design.md Budgets
- [x] 5.2 Run the full derive on `~/flashpoint-lake` with `--force` and record wall time against the pre-change run; verify the increase is within 3 s per match

## 6. Views

- [x] 6.1 Write failing tests for `report.toml`: `dropout_warn_s` accepted (default 0.25), below 0.1 rejected with a message, existing files with `sample_gap_s` still load; implement in `report/settings.py` and update the comments in `config/report.toml`; verify tests pass
- [x] 6.2 Write failing marker tests ("Reboot marker" message "Device rebooted (reset count 0 → 1)", "Long dropout marker" "No CAN frames for 0.82 s", "Whole bus silent" on `bus:rio` with no slot dropout markers, unmapped devices get none); replace `gap_markers` with CAN rules reading `gold/can_events`; bump `REPORT_VERSION` to 3; verify tests pass
- [x] 6.3 Update the Replay front end so `bus:<name>` sources render in the timeline, marker list, and match-list counts, escaped as text; add a browser test that a `bus:rio` marker is focusable and moves the cursor; verify the browser suite passes
- [x] 6.4 Add the corpus test "Logger gaps stay quiet" (Q7 Replay has no reboot, dropout, or bus-gap marker) and re-check the Q7 payload is under 2 MB; verify `pytest -m corpus` passes
- [ ] 6.5 Write failing tests for `Metric.table` and the four CAN metrics in History queries (filters still bind as parameters; "No hoot for a match" gives a gap, not zero; hostile filter test still passes on the new table); implement in `views/queries.py`; verify tests pass
- [ ] 6.6 Add the CAN metrics to the History metric picker and a browser test for "Worsening connector" (four points in order, last point drills to Replay); verify the browser suite passes

## 7. Field check, docs, and gate

- [ ] 7.1 [HUMAN] Derive and serve `~/flashpoint-lake`; confirm Q105 shows a FAULT reboot and a WARN dropout on the intake roller slot near hoot time 344–345 s, TalonFX 14's isolated-gap line climbs at Q105, and Q15/Q53/Q76 show no reboot or dropout markers
- [ ] 7.2 Update `docs/rewrite/06-roadmap.md` §P5 Tier 1 (CAN dropout counts now come from hoots via can-health; the DS log reader is no longer required for them) and `docs/rewrite/05-target-architecture.md` §4 (the three gold tables); verify with `grep -n "CAN dropout" docs/rewrite/06-roadmap.md` that the P5 line names can-health, and that the relative links in both edited sections point to files that exist
- [ ] 7.3 Run ruff, mypy --strict, the full pytest suite, the corpus e2e and perf jobs, and the browser suite; open the PR into `rewrite` with the measurement table and budgets in the description; verify CI is green
