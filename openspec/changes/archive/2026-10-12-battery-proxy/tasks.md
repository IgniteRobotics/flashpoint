## 1. Robot configuration

- [x] 1.1 Write failing tests: `battery_proxy` is optional; an unknown slot is rejected with the file and slot named (Unknown battery proxy); `2026-comp` declares `gyro` (Valid 2026 configuration). Then add `battery_proxy` to `RobotConfig` and `config/robots/2026-comp.toml` (with a comment on what qualifies as a proxy). Verify the robot-config tests pass, and that the derive and Replay fingerprints change only as decided (design §5)

## 2. Battery proxy and power markers

- [x] 2.1 Write failing marker tests:
  - Brownout marker (the message names the gyro);
  - Motor dip is not a brownout;
  - Median fallback (the message names the median of N devices);
  - Logging gap does not stretch a dip (the marker lasts 0.1 s, not 0.8 s);
  - the existing sag and brief-dip tests rewritten for the proxy.

  Then implement the proxy series (the declared slot, else a per-20 ms-bin median of at least 3 devices) and `gap_limit_s` in `report/markers.py` and `report/settings.py`. Verify `tests/report/test_markers.py` passes
- [x] 2.2 Write failing tests for the battery track: Replay's default track is `battery_proxy_v`, labelled with the proxy used. Then switch `report/build.py` from `supply_voltage_min`, and bump `REPORT_VERSION`. Verify the report build and browser track tests pass
- [x] 2.3 Set the `brownout_hold_s` default to 0.02 and add `gap_limit_s = 0.1` to `config/report.toml` and the code defaults. Verify `test_repo_config_matches_defaults` passes
- [x] 2.4 Write failing tests: `power.thresholds_hash` changes with the proxy slot, `gap_limit_s`, and the fallback rule; brownout counts equal Replay's FAULT markers under the new rule. Then update `anomaly/power.py`, and name the proxy in the brownout anomaly's why. Verify the power tests pass

## 3. Supply path resistance

- [x] 3.1 Write failing physics tests:
  - Known resistance (20 mΩ within 1 mΩ);
  - Load does not change it;
  - Too little current (`low-current`);
  - no proxy (`no-proxy`);
  - negative slope (`no-fit`).

  Then implement the time-weighted fit in `semantics/physics.py`, with `resistance_min_a` and `resistance_min_s` in `PhysicsConfig`. Verify the tests pass
- [x] 3.2 Write failing gold tests: `match_features` carries `supply_resistance_mohm` and its flag per (match, phase, slot, unit); the proxy series is read once per session. Then wire `gold.session_features` and bump `DERIVE_VERSION`. Verify the gold and derive tests pass
- [x] 3.3 Write the failing corpus test Corpus drive motors (each drive motor's Q7 match resistance is between 5 and 40 mΩ). Verify it passes against the real Q7 derive
- [x] 3.4 Write failing detection tests: Degrading connector (a STEP on `supply_resistance_mohm` with onset at match 16); the metric is in the default baseline metrics with a 3 mΩ floor and displays in mΩ; a flywheel sibling with higher resistance than its group raises SIBLING. Then update `anomaly/settings.py` (BASELINE_METRICS, METRIC_INFO, floors, `config/anomaly.toml`) and the history-metric mapping in `anomalies.js`. Verify the anomaly tests pass

## 4. Corpus scenarios, labels, backtest

- [x] 4.1 Add a corpus test config fixture (a copy of `config/` with `brownout_v = 8.5`, `sag_v = 9.0`). Record Q7's brownout count under it. Write the corpus test No brownouts from motor dips (shipped config: no `power` anomaly on Q7). Verify the corpus anomaly tests pass
- [x] 4.2 Move the Q7 brownout scenarios to the test config fixture:
  - Q7 brownouts;
  - Brownouts agree with Replay;
  - Queue from the corpus;
  - Served badge;
  - Resolved anomaly drops its badge;
  - Static snapshot.

  Verify the corpus and browser suites pass
- [x] 4.3 Remove the GACMP Q7 label from `config/failure-labels.toml`, and update the seeded-labels test (at least two labels) and the backtest tests that counted three. Verify the backtest tests pass

## 5. Docs, gate, field check

- [x] 5.1 Add a dated amendment to ADR-0010 (the battery proxy is a declared low-current device with a median fallback; adds supply path resistance). Update docs/rewrite 06-roadmap P5 deferred items (battery-resistance metric delivered as supply path resistance; DS log reader still deferred) and the README's Anomalies section. Verify the links resolve
- [x] 5.2 Rerun the perf budgets: Q7 ingest + derive + detect < 30 s and < 1 GB; brownout count ≤ 0.5 s; season detect < 5 s and < 400 MB. Record the measurements in design.md
- [x] 5.3 Run the full gate locally (ruff, mypy, unit, corpus, browser, perf), then open a PR into `rewrite`. Verify CI is green on all jobs
- [x] 5.4 [HUMAN] Re-derive `~/flashpoint-lake`, then run `flashpoint report`, `detect` and `backtest`. Confirm:
  - there are no brownout anomalies, or every one is backed by a real gyro dip;
  - drive-motor path resistance reads 15–25 mΩ in History and Anomalies evidence;
  - the flywheel follower's higher resistance shows as SIBLING, or is consistent with its wiring.

  Record the results in design.md
