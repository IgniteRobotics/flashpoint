## Context

See proposal.md for why. The current state:

- **One rule, two consumers.** `report/markers.power_markers` turns silver `supply_voltage` rows into intervals below a threshold, taking any device into account (the lowest device), and the Replay battery track plots `supply_voltage_min`. Detection's brownout count calls the same function through `report.build.match_span`, so the two always agree (anomaly-detection, "Brownouts agree with Replay").
- **The hold cap is the problem behind the 0.5 s numbers.** Each sample holds until its device's next sample, capped at `PhysicsConfig.hold_cap_us` = 1 s. A single low sample before a logging gap can therefore count as a brownout of up to 1 s. That cap was chosen for energy integration, not for event detection.
- **Gold features are computed one motor at a time** (`gold.session_features` → `physics.motor_features`), to keep memory bounded by one motor's samples (D3, D4).
- **Evidence from `~/flashpoint-lake`** (24 matches, GADAL, GACMP, Johnson):
  - The gyro's lowest supply voltage per match is 7.59–10.27 V.
  - The median across devices is below 6.75 V in 9 matches at a 0.02 s hold, and in none at 0.5 s.
  - Probed path resistance:
    - drive motors: 18–20 mΩ, steady from match to match;
    - flywheels: 34–52 mΩ, with the left follower about 50% above its leader;
    - steer and indexer motors: noise, including negative values, because they never draw enough current.
  - The probe scripts used: brownout_probe.py and resistance_probe.py. Their method is recorded below.

## Goals / Non-Goals

**Goals:**
- A brownout means the bus voltage that the roboRIO sees fell below its trip point. A single motor's wiring drop never counts as one.
- Replay and detection keep sharing one rule and one proxy.
- A per-unit connector-health metric whose value does not depend on load.

**Non-Goals:**
- Changing any other marker rule.
- Changing how energy is integrated: the 1 s hold cap stays for physics.
- Inferring a proxy automatically when the robot declares none (beyond the median fallback).

## Decisions

### 1. Declared proxy device, median fallback (refines D10)
`RobotConfig.battery_proxy: str | None` names a slot. `2026-comp` declares `gyro`, the Pigeon2: about 0.1 A, fed from the main bus, so its supply voltage tracks the bus within millivolts. For each match, the proxy series is that slot's `supply_voltage`. If the slot is undeclared or absent in the match, the proxy is the median across the devices present in each 20 ms bin, with at least 3 devices required.

- **Alternatives:**
  - Lowest device (today): reads wiring drops as brownouts.
  - A fraction of devices below the threshold: gave the same counts as the median in the probe, and is harder to explain.
  - roboRIO or PDH voltage: not in the 2026 logs (a GACMP wpilog has none among its 561 entries).
- **Recording which proxy was used:** the marker message and the anomaly's why name the proxy ("gyro" or "median of N devices"). The battery track becomes `battery_proxy_v`.
- **Spec impact:** ADR-0010's amendment says "brownouts from the battery proxy", and this defines the proxy more precisely, so no new ADR is needed. A dated note is added to ADR-0010's amendments.

### 2. Gap limit for power intervals
Power markers get their own `gap_limit_s` in `[thresholds]` (default 0.1 s), used in place of the 1 s physics cap when holding proxy samples. The proxy logs at about 100 Hz, so 0.1 s spans roughly 10 missed samples. The 1 s cap stays in place for physics.

- **Alternative:** a cap based on each device's median sample period. It is more adaptive, but harder to test and to explain in the message.

### 3. Brownout hold 0.02 s
This returns the default to its pre-tuning value. The proxy now carries no load-induced drop, so a short hold costs no noise: zero brownouts on the gyro across all 24 matches.

### 4. Supply path resistance
- **Inputs:** `gold.session_features` loads the proxy series once per session (one device, at most about 20k rows) and passes it to `motor_features`. There, each `supply_current` sample is as-of joined to the proxy voltage within the existing 50 ms tolerance (`_with`), and to the motor's own `supply_voltage`.
- **Estimate:** a time-weighted least-squares fit of drop = V_proxy − V_supply against I_supply, over samples with I_supply ≥ `resistance_min_a` (10 A). The intercept is free, because it absorbs calibration offsets between devices. The slope ×1000 gives `supply_resistance_mohm`.
- **Gates:**
  - absent with `no-proxy` when there is no declared proxy in the period. The median fallback is not used, because it contains the motor itself;
  - absent with `low-current` when there is less than `resistance_min_s` (1 s) of data at or above 10 A, or the P5–P95 current spread at those moments is under 15 A;
  - absent with `no-fit` when the slope is negative.
- **Detection:** the metric joins `BASELINE_METRICS`, with a floor of 3 mΩ, display "supply path resistance" in mΩ, and history-metric mapping `peak_current`. It is in the default `[baseline] metrics`. The sibling groups (drive, flywheel, intake roller) pick it up with no configuration change.
- **Alternatives:**
  - The raw P95 drop: confounded by load, which was the question you raised. Fitting against the motor's own supply current removes the effect of load. Voltage compensation changes output (motor) voltage, never supply voltage, so it can't bias the fit.
  - A ratio per sample: noisy near zero current.

### 5. Versions and caches
- `DERIVE_VERSION` +1: a new gold column, so one re-derive.
- `REPORT_VERSION` +1: the proxy series and markers change, so Replay rebuilds.
- `power.thresholds_hash` also covers the proxy slot, `gap_limit_s`, and the fallback rule, so brownout counts recount.
- `DETECT_VERSION` is unchanged, because the rows' schema is unchanged.

### 6. Corpus scenarios use a test threshold
Q7's gyro bottoms out at about 8.1 V, so shipped thresholds give no power anomaly. The badge, queue, and agreement scenarios run with a test config copy setting `brownout_v = 8.5` and `sag_v = 9.0`. The fixture records Q7's brownout count under that config, so a regression in the proxy shows up as a changed count. A new corpus scenario asserts zero brownouts with the shipped config.

### 7. Labels
The GACMP Q7 entry is removed from `config/failure-labels.toml`. The backtest corpus test drops to 2 seeded labels.

## Budgets

- **Derive:** one extra small read per session (the proxy device's `supply_voltage`) and one fit per motor-period, which is O(samples). Q7 ingest + derive + detect stays under its 30 s / 1 GB budget (it measures 14.6 s today). The 5.6 perf test is rerun.
- **Brownout count:** still at most 0.5 s per uncached match, because it reads one device's series instead of all of them.
- **Detect season budget:** one more baseline metric. It stays well inside 5 s / 400 MB (1.6 s today), and the perf test is rerun.

**Measured (2026-10-12, task 5.2, reference laptop, `pytest -m perf -s`):**
- Q7 ingest + derive + detect: 17.2–19.5 s, 720–840 MB peak (budget 30 s / 1 GB). The same test on the unchanged branch measured 16.8 s on the same day, so this change adds 0.4–2.5 s; the 14.6 s above was an earlier, quieter run.
- Uncached Q7 brownout count: 0.056 s, +32 MB (budget 0.5 s).
- Season detect (60 × 23): 1.88 s, 226 MB (budget 5 s / 400 MB).
- Backtest (two seasons): 3.7 s, 258 MB.

**Q7 values from the corpus derive:** drive 14.3–18.4 mΩ; flywheels 50.0 (left leader), 65.8 (left follower), 67.3 (right) mΩ; intake rollers 64.8 / 72.1 mΩ; indexer accelerator 35.1 mΩ. The hood and intake extension are `low-current`, and indexer-main is `no-fit`. Three steer motors pass the gates (0.7, 9.6, 10.0 mΩ), so they are noisier than the probe suggested; the 3 mΩ floor keeps them from flagging on noise alone. Under the 8.5 V test threshold, Q7 has 3 brownouts on the gyro (8.10, 8.27, 8.22 V).

## Risks / Trade-offs

- [The proxy device isn't on the main bus on another robot] → The proxy is declared per robot, and validation only checks that the slot exists. The robot TOML comment says what qualifies.
- [The proxy's own feed has a bad connector] → A brownout would be over-reported. The proxy's path-resistance-like behaviour isn't measurable (it draws no current). The why text names the proxy, so a student can check it.
- [No real brownout exists in 2026 data to validate against] → Synthetic scenarios cover the rule. The Pigeon-versus-median probe numbers are recorded here. A DS log reader (deferred) would provide ground truth later.
- [Path resistance includes shared upstream wiring (main breaker, PDH bus bar)] → Shared drops are common to the proxy and the motor only above the proxy's tap. Siblings share it too, so SIBLING comparisons cancel it.
- [Re-derive on upgrade] → It's one-time and expected; the lake re-derives on the next `flashpoint derive` (minutes on the current lake).

## Migration Plan

1. Upgrade.
2. Run `flashpoint derive` (re-derives every session because of the `DERIVE_VERSION` bump, then runs detect).
3. Run `flashpoint report` (rebuilds Replay).

To roll back, revert the commits and re-derive.
