# 0010. Anomaly detection v1: rules + per-unit robust baselines + physics residuals

- Status: Accepted with amendments (2026-10-11, 2026-10-12, 2026-11-25)
- Date: 2026-10-04
- Refs: 04 §4, D10; 06 P5; openspec changes `p5-anomaly-detection`, `battery-proxy`, `mvp-outside-team-logs`

## Context
There are about 50–150 matches per season and few labelled failures. Students must be able to trust and explain alerts.

## Decision
- **Tier 1:** rules (thermal limits, stall time, CAN dropouts, brownouts).
- **Tier 2:** a robust z-score (median/MAD) per unit against that unit's own history, plus sibling-slot comparison.
- **Tier 3:** ML detectors (isolation forest, PyOD, River, STUMPY), added only once tiers 1 and 2 are backtested.
- **Physics residual:** measured current minus the DCMotor-model current is a core feature.

## Consequences
- Explainable alerts from the first season.
- ML is deferred until there is data.

## Alternatives considered
- **ML first:** too little data, and models would learn today's pipeline bugs.

## Amendments (2026-10-11, P5)
P5 implemented this decision with these changes:
- **MODEL is optional and off by default.** An isolation forest per subsystem is fit on earlier events only, capped at MED severity, and needs the `ml` extra (scikit-learn). If it is enabled without the extra, detection warns and skips it.
- **No SPEC limit on the residual.** The current residual means something only against the unit's own baseline (P2), so it is a DRIFT/STEP metric only.
- **NOISE, STUMPY, and River are deferred.** NOISE needs a commanded-off feature in gold.
- **Brownouts come from the battery proxy** (the lowest device supply voltage), counted by Replay's own marker rule. No Driver Station log reader. (The proxy is redefined by the 2026-10-12 amendment.)
- **STEP needs a jump.** The run's median must be `step_z` spreads from the matches just before it. A STEP inside a drift's baseline window suppresses the DRIFT. Without this, a slow drift reads as a STEP once it crosses the threshold, and a step reads as a DRIFT while its new level enters the baseline.
- Triage writes, the app's first stored write, are [ADR-0015](0015-anomaly-triage-writes.md).

## Amendment (2026-10-12, battery-proxy)
The lowest device supply voltage read one motor's own wiring drop under load as a brownout: it flagged 16 of the 24 matches in the 2026 lake, while the gyro never went below 7.59 V. So:
- **The battery proxy is a declared low-current device.** A robot configuration names one slot on the main power bus (`battery_proxy`; the gyro on `2026-comp`). When it declares none, or that device logged no supply voltage, the proxy is the median supply voltage across devices in each 20 ms bin (at least 3 devices). Replay's battery track and power markers and detection's brownout count share that proxy, and every power marker and brownout anomaly names it.
- **No hold across logging gaps.** A low proxy sample counts until the next sample, at most `gap_limit_s` (0.1 s). The brownout hold returns to 0.02 s.
- **Supply path resistance** is a new baseline metric (DRIFT, STEP, SIBLING): per motor and period, the slope of (proxy voltage − motor supply voltage) against the motor's supply current, in mΩ. It is absent when there is no declared proxy, too little current to fit, or a negative slope. This is the deferred battery-resistance metric, measured on each motor's connection rather than the battery.
- A Driver Station log reader is still deferred.

## Amendment (2026-11-25, mvp-outside-team-logs)
Two refinements for robots that aren't Ignite's, and for Ignite's own CANivore:
- **A brownout on the median fallback is at most MED.** When the robot file declares no `battery_proxy`, the fallback can read wiring drops as brownouts. Its counts, thresholds hash and anomaly ids are unchanged. Only the severity is capped, and the `why` says why and suggests declaring a battery-proxy slot.
- **CAN-gap spread floors scale with frame rate.** For isolated CAN gaps and frames missed per enabled minute, a device sending at most 125 frames per second keeps the configured floor. Above that, the floor is the configured floor × rate ÷ 50 (constants `CAN_FLOOR_RIO_MAX_HZ`, `CAN_FLOOR_REFERENCE_HZ`). The rate is the median over the baseline window plus the judged match, from gold `can_health.frame_rate_hz`. It applies to DRIFT, STEP, SIBLING and MODEL scaling. The SPEC limit on the longest gap is untouched. On Ignite's lake this cut CANivore CAN-gap findings from 41 to 1 (a strict LOW DRIFT), with every rio finding unchanged. The `why` states the rate and the floor. Rows derived before rates were recorded keep the configured floor, and the summary counts them.
