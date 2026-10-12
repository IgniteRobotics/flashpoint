# 0010. Anomaly detection v1: rules + per-unit robust baselines + physics residuals

- Status: Accepted with amendments (2026-10-11)
- Date: 2026-10-04
- Refs: 04 §4, D10; 06 P5; openspec change `p5-anomaly-detection`

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
- **Brownouts come from the battery proxy** (the lowest device supply voltage), counted by Replay's own marker rule. No Driver Station log reader.
- **STEP needs a jump.** The run's median must be `step_z` spreads from the matches just before it. A STEP inside a drift's baseline window suppresses the DRIFT. Without this, a slow drift reads as a STEP once it crosses the threshold, and a step reads as a DRIFT while its new level enters the baseline.
- Triage writes, the app's first stored write, are [ADR-0015](0015-anomaly-triage-writes.md).

