# 0010. Anomaly detection v1: rules + per-unit robust baselines + physics residuals

- Status: Proposed
- Date: 2026-10-04
- Refs: 04 §4, D10; 06 P5

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
