## Why

This is the project's actual goal. Detection only makes sense on correct features with stable unit identity (P1, P2). A model trained on today's pipeline would learn its bugs (06 §6). Roadmap phase **P5**; decision D10.

## What Changes

- **Tier 1, rules:**
  - thermal limits (55 °C average / 65 °C maximum, carried over);
  - stall time;
  - current-residual P95;
  - CAN dropout and brownout counts.
- **Tier 2, baselines:** a robust z-score (median/MAD) for each feature against **that unit's** own history, plus comparison between sibling slots
- **Tier 3, models (optional):** batch isolation-forest or PyOD detectors on gold features, online half-space trees, matrix-profile discords on current traces
- An anomalies table with an explanation for each one. Badges in the views. Optional Slack or Discord webhook
- A backtest harness over the 2025–2026 logs, with pit notes as ground truth

## Capabilities

### New Capabilities
- `anomaly-detection`: detectors, scoring, explanations, thresholds, and how they are configured
- `anomaly-alerting`: alert delivery and suppression rules
- `detection-backtest`: reproducible evaluation against labeled history

### Modified Capabilities
- `match-reports`: shows anomaly badges
- `lifetime-trends`: shows anomaly badges

## Non-goals

- Time-to-failure prediction (P6)
- Driver Station log ingest, beyond the brownout and dropout counts (it may need its own change)

## Impact

New `src/flashpoint/anomaly/`. Dependencies: scikit-learn, and optionally pyod, river, stumpy.
