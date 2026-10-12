## Why

Catching failing hardware early is the project's goal. It needs correct features and stable unit identity (P1, P2, CAN health); a model on the legacy pipeline would learn its bugs (06 §6). This change is roadmap phase **P5** (06 §2 P5), implementing decision D10 (05 §6, ADR-0010). The canvas board **Anomalies** sets the UX.

## What Changes

- **Detection over gold**, recomputed as an `anomalies` table after every derive and on demand (`flashpoint detect`). Each match is judged only against earlier matches, so results never look ahead. Methods, as the canvas names them:
  - **SPEC**: fixed limits. The thermal limits (55 °C mean / 65 °C maximum) carry over from legacy. The rest are stall time, reboots while enabled, long isolated CAN dropouts, and brownouts from the battery proxy.
  - **DRIFT** and **STEP**: a robust z-score (median/MAD) of each match feature against **that unit's** own history. DRIFT is a slow move and STEP is a sudden, sustained one. A swapped-in unit starts its own baseline.
  - **SIBLING**: a unit that departs from the other members of its sibling group in the same matches (for example, the four drive motors). Sibling groups are declared in the robot config.
  - **MODEL** (tier 3, optional, off by default): an isolation-forest detector over per-unit feature vectors, installed as an extra and never rated above medium severity.
- **Explanations** for every anomaly: actual against expected, first seen, matches affected, confidence, a per-match evidence series with its expected band, a "why" sentence, and likely causes from a team-editable catalogue. "Seen this before" lists closed anomalies on the same slot or unit with their outcomes.
- **Anomalies view** in the app shell, following the canvas. It has a queue (open, resolved, dismissed), relaxed/normal/strict sensitivity, and an evidence panel. **Triage writes**: investigate, resolve, dismiss as expected (which can restart the baseline), and reopen. A reason is required to close an anomaly. Triage is stored in the lake's metadata (backed up), accepted only from the local machine, and survives rebuilds.
- **Badges**: open anomalies show in the navigation, on Replay's match list and tracks, and in History's device health.
- **Backtest**: `flashpoint backtest` replays detection in match order against a hand-kept labels file (`config/failure-labels.toml`, seeded with Johnson Q105 and GACMP Q7). It reports which labels were caught, lead time, and the false-positive rate per method.

## Capabilities

### New Capabilities
- `anomaly-detection`: detectors, the anomalies table, severity, confidence, explanations, configuration, budgets
- `anomaly-review`: the Anomalies view, triage states and writes, and the navigation badge
- `detection-backtest`: no-lookahead replay against labels, with recall, lead time, and false-positive reports

### Modified Capabilities
- `match-reports`: anomaly badges in Replay (served live, static as a snapshot)
- `lifetime-trends`: device health includes open anomalies; the query service accepts guarded triage writes
- `robot-config`: optional sibling groups

## Pitfalls addressed

None owned; detection relies on the device-identity fix (03, 05 §4a).

## Non-goals

- Failure prediction (P6).
- Alerting (Slack/Discord webhook), until the backtest shows a tolerable false-positive rate.
- A Driver Station log reader (brownouts use the battery proxy).
- NOISE (idle-current jitter). It needs a new commanded-off feature in gold.
- Canvas items with no data: spin tests, battery resistance, current-limit counts, pit tasks.
- An absolute residual limit: residual means something only against the unit's own baseline (P2), so it is DRIFT/STEP only.
- STUMPY discords and River online learners.

## Impact

- `src/flashpoint/anomaly/`, an Anomalies view, CLI `detect` and `backtest`.
- `gold/anomalies`; a triage table in the ledger database.
- `config/anomaly.toml`, `anomaly-causes.toml`, `failure-labels.toml`.
- Optional extra `ml` (scikit-learn).
- New ADR for view writes (amends 0013); ADR-0010 accepted with amendments.
