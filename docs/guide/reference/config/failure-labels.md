# failure-labels.toml

`failure-labels.toml` records real failures and repairs, so [`flashpoint backtest`](../cli.md#flashpoint-backtest) can check whether detection caught them, and how early. There's no default: you write it as things break. Ignite's is [`config/failure-labels.toml`](https://github.com/IgniteRobotics/flashpoint/blob/main/config/failure-labels.toml).

A label is caught when an anomaly on its motor (and metric, when given) is open at its match, or within the 5 matches before it.

```toml
[[label]]
robot = "2026-comp"
slot = "intake-roller"
match = "2026johnson_qm105"
kind = "failure"
metric = "reboots"
note = "Intake roller leader rebooted while enabled after an 820 ms CAN dropout."
```

Each `[[label]]` table:

| Key | Required | What it is |
|---|---|---|
| `robot` | yes | A robot file's `robot` name |
| `unit` | a unit or a slot | The motor: a serial (`ctre:<serial>`) or a legacy unit id |
| `slot` | a unit or a slot | A slot id on that robot, or `power` for brownouts |
| `match` | a match or a date | A match key like `2026gacmp_qm7` |
| `date` | a match or a date | `YYYY-MM-DD`, for a failure outside a match |
| `kind` | yes | `failure`, `degradation` or `repair` |
| `note` | yes | What happened |
| `metric` | no | Only an anomaly on this metric counts |

## Common errors

From `flashpoint doctor`:

- A kind that isn't one of the three:
  ```text
  failure-labels.toml: kind: Input should be 'failure', 'degradation' or 'repair'
  ```
- A robot with no robot file:
  ```text
  failure-labels.toml: label 1: unknown robot 'x'
  ```
