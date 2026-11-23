# anomaly-causes.toml

`anomaly-causes.toml` holds the "likely causes" shown with each anomaly: suggestions to check, never a diagnosis. Flashpoint ships a [starting list](https://github.com/IgniteRobotics/flashpoint/blob/main/src/flashpoint/defaults/anomaly-causes.toml). Copy it to your config folder and add what you found each time you resolve an anomaly, so the next student sees it.

```toml
[[cause]]
method = "SPEC"
metric = "reboots"
text = "Loose power lug or connector"

[[cause]]
method = "STEP"
metric = "stator_current_p95"
subsystem = "intake"
text = "Something rubbing or jammed in the intake"
```

Each `[[cause]]` table:

| Key | Required | What it is |
|---|---|---|
| `method` | yes | `SPEC`, `DRIFT`, `STEP`, `SIBLING` or `MODEL` |
| `metric` | yes | The metric, as `anomaly.toml`'s [`metrics`](anomaly.md#baseline-drift-and-step) names it, or `reboots`, `longest_gap_ms`, `brownouts` or `feature_vector` (MODEL) |
| `subsystem` | no | Only for slots in this subsystem. These entries are listed first |
| `text` | yes | The cause, up to 300 characters |

## Common errors

From `flashpoint doctor`:

- A cause with no text:
  ```text
  anomaly-causes.toml: text: Field required
  ```
