# report.toml

`report.toml` sets the limits behind Replay's rule markers, History's health badges and the charts' reference lines, and where to find AdvantageScope. Most teams never change it. The values and their defaults are in the [bundled default](https://github.com/IgniteRobotics/flashpoint/blob/main/src/flashpoint/defaults/report.toml); copy it to your config folder to change one. Unknown keys are an error. After an edit, run `flashpoint report` (or let the watch do it) to rebuild Replay data.

## `[thresholds]`

| Key | Used for |
|---|---|
| `temp_warn_c` | Motor temperature for a WARN marker, History's WARN health, and the temperature reference line |
| `temp_fault_c` | Temperature for a FAULT marker and History's HOT health |
| `brownout_v` | The battery proxy below this voltage ... |
| `brownout_hold_s` | ... for at least this long is a brownout: a FAULT marker |
| `sag_v` | The battery proxy below this voltage: a WARN marker |
| `voltage_danger_v` | History's minimum-voltage reference line |
| `sample_gap_s` | A whole CAN bus silent this long during the match: WARN |
| `dropout_warn_s` | One device's CAN frames stopping this long while others still talk: WARN. At least the event floor Flashpoint keeps gaps from |
| `gap_limit_s` | How long one battery proxy sample can count as low, at most, before the next |

The battery proxy is the robot file's [`battery_proxy`](robots.md#the-battery-proxy) slot, or else the median supply voltage across devices. Detection has its own limits, in [`anomaly.toml`](anomaly.md).

## `[advantagescope]`

| Key | Used for |
|---|---|
| `path` | The AdvantageScope program to open matches in. Empty (the default) uses the newest WPILib install, then a standalone install. On Windows, use single quotes so backslashes stay literal: `path = 'C:\Users\Public\wpilib\2026\advantagescope\AdvantageScope (WPILib).exe'` |

See [Open a match in AdvantageScope](../../how-to/open-in-advantagescope.md).

## Common errors

From `flashpoint doctor`:

- A misspelt threshold:
  ```text
  report.toml: unknown keys ['temp_warn']
  ```
