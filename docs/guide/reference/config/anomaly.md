# anomaly.toml

`anomaly.toml` sets how [anomaly detection](../views/anomalies.md) judges each motor. The values and their defaults are in the [bundled default](https://github.com/IgniteRobotics/flashpoint/blob/main/src/flashpoint/defaults/anomaly.toml), with a comment on each; copy it to your config folder to change one. Unknown keys are rejected. After an edit, run `flashpoint detect`; you don't need to re-derive.

The five methods are described in [How it works](../../under-the-hood/how-it-works.md). Each table below belongs to one of them.

## `[spec]`: fixed limits

Judged per match, and shown at every sensitivity.

| Key | Finding |
|---|---|
| `temp_max_med_c` | Match maximum temperature at or above this: MED |
| `temp_max_high_c` | ... HIGH |
| `temp_mean_med_c` | Match mean temperature: MED |
| `stall_med_s` | Stall time in a match: MED |
| `gap_med_ms` | Longest isolated CAN gap: MED |
| `gap_high_ms` | ... HIGH |
| `brownout_high` | This many brownouts in a match (same rule as Replay's FAULT marker): HIGH |

Reboots have no key: any reboot while enabled is HIGH, any other is MED.

## `[baseline]`: DRIFT and STEP

Each motor against its own history (median and MAD of its last matches).

| Key | What it sets |
|---|---|
| `metrics` | The metrics judged against the motor's history |
| `window` | How many previous matches make the baseline |
| `min_baseline` | Fewer prior matches than this: "building baseline", no DRIFT or STEP |
| `step_z` | STEP: the last `step_persist` matches all beyond this z, same sign |
| `step_persist` | ... for this many matches |
| `drift_z` | DRIFT: the median of the last `drift_span` matches beyond this z |
| `drift_span` | ... over this many matches |
| `med_z` | Severity: MED at \|z\| of at least this |
| `high_z` | ... HIGH at least this; below `med_z`, LOW |
| `floors` | The `[baseline.floors]` table: the smallest spread per metric, so a very steady history can't make every change huge |

## `[sibling]`: SIBLING

Each motor against the median of its [sibling group](robots.md#siblings-slots-compared-with-each-other).

| Key | What it sets |
|---|---|
| `z` | How far from the group's median counts |
| `need` | Flagged in at least this many ... |
| `of` | ... of the last this-many matches the whole group played |

## `[strict]`: the strict sensitivity

Extra findings at lower thresholds, shown when the Anomalies view's sensitivity is **strict**. Each must be at or below the normal value.

| Key | Instead of |
|---|---|
| `step_z` | `[baseline] step_z` |
| `drift_z` | `[baseline] drift_z` |
| `sibling_z` | `[sibling] z` |
| `persist` | `[baseline] step_persist` |

## `[model]`: MODEL (off by default)

An isolation forest over each motor's match features. It needs the optional `ml` extra installed.

| Key | What it sets |
|---|---|
| `enabled` | Turn it on |
| `contamination` | The share of scores to flag (0.01 is the top 1 %) |
| `min_train` | Motor-matches needed before it scores anything |

## Common errors

From `flashpoint doctor`:

- A value that isn't a number:
  ```text
  anomaly.toml: baseline.step_z: Input should be a valid number, unable to parse string as a number
  ```
