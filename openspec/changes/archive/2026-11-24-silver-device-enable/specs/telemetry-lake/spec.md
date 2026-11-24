## ADDED Requirements

### Requirement: Device enable state in silver
Silver SHALL hold each mapped device's enable state as the metric `device_enable`, valued 1 when the device reports itself enabled and 0 when it reports itself disabled. The state SHALL be read from the device's logged enable signal whether that signal was logged as text (`Enabled` / `Disabled`, exact match) or as a boolean. A text value other than those two SHALL produce no silver row for that sample, and SHALL NOT fail or quarantine the log or the session. Other device metrics SHALL keep their values unchanged.

A lake derived before this requirement SHALL gain `device_enable` rows the next time derive runs, without a raw-file rebuild.

#### Scenario: Text enable state is kept
- **WHEN** a hoot logs a TalonFX's enable signal as `Enabled` at one time and `Disabled` at a later time, and the session is derived
- **THEN** silver holds two `device_enable` rows for that slot, valued 1 and 0, on the aligned wpilog clock with phase and match time like any other metric

#### Scenario: Boolean enable state is still kept
- **WHEN** a device's enable signal is logged as a boolean true
- **THEN** silver holds a `device_enable` row valued 1

#### Scenario: Unknown text is dropped, not fatal
- **WHEN** a device's enable signal carries a text value that is neither `Enabled` nor `Disabled`
- **THEN** no silver row is written for that sample, the device's other samples are still written, and the session derives successfully

#### Scenario: Q7 enable state follows the match
- **WHEN** corpus group `2026-gacmp-q7` is ingested and derived
- **THEN** every TalonFX slot in the session has `device_enable` rows, all valued 0 or 1; at least 95 % of the session's `auto` and `teleop` rows are 1 and at most 1 % of its `pre`, `gap`, and `post` rows are 1; and per-unit powered and enabled seconds are unchanged from before this requirement

#### Scenario: Existing lake upgrades on derive
- **WHEN** a lake derived without `device_enable` is derived again by a version of Flashpoint that has this requirement
- **THEN** every session is re-derived and silver holds `device_enable` rows, and raw files are not re-read
