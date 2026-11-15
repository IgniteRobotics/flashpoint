## ADDED Requirements

### Requirement: CAN-gap floors scale with frame rate
The spread floors for the two CAN-gap baseline metrics, isolated CAN gaps and frames missed per enabled minute, SHALL stay as configured for a device sending at most 125 frames per second, which covers roboRIO-bus status rates. For a device sending more than 125 frames per second they SHALL scale up in proportion to its nominal frame rate. The floor used for a unit and metric SHALL be the configured floor when the rate is at most 125, and the configured floor × (rate ÷ 50) when it is above 125, using the median nominal frame rate of the unit's rows in the baseline window and the match being judged. Rows with no recorded frame rate (derived before rates were recorded) SHALL use the configured floor unchanged. The scaling SHALL apply to DRIFT, STEP and SIBLING, at every sensitivity. It SHALL NOT change the SPEC limit on the longest isolated gap, which stays in milliseconds whatever the rate. The "why it was flagged" text of a CAN-gap anomaly SHALL state the frame rate and the floor used.

#### Scenario: Jitter on a 250 Hz CANivore
- **WHEN** a synthetic 250 Hz unit has 0 isolated gaps and 0 frames missed per minute for 15 matches, then 9 isolated gaps of about 12 ms and 3.7 frames missed per enabled minute in each of the next 5 matches
- **THEN** it has no DRIFT, STEP or SIBLING anomaly on either CAN-gap metric at any sensitivity

#### Scenario: Real dropout still found
- **WHEN** the same synthetic 250 Hz unit instead goes silent for 1 s while enabled in each of matches 16–20, with about 2.5 enabled minutes per match
- **THEN** it has a HIGH SPEC anomaly on its longest isolated gap, and a STEP anomaly on frames missed per enabled minute with onset match 16

#### Scenario: Dropout on a corpus match
- **WHEN** detection runs on a synthetic copy of corpus `2026-gacmp-q7` in which one CANivore drive motor is silent for 1 s while enabled
- **THEN** there is a HIGH SPEC anomaly on that unit's longest isolated gap, and no CAN-gap anomaly on any other CANivore unit

#### Scenario: Rio-rate device keeps today's floor
- **WHEN** a synthetic 105 Hz unit has the same isolated-gap and frames-missed history as a corpus rio Talon FX with a normal-sensitivity DRIFT today
- **THEN** it is judged with the configured floors, and it has the same DRIFT at normal sensitivity as before frame rates were recorded

#### Scenario: Slow device keeps the configured floor
- **WHEN** a synthetic 4 Hz unit and a synthetic 50 Hz unit each have the same history of isolated gaps
- **THEN** both are judged with the configured floors, and their anomalies are the same as before frame rates were recorded

#### Scenario: Rows without a frame rate
- **WHEN** detection runs on a lake whose CAN-health rows have no frame rate
- **THEN** it completes, using the configured floors, and the summary counts the rows judged without a rate

## MODIFIED Requirements

### Requirement: SPEC limits
The system SHALL flag a match when a configured fixed limit is crossed. These limits are the defaults, and each is configurable:

| Metric | MED | HIGH |
|---|---|---|
| maximum temperature | ≥ 65 °C | ≥ 75 °C |
| mean temperature | ≥ 55 °C | — |
| stall time | ≥ 5 s | — |
| reboots | any reboot not while enabled | any reboot while enabled |
| longest isolated CAN gap | ≥ 0.25 s | ≥ 1 s |
| brownouts (battery proxy below the brownout threshold for the hold time, with the same proxy and rule as Replay's markers; subject is the robot's `power`) | ≥ 1, when the proxy is the median fallback | ≥ 1, when the proxy is the robot's declared battery-proxy slot |

A brownout anomaly's explanation SHALL name the battery proxy it used. A brownout found on the median fallback SHALL be at most MED. Its explanation SHALL say that the fallback can read wiring drops as brownouts, and that declaring a battery-proxy slot makes the finding trustworthy. A SPEC anomaly's severity SHALL be the highest severity reached in its episode. Its expected value SHALL state the limit. SPEC anomalies SHALL show at every sensitivity.

#### Scenario: Roller reboot
- **WHEN** detection runs on a synthetic copy of Johnson Q105, with a reboot while enabled on the intake roller leader after an 820 ms isolated gap
- **THEN** there is a HIGH SPEC anomaly for that unit's reboots, and a MED SPEC anomaly for its longest isolated gap, both with onset `2026johnson_qm105`

#### Scenario: Hot motor
- **WHEN** a synthetic unit's maximum temperature is 66 °C in one match
- **THEN** there is a MED SPEC anomaly with actual "66 °C" and expected "< 65 °C"

#### Scenario: No brownouts from motor dips
- **WHEN** detection runs on corpus `2026-gacmp-q7` with the shipped configuration
- **THEN** there is no brownout anomaly, because the gyro's supply voltage stays above 6.75 V even where motors' supply voltages dip below it

#### Scenario: Q7 brownouts
- **WHEN** detection runs on corpus `2026-gacmp-q7` with a test configuration that sets the brownout threshold to 8.5 V and the sag threshold to 9.0 V (above Q7's lowest battery proxy voltage)
- **THEN** there is one HIGH SPEC brownout anomaly whose subject is `power` for robot `2026-comp`, and its actual value counts the brownout intervals in the match

#### Scenario: Brownout on the median fallback
- **WHEN** detection runs on corpus `2026-gacmp-q7` with the same test thresholds and a copy of the robot configuration that declares no battery proxy
- **THEN** any brownout anomaly for `power` is MED, never HIGH, and its explanation names "median of N devices" and suggests declaring a battery-proxy slot

### Requirement: Optional model detector
The system SHALL include a model detector, off by default. When it is enabled in configuration and its optional dependency is installed, it SHALL score each unit-match feature vector against all earlier unit-matches of the same subsystem. It SHALL flag the top-scoring fraction (default 1 %) once at least the minimum training size (default 200 unit-matches) is available.

MODEL anomalies SHALL be:
- at most MED severity;
- shown only at normal and strict sensitivity;
- explained by naming the three features furthest from the unit's baseline.

If it is enabled but its dependency is missing, detection SHALL warn once and skip it, never fail. The warning, and the comment beside the switch in the shipped configuration, SHALL name the `ml` extra and give the install command for each supported install method: pipx, uv tool, pip and a development checkout. It SHALL be deterministic for the same inputs.

#### Scenario: Off by default
- **WHEN** detection runs with the default configuration
- **THEN** no MODEL anomalies exist, and the optional dependency is not imported

#### Scenario: Enabled without the dependency
- **WHEN** the model detector is enabled and its dependency is not installed
- **THEN** detection completes with a warning naming the missing extra and the commands `pipx install --force "flashpoint[ml]"`, `uv tool install "flashpoint[ml]"` and `pip install "flashpoint[ml]"`, and the other methods' anomalies are unchanged

#### Scenario: Deterministic
- **WHEN** the model detector runs twice on the same synthetic season
- **THEN** both runs flag the same unit-matches

### Requirement: Partial and malformed inputs
Detection SHALL skip rows it cannot judge and SHALL never fail the whole run for one bad row. Rows with a missing value for a metric SHALL be skipped for that metric only. Matches with low alignment confidence SHALL be judged, and their anomalies SHALL carry that flag. Sessions quarantined or marked incomplete-read upstream SHALL count toward baselines only through the gold rows they produced. A unit whose rows are all missing for a metric SHALL produce no anomaly for it. Missing temperature (non-Pro hoots, or a device model whose temperature is not tracked) SHALL never produce a temperature anomaly. When a unit's temperature disappears or reappears because its hoots' licensing changed, that SHALL NOT count as a change by any method. The summary SHALL count the temperature rows skipped because the hoots were not Pro-licensed, separately from other skipped rows.

#### Scenario: Missing residual
- **WHEN** a unit's gold rows have no residual (no motor constants)
- **THEN** it has no residual anomalies, and its temperature anomalies are unaffected

#### Scenario: Truncated corpus log
- **WHEN** detection runs on a lake built from the whole golden corpus, including the truncated and quarantined files
- **THEN** it completes without error, and the summary counts the rows it skipped by reason

#### Scenario: Non-Pro season
- **WHEN** detection runs on a synthetic season of 30 matches whose hoots are all non-Pro and carry no temperature
- **THEN** it completes, there are no temperature anomalies by any method, current and CAN anomalies are judged as usual, and the summary counts the temperature rows skipped as "not logged (needs Phoenix Pro)"

#### Scenario: Licence lapses mid-season
- **WHEN** a synthetic unit has temperature from Pro hoots in matches 1–15 and none from non-Pro hoots in matches 16–30
- **THEN** it has no temperature anomaly with onset after match 15, and no SIBLING temperature anomaly in matches 16–30
