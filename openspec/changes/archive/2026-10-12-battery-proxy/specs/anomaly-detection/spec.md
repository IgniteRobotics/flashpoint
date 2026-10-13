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
| brownouts (battery proxy below the brownout threshold for the hold time, with the same proxy and rule as Replay's markers; subject is the robot's `power`) | — | ≥ 1 |

A brownout anomaly's explanation SHALL name the battery proxy it used. A SPEC anomaly's severity SHALL be the highest severity reached in its episode. Its expected value SHALL state the limit. SPEC anomalies SHALL show at every sensitivity.

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

### Requirement: Per-unit baselines (DRIFT and STEP)
For each unit and each baseline metric, the system SHALL compare the match value with that unit's own baseline. The baseline metrics are:
- maximum and mean temperature, and the temperature rise rate;
- stator current P95;
- residual P95;
- stall time;
- isolated CAN gaps and frames missed per enabled minute;
- supply path resistance, where available.

The baseline SHALL be:
- the median of the unit's previous matches, up to a window (default 20);
- with a spread of 1.4826 × the median absolute deviation, floored at a per-metric minimum so a perfectly steady history cannot make every change infinite.

A unit with fewer than the minimum baseline matches (default 5) SHALL get no DRIFT or STEP anomalies, and the system SHALL report it as still building its baseline.

The system SHALL label the anomaly by the shape of the change:
- **STEP:** the last 3 matches all differ from the baseline by at least the step z-score (default 3.5), in the same direction. Onset is the first of them.
- **DRIFT:** otherwise, the median of the last 5 matches differs from the baseline of the matches before them by at least the drift z-score (default 2.5). Onset is the first match whose value left the baseline band in that direction.

A single-match outlier SHALL NOT produce an anomaly at normal sensitivity.

A unit history SHALL follow the unit, never the slot. A unit observed in a new slot keeps its baseline. A new unit in an old slot starts empty.

A STEP whose onset is the unit's first match at an event, when the unit played at an earlier event, SHALL be tagged with the event boundary (the previous and the new event). The tag SHALL NOT change its severity or sensitivity. Its explanation SHALL name both events, and the Anomalies view SHALL show the tag in the queue and the detail. A STEP that starts later in an event SHALL NOT be tagged.

#### Scenario: Sudden step
- **WHEN** a synthetic unit's maximum temperature is 45 ± 1 °C for 20 matches, then 55 ± 1 °C from match 21
- **THEN** after match 23 there is a STEP anomaly with onset match 21, and no DRIFT anomaly for that metric

#### Scenario: Slow drift
- **WHEN** a synthetic unit's stator current P95 is 40 ± 1 A for 15 matches, then rises by 0.6 A per match
- **THEN** a DRIFT anomaly exists by match 30, and no STEP anomaly exists for that metric

#### Scenario: One bad match
- **WHEN** a synthetic unit's history is steady except for one match at z = 8
- **THEN** no DRIFT or STEP anomaly exists at normal sensitivity

#### Scenario: Swapped-in unit
- **WHEN** a slot's unit is replaced at match 20 by a new serial, and the new unit runs 10 °C hotter than the old one did
- **THEN** no DRIFT or STEP anomaly is raised for the new unit before its fifth match, and the old unit's history is not used for it

#### Scenario: Change between events
- **WHEN** a synthetic unit's maximum temperature is 45 ± 1 °C for 10 matches at one event, then 55 ± 1 °C from its first match at the next event
- **THEN** there is one HIGH STEP anomaly with onset at the second event's first match, tagged with both events, its explanation names both events, and the Anomalies view shows an event-boundary tag on it

#### Scenario: Step inside an event
- **WHEN** the same step starts at the second event's third match
- **THEN** the STEP anomaly is HIGH and has no event-boundary tag

#### Scenario: Degrading connector
- **WHEN** a synthetic unit's supply path resistance is 20 ± 1 mΩ for 15 matches, then 35 ± 1 mΩ from match 16
- **THEN** after match 18 there is a STEP anomaly on supply path resistance with onset match 16

#### Scenario: Building baseline
- **WHEN** detection runs on a lake where a unit has played 3 matches
- **THEN** that unit has no DRIFT or STEP anomalies, and the detection summary counts it as building its baseline (3 of 5)

### Requirement: Detection budget
A detection run over one season of gold SHALL finish in under 5 seconds and under 400 MB peak memory on the reference laptop. This assumes 60 matches, 23 slots, brownout counts already known for every match, and the model detector off. Brownout counts SHALL be kept per match and recomputed only when that match's silver or the brownout thresholds change. Counting a match not yet counted SHALL add at most 0.5 seconds. Detection SHALL NOT hold more than one match's silver in memory at a time. The brownout count of a match SHALL equal the number of brownout markers Replay shows for it.

#### Scenario: Season budget
- **WHEN** detection runs a second time on a synthetic season of 60 matches × 23 slots with the model detector off
- **THEN** it finishes in under 5 seconds with peak memory under 400 MB

#### Scenario: Brownouts agree with Replay
- **WHEN** detection and the Replay build both run on corpus `2026-gacmp-q7` with a test configuration that sets the brownout threshold to 8.5 V and the sag threshold to 9.0 V (above Q7's lowest battery proxy voltage)
- **THEN** the brownout anomaly's actual count equals the number of brownout FAULT markers in Q7's Replay data
