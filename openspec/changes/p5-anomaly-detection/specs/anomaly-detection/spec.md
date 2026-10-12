## Purpose

Find units and robots whose match-level behaviour breaks a fixed limit, departs from their own history, or departs from their siblings, and explain each finding in terms a student can check, so the team can act on hardware before it fails.

## ADDED Requirements

### Requirement: Detection inputs and timing
Detection SHALL read only the lake's derived layers and configuration:
- gold features for the whole-match phase;
- CAN health per match;
- the battery proxy in silver, for brownouts;
- triage records.

It SHALL NOT read raw logs. Each match SHALL be judged only against matches that started before it, so adding a later match never changes how an earlier match was judged. Detection SHALL run automatically at the end of every derive that changed gold, and on demand with a `detect` command.

#### Scenario: Runs after derive
- **WHEN** corpus `2026-gacmp-q7` is ingested and derived into an empty lake
- **THEN** the anomalies table exists, and its build time is later than the derive's gold write

#### Scenario: No lookahead
- **WHEN** detection runs on a synthetic unit history of 30 matches, and again after 10 more matches are appended
- **THEN** every anomaly whose onset and last flagged match fall within the first 30 matches has the same id, onset, and evidence values in both runs

#### Scenario: Raw store absent
- **WHEN** detection runs on a lake whose raw folder has been removed
- **THEN** it completes and produces the same anomalies as with the raw folder present

### Requirement: Anomalies table
Detection SHALL write one anomalies table that is fully rebuildable from the derived layers, configuration, and triage. Each row SHALL hold:
- a stable id, derived from the subject, method, metric, and onset match, and unchanged when nothing those inputs depend on changes;
- the subject: a unit (with its current slot and robot), or a robot's `power` for brownouts;
- the method: SPEC, DRIFT, STEP, SIBLING, or MODEL;
- the metric and its unit;
- the severity: HIGH, MED, or LOW;
- the lowest sensitivity at which it shows: relaxed, normal, or strict;
- the onset match, the last flagged match, and the number of matches affected;
- the actual value, the expected value or band, and the confidence: Low, Medium, or High;
- an explanation and an evidence series (see below).

An episode SHALL start at the first flagged match of a subject, method, and metric. It SHALL continue through later flagged matches, and SHALL end after 3 consecutive unflagged matches of that subject. A flag after an episode ends SHALL start a new anomaly with a new id.

#### Scenario: Stable ids across runs
- **WHEN** detection runs twice on the same lake and configuration
- **THEN** both tables are row-identical

#### Scenario: Episode continues
- **WHEN** a synthetic unit is flagged by STEP in matches 21, 22, 23, and 25, but not in 24
- **THEN** there is one STEP anomaly for that unit with onset 21, last flagged 25, and 4 matches affected

#### Scenario: Episode ends
- **WHEN** a synthetic unit is flagged in matches 21–23, is not flagged in 24–26, and is flagged again in 27
- **THEN** there are two anomalies with different ids, onsets 21 and 27

### Requirement: SPEC limits
The system SHALL flag a match when a configured fixed limit is crossed. These limits are the defaults, and each is configurable:

| Metric | MED | HIGH |
|---|---|---|
| maximum temperature | ≥ 65 °C | ≥ 75 °C |
| mean temperature | ≥ 55 °C | — |
| stall time | ≥ 5 s | — |
| reboots | any reboot not while enabled | any reboot while enabled |
| longest isolated CAN gap | ≥ 0.25 s | ≥ 1 s |
| brownouts (battery proxy below the brownout threshold for the hold time; subject is the robot's `power`) | — | ≥ 1 |

A SPEC anomaly's severity SHALL be the highest severity reached in its episode. Its expected value SHALL state the limit. SPEC anomalies SHALL show at every sensitivity.

#### Scenario: Roller reboot
- **WHEN** detection runs on a synthetic copy of Johnson Q105, with a reboot while enabled on the intake roller leader after an 820 ms isolated gap
- **THEN** there is a HIGH SPEC anomaly for that unit's reboots, and a MED SPEC anomaly for its longest isolated gap, both with onset `2026johnson_qm105`

#### Scenario: Hot motor
- **WHEN** a synthetic unit's maximum temperature is 66 °C in one match
- **THEN** there is a MED SPEC anomaly with actual "66 °C" and expected "< 65 °C"

#### Scenario: Q7 brownouts
- **WHEN** detection runs on corpus `2026-gacmp-q7`
- **THEN** there is one HIGH SPEC brownout anomaly whose subject is `power` for robot `2026-comp`, and its actual value counts the brownout intervals in the match

### Requirement: Per-unit baselines (DRIFT and STEP)
For each unit and each baseline metric, the system SHALL compare the match value with that unit's own baseline. The baseline metrics are:
- maximum and mean temperature, and the temperature rise rate;
- stator current P95;
- residual P95;
- stall time;
- isolated CAN gaps and frames missed per enabled minute.

The baseline SHALL be:
- the median of the unit's previous matches, up to a window (default 20);
- with a spread of 1.4826 × the median absolute deviation, floored at a per-metric minimum so a perfectly steady history cannot make every change infinite.

A unit with fewer than the minimum baseline matches (default 5) SHALL get no DRIFT or STEP anomalies, and the system SHALL report it as still building its baseline.

The system SHALL label the anomaly by the shape of the change:
- **STEP:** the last 3 matches all differ from the baseline by at least the step z-score (default 3.5), in the same direction. Onset is the first of them.
- **DRIFT:** otherwise, the median of the last 5 matches differs from the baseline of the matches before them by at least the drift z-score (default 2.5). Onset is the first match whose value left the baseline band in that direction.

A single-match outlier SHALL NOT produce an anomaly at normal sensitivity.

A unit history SHALL follow the unit, never the slot. A unit observed in a new slot keeps its baseline. A new unit in an old slot starts empty.

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

#### Scenario: Building baseline
- **WHEN** detection runs on a lake where a unit has played 3 matches
- **THEN** that unit has no DRIFT or STEP anomalies, and the detection summary counts it as building its baseline (3 of 5)

### Requirement: Sibling comparison
A robot configuration MAY declare sibling groups of slots that do the same job. For each baseline metric and each match, the system SHALL compare each member's value with the median of the other members in that match. It SHALL standardise the difference by its own history, using the same median and spread rules as the per-unit baseline. A SIBLING anomaly SHALL be raised when the difference reaches the sibling z-score (default 3.0) in at least 3 of the last 5 matches in which the whole group played.

In a group of two, the anomaly SHALL be raised on the member whose own value is further from its own baseline, and SHALL name the other member as the peer. A match where any member is missing SHALL be skipped for that group.

#### Scenario: One drive motor runs hot
- **WHEN** four synthetic drive units match each other for 15 matches, then one runs 6 °C hotter than the other three in matches 16–20
- **THEN** that unit has a SIBLING anomaly with its peer named as the median of the other three, and the other three have none

#### Scenario: Whole group warms together
- **WHEN** all four synthetic drive units rise 6 °C together from match 16
- **THEN** no SIBLING anomaly is raised (a DRIFT or STEP anomaly may be)

#### Scenario: Missing member
- **WHEN** one member of a sibling group has no feature row in a match
- **THEN** that match is skipped for the group's sibling comparison, and detection does not error

### Requirement: Optional model detector
The system SHALL include a model detector, off by default. When it is enabled in configuration and its optional dependency is installed, it SHALL score each unit-match feature vector against all earlier unit-matches of the same subsystem. It SHALL flag the top-scoring fraction (default 1 %) once at least the minimum training size (default 200 unit-matches) is available.

MODEL anomalies SHALL be:
- at most MED severity;
- shown only at normal and strict sensitivity;
- explained by naming the three features furthest from the unit's baseline.

If it is enabled but its dependency is missing, detection SHALL warn once and skip it, never fail. It SHALL be deterministic for the same inputs.

#### Scenario: Off by default
- **WHEN** detection runs with the default configuration
- **THEN** no MODEL anomalies exist, and the optional dependency is not imported

#### Scenario: Enabled without the dependency
- **WHEN** the model detector is enabled and its dependency is not installed
- **THEN** detection completes with a warning naming the missing extra, and the other methods' anomalies are unchanged

#### Scenario: Deterministic
- **WHEN** the model detector runs twice on the same synthetic season
- **THEN** both runs flag the same unit-matches

### Requirement: Severity, confidence, and sensitivity
Severity SHALL be set as follows:
- SPEC: from its table.
- DRIFT, STEP, and SIBLING: from the largest z-score in the episode. HIGH at z ≥ 6, MED at z ≥ 4, LOW below that.

Confidence SHALL be Low when the baseline had fewer than 10 matches or the episode spans 1 match. It SHALL be High when the baseline had at least 20 matches and the episode spans at least 3. It SHALL be Medium otherwise.

Sensitivity SHALL control which anomalies show, never how they are computed:
- **normal** shows anomalies found at the default thresholds;
- **relaxed** hides LOW;
- **strict** adds anomalies found at the strict thresholds (default z-scores lowered by 1.0, persistence lowered to 2 matches).

#### Scenario: Strict-only finding
- **WHEN** a synthetic unit drifts to z = 2.0 for 5 matches
- **THEN** it has a LOW DRIFT anomaly whose lowest sensitivity is strict, and it is absent at normal

#### Scenario: Low confidence
- **WHEN** a STEP anomaly is raised on a unit with 7 baseline matches
- **THEN** its confidence is Low

### Requirement: Explanations and evidence
Every anomaly SHALL carry:
- a one-sentence title stating the change with numbers;
- a "why it was flagged" text naming the method and the comparison made;
- the evidence series: the metric per match for up to the 40 matches ending at the last flagged match, the expected band per match, the flagged matches, and for SIBLING the peer series;
- likely causes, taken from a team-editable catalogue keyed by method, metric, and optionally subsystem, and shown as suggestions;
- "seen this before": closed anomalies (resolved or dismissed) with the same metric and method on the same slot or the same unit, with their outcome text.

Text built from configuration or log values SHALL be stored as text, never as markup. An anomaly with no catalogue entry SHALL say no causes are recorded yet, rather than inventing one.

#### Scenario: Evidence for a step
- **WHEN** the synthetic sudden-step anomaly is read
- **THEN** its evidence has one point per match from match 1 to 23, a band of the baseline median ± the step threshold, and matches 21–23 marked as flagged

#### Scenario: Seen before
- **WHEN** a slot had a DRIFT anomaly on stator current P95 that was resolved with the outcome "wheel bearing replaced", and a new DRIFT on the same metric opens on that slot's next unit
- **THEN** the new anomaly's "seen this before" lists the earlier one with "wheel bearing replaced"

#### Scenario: No catalogue entry
- **WHEN** an anomaly's method and metric have no catalogue entry
- **THEN** its likely causes read "No causes recorded for this pattern yet"

### Requirement: Detection configuration
Thresholds, windows, persistence, the model detector switch, and the cause catalogue SHALL live in configuration files and be validated on load. Unknown keys, negative thresholds, and a strict threshold above its normal one SHALL be rejected with a message naming the file and key. Changing configuration SHALL change the anomalies on the next detection run without a re-derive.

#### Scenario: Bad key
- **WHEN** the detection configuration contains `drift_z_score = 2.5` (a misspelt key)
- **THEN** detection stops with an error naming the file and `drift_z_score`, and the previous anomalies table is left in place

#### Scenario: Raise a limit
- **WHEN** the stall-time limit is raised from 5 s to 10 s and detection runs again
- **THEN** stall anomalies below 10 s are gone, and no derive ran

### Requirement: Partial and malformed inputs
Detection SHALL skip rows it cannot judge and SHALL never fail the whole run for one bad row. Rows with a missing value for a metric SHALL be skipped for that metric only. Matches with low alignment confidence SHALL be judged, and their anomalies SHALL carry that flag. Sessions quarantined or marked incomplete-read upstream SHALL count toward baselines only through the gold rows they produced. A unit whose rows are all missing for a metric SHALL produce no anomaly for it.

#### Scenario: Missing residual
- **WHEN** a unit's gold rows have no residual (no motor constants)
- **THEN** it has no residual anomalies, and its temperature anomalies are unaffected

#### Scenario: Truncated corpus log
- **WHEN** detection runs on a lake built from the whole golden corpus, including the truncated and quarantined files
- **THEN** it completes without error, and the summary counts the rows it skipped by reason

### Requirement: Detection budget
A detection run over one season of gold SHALL finish in under 5 seconds and under 400 MB peak memory on the reference laptop. This assumes 60 matches, 23 slots, brownout counts already known for every match, and the model detector off. Brownout counts SHALL be kept per match and recomputed only when that match's silver or the brownout thresholds change. Counting a match not yet counted SHALL add at most 0.5 seconds. Detection SHALL NOT hold more than one match's silver in memory at a time. The brownout count of a match SHALL equal the number of brownout markers Replay shows for it.

#### Scenario: Season budget
- **WHEN** detection runs a second time on a synthetic season of 60 matches × 23 slots with the model detector off
- **THEN** it finishes in under 5 seconds with peak memory under 400 MB

#### Scenario: Brownouts agree with Replay
- **WHEN** detection and the Replay build both run on corpus `2026-gacmp-q7`
- **THEN** the brownout anomaly's actual count equals the number of brownout FAULT markers in Q7's Replay data
