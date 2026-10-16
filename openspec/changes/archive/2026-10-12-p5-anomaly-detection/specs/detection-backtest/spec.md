## Purpose

Measure whether detection would have caught the team's known failures, how early, and how often it raises findings nobody cares about, using the lake's history and a hand-kept labels file, so thresholds are tuned on evidence rather than guesses.

## ADDED Requirements

### Requirement: Failure labels file
The team SHALL record known failures and repairs in one labels file. Each label SHALL give:
- a robot, and either a unit (serial or legacy unit id) or a slot;
- the match key at which it was seen, or a date;
- a kind: failure, degradation, or repair;
- a free-text note.

It MAY also give the metric it is expected to show in. The file SHALL be validated on load. Unknown keys, a label with neither unit nor slot, an unknown robot or slot, and a malformed match key or date SHALL be rejected with a message naming the label. The file SHALL ship seeded with the known cases:
- Johnson Q105: the intake roller leader rebooted after an 820 ms dropout (failure), and the intake-extension follower's isolated CAN gaps rose (degradation);
- GACMP Q7: brownouts coinciding with intake stalls (failure).

#### Scenario: Seeded labels load
- **WHEN** the shipped labels file is loaded with the shipped robot configurations
- **THEN** it yields at least the three seeded labels, each resolved to a robot and slot

#### Scenario: Bad label
- **WHEN** a label names slot `drive-xx`, which robot `2026-comp` does not have
- **THEN** loading fails with an error naming the label and `drive-xx`

### Requirement: No-lookahead replay
A `backtest` command SHALL replay detection over the lake's matches in start-time order. At each match, only matches that started at or before it are visible. It SHALL record every anomaly as it first appears, with the match at which it appeared. Its results SHALL equal the anomalies a live detection run would have produced at each point in time. It SHALL read only the lake's derived layers, configuration, and the labels. Triage records SHALL be ignored, so that a backtest measures detection, not people.

#### Scenario: Equals live detection
- **WHEN** a backtest runs over a synthetic season of 30 matches
- **THEN** the anomalies open after match 20 in the backtest equal those from a detection run on a copy of the lake holding only matches 1–20

#### Scenario: Triage ignored
- **WHEN** an anomaly is dismissed with "restart baseline", and the backtest runs
- **THEN** the backtest results are the same as before the dismissal

### Requirement: Backtest report
The backtest SHALL report, for the selected season or all seasons, and for each sensitivity:
- **for each label:** caught or missed, the anomaly that caught it, and the lead time in matches. A label is caught when an anomaly on its unit or slot (and on its metric, when given) is open at the label's match or within the 5 matches before it.
- **false positives:** anomalies that match no label, as a count per method and per 10 robot-matches.
- **coverage:** matches judged, unit-matches skipped by reason, and units that never left the building-baseline state.

The report SHALL be written as both a readable table and machine-readable data under the lake's report folder, and summarised on standard output. Two runs on the same inputs SHALL produce identical machine-readable data.

#### Scenario: Q105 caught
- **WHEN** the backtest runs on a lake holding the Johnson matches Q15, Q53, Q76, and Q105, with the seeded labels
- **THEN** the intake roller leader failure is caught at Q105 with lead time 0, by a SPEC reboot anomaly

#### Scenario: Degradation needs history
- **WHEN** the same backtest is read for the intake-extension follower degradation label
- **THEN** the label is reported missed, and the report gives the reason as "building baseline (3 of 5)"

#### Scenario: Reproducible
- **WHEN** the backtest runs twice on the same lake and configuration
- **THEN** the two machine-readable reports are byte-identical

### Requirement: Backtest robustness and budget
A label that refers to a match not in the lake SHALL be reported as not evaluable, never as missed. A lake with no gold SHALL produce an empty report and a message, not an error. A backtest over two seasons of gold SHALL finish in under 60 seconds and under 1 GB peak memory on the reference laptop. This assumes 120 matches, 23 slots, and the model detector off.

#### Scenario: Label outside the lake
- **WHEN** a label refers to `2025gacmp_qm19` and the lake holds only 2026 matches
- **THEN** that label is reported as not evaluable, and the other labels are scored

#### Scenario: Two-season budget
- **WHEN** the backtest runs on a synthetic two-season lake of 120 matches × 23 slots
- **THEN** it finishes in under 60 seconds with peak memory under 1 GB
