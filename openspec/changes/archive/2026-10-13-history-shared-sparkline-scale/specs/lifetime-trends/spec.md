## MODIFIED Requirements

### Requirement: Device table and summary tiles
Below the chart, History SHALL list every unit in range with:
- its current slot (role) and serial;
- its in-service span;
- the selected metric's latest value and change over the range;
- a small trend line;
- a health status.

Every row's trend line SHALL be drawn on one vertical scale shared by all listed rows for the selected metric, so rows can be compared at a glance. The scale SHALL start at zero when no value in range is negative, and otherwise run from the lowest to the highest value in range. A match where the unit has no value for the metric (for example, temperature not logged) SHALL be a gap in its line and SHALL NOT widen the scale. A unit with fewer than two values in range SHALL show no line. Each trend line's description SHALL give the unit's own lowest and highest value in range and the shared scale, with the metric's unit.

Health SHALL be the worse of two parts:
- temperature limits: OK, WARN (a match maximum at or above the warning limit in range), or HOT (at or above the fault limit);
- the unit's open anomalies at normal sensitivity: a MED or LOW anomaly counts as WARN, and a HIGH anomaly counts as HOT.

In served mode, a unit with open anomalies SHALL show their count and highest severity in its row, linking to the Anomalies view filtered to that unit. Above the chart, tiles SHALL show units tracked, matches in range, total powered-on hours in range, and units with a WARN or HOT status.

#### Scenario: Health from temperature
- **WHEN** a unit's match maximum temperature reached 66 °C once in the range
- **THEN** its health reads WARN, and the "units warn/hot" tile counts it

#### Scenario: Health from an anomaly
- **WHEN** a unit stays under every temperature limit but has an open HIGH SIBLING anomaly
- **THEN** its health reads HOT, its row shows 1 HIGH anomaly linking to the Anomalies view, and the "units warn/hot" tile counts it

#### Scenario: Closed anomalies do not count
- **WHEN** a unit's only anomaly is dismissed
- **THEN** its health is computed from temperature alone

#### Scenario: Rows share one scale
- **WHEN** the selected metric is a count, one unit's values in range are 3, 0, 1, 3 and another's are 7, 20, 1, 42
- **THEN** both trend lines are drawn on the scale 0 to 42, the first line stays in the bottom tenth of its cell while the second reaches the top, and the first line's description reads its own range 0 to 3 and the shared scale 0 to 42

#### Scenario: Negative values in range
- **WHEN** the selected metric's values in range run from −2 to 5
- **THEN** every trend line is drawn on the scale −2 to 5

#### Scenario: Unit missing values in some matches
- **WHEN** a unit has no value in two matches in range and its other values are 10 to 12, and the highest value of any unit is 30
- **THEN** its line has gaps at those two matches, and the shared scale is 0 to 30

#### Scenario: One value in range
- **WHEN** a unit has a single value in range
- **THEN** its trend cell shows no line, its description still gives its value, and the shared scale is unchanged
