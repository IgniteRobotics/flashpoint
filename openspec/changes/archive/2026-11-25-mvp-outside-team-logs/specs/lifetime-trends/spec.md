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
- temperature limits:
  - OK, WARN (a match maximum at or above the warning limit in range), or HOT (at or above the fault limit);
  - when the unit has no temperature value in range, it reads N/A, NEEDS PRO or NO DATA instead (see below);
- the unit's open anomalies at normal sensitivity: a MED or LOW anomaly counts as WARN, and a HIGH anomaly counts as HOT.

A unit with no temperature value in range SHALL read:
- **N/A** when Flashpoint tracks no temperature for its device model (a CANcoder has no temperature signal; a Pigeon 2's own IMU temperature is not tracked);
- **NEEDS PRO** when it is a device that reports temperature, and every hoot that carried it in range was not Pro-licensed;
- **NO DATA** otherwise (a device that should report temperature, on Pro-licensed hoots, with no value).

N/A, NEEDS PRO and NO DATA rank below OK. An open anomaly still raises any of them to WARN or HOT. For a temperature metric, a NEEDS PRO unit's latest value and its trend description SHALL read "not logged (needs Phoenix Pro)", and an N/A unit's SHALL read "temperature not tracked". None of the three SHALL count toward the "units warn/hot" tile, and none SHALL use the fault style.

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

#### Scenario: Gyro and encoders read N/A
- **WHEN** History is opened on a lake built from corpus `2026-gacmp-q7` with the 2026 robot configuration
- **THEN** the Pigeon 2 and every CANcoder read N/A, every Talon FX reads OK, WARN or HOT, and no unit reads NO DATA

#### Scenario: Non-Pro lake
- **WHEN** History is opened on a synthetic lake whose hoots are all non-Pro and carry no temperature, with maximum temperature selected
- **THEN** every motor unit reads NEEDS PRO with latest value "not logged (needs Phoenix Pro)", the gyro reads N/A, the "units warn/hot" tile reads 0, and the view raises no error

#### Scenario: Pro for part of the range
- **WHEN** a synthetic motor unit has Pro-licensed hoots with a 50 °C maximum in its first 5 matches and non-Pro hoots in its next 5
- **THEN** its health reads OK from the first 5 matches, and its trend line has gaps for the last 5

#### Scenario: Missing temperature on a Pro hoot
- **WHEN** a synthetic motor unit is on Pro-licensed hoots but has no temperature value in range
- **THEN** its health reads NO DATA

#### Scenario: Anomaly on a non-Pro unit
- **WHEN** a NEEDS PRO unit has an open MED STEP anomaly on stator current P95
- **THEN** its health reads WARN, and the "units warn/hot" tile counts it

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
