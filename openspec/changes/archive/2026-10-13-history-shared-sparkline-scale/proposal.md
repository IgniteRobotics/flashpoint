## Why

History's device table scales each unit's TREND sparkline to that unit's own range, so every row looks like a climb: a count wiggling 0 → 3 fills its cell exactly like one jumping 1 → 42 (issue #39, found during the can-health hand check on the Johnson lake). A drive coach scanning the table for "which unit is getting worse" is misled by the one column meant to answer it at a glance. This is a follow-up to roadmap phase **P4** (Views: History, docs/rewrite/06-roadmap.md §P4), fixing what the P4 table shipped with.

## What Changes

- Every sparkline in the device table is drawn on **one y-scale shared by all rows** for the selected metric and the current filters.
- The shared scale starts at **zero** when no value in range is negative (temperatures, counts, hours, resistance); otherwise it runs from the lowest to the highest value in range.
- The trend cell's description (its tooltip) gives **the unit's own range and the shared scale**, with the metric's unit, replacing the fixed "This unit's own range".
- Missing values (a match where the unit has a row but no value, such as temperature not logged) stay gaps and do not stretch the scale; a unit with fewer than two values shows no line, as today.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `lifetime-trends`: "Device table and summary tiles": the trend line is drawn on a scale shared by every row, and its description gives the unit's own range.

## Non-goals

- The two main-chart problems also noted in #39, filed as their own issues: lines on the History chart are all one amber with no labels, and health reads NO DATA for units with no temperature (gyro, encoders).
- The sparkline's x-axis: each line still spans its cell over that unit's own matches. A unit with 4 matches and one with 100 both fill the width.
- Log scales or outlier clipping. One unit with an extreme value flattens the others; that is the comparison the table should show, and the unit's own range stays in its description.
- Any change to the LATEST and CHANGE columns, the main chart, or the query service's payload.

## Pitfalls fixed

None from 03-pitfalls-and-performance.md; this fixes issue #39.

## Impact

- `src/flashpoint/web/static/shell.js`: `FP.spark` takes an optional fixed range.
- `src/flashpoint/web/static/views/history.js`: the device table computes the shared range from the rows it renders and writes each cell's description.
- `tests/web/`: a History fixture with two units on very different ranges, and browser tests for the shared scale and the description.
- No API, query, or lake change. Closes #39.
