## Why

History's trend chart puts every match on one axis, and the tick labels show only the match number (`qm12`, `e2`). Over a season you can't tell where one competition ends and the next starts, which is the question behind P4's exit test ("is FL-drive running hotter than last event?"). The Anomalies evidence chart already answers this: it names each competition where it starts and draws a labelled boundary line between them. History should do the same. This is a follow-up to roadmap phase **P4** (Views: History, docs/rewrite/06-roadmap.md §P4) and matches the boundary added to anomaly review in **P5**.

## What Changes

- **Boundary lines on the History chart.** Between the last match of one competition and the first match of the next, the trend chart draws a labelled vertical line, named for the new competition (for example `GACMP`). The first competition is named at the chart's start with no line. Same look as the anomaly evidence chart.
- **Competitions line above the chart.** History lists the chart's competitions in match order with their match counts, as Anomalies does: `COMPETITIONS · GADAL · 30 MATCHES → GACMP · 30 MATCHES`, or `COMPETITION · GADAL` when there is one.
- **Season in the name when needed.** When the chart spans more than one season, names carry the year (`2025 GACMP`) so a repeat event across seasons is not ambiguous. In a single season they don't.
- **Labels don't collide.** When two boundaries are too close for both labels, the later label is left off and its line kept. The competitions line still names every competition. This applies to the anomaly evidence chart too, since both use the same chart helper.
- **One shared helper.** The run-of-events logic and the event name format move from `anomalies.js` into the shell so both views use the same code.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `lifetime-trends`: adds a "Competition boundaries in History" requirement (boundary lines, the competitions line, the year in names across seasons, labels that don't collide).
- `anomaly-review`: "Competitions in the evidence" allows a boundary label to be left off when it would overlap the previous one; the line and the competitions list stay.

## Non-goals

- Shading alternate competitions or colouring lines by competition. The design canvas keeps amber lines on dark, and the dashed boundary already does the job.
- Boundaries between seasons or robots beyond what the event change already shows.
- Making a boundary label clickable (for example, to set the event filter). Possible later; the filter bar already does this.
- Boundaries on the device-table sparklines. They are too small to carry labels.
- Any change to the query service, the API, or the lake. `/api/trend` already returns each match's `event`.

## Pitfalls fixed

None from 03-pitfalls-and-performance.md.

## Impact

- `src/flashpoint/web/static/shell.js`: shared `FP.competitions(...)` and event name helpers; `FP.track` leaves off a mark label that would overlap the previous one.
- `src/flashpoint/web/static/views/history.js`: builds marks and the competitions line from `trend.matches`; adds `marks` and `competitions` to the `FP.historyChart()` test hook.
- `src/flashpoint/web/static/views/anomalies.js`: uses the shared helpers (no behaviour change).
- `tests/web/test_browser.py`: History browser tests on the existing synthetic season (30 GADAL, 30 GACMP matches). The existing anomaly competition tests must still pass.
- Docs: the README's History paragraph and the user guide in `docs/guide/` if they describe the chart.
