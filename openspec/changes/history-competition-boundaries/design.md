## Context

See proposal.md for why. The pieces already exist:

- `/api/trend` returns `matches: [{ match_key, t, event, robot }]` in time order (`Queries.matches`, `src/flashpoint/views/queries.py`). `event` is the match key's prefix (`2026gacmp`), so it is never empty.
- `FP.track` (`shell.js`) draws `marks: [{ x, label, line }]` as dashed vertical lines with a label at the top left of each line. The anomaly evidence chart uses it.
- `anomalies.js` holds `competitions(points)` (runs of consecutive events, `{ name, start, n }`) and `eventText` (drops the year, capitals). Both are private to that view.

This is a browser-only change. No query, API, lake or D1-D11 decision is touched; it stays inside the History view (D7, `flashpoint serve`) and the shared shell it uses with Replay (D6): vendored JS, offline. No full-log work, so no memory or time budget applies: the boundaries are one pass over the matches already on the x axis (at most a few hundred).

## Goals / Non-Goals

**Goals:**
- One implementation of "runs of competitions" and "competition name" used by History and Anomalies.
- History boundaries as specified in `specs/lifetime-trends/spec.md`.
- Crowded labels handled once, in `FP.track`, for both charts.

**Non-Goals:**
- Changing how either chart looks apart from the label rule and the year in names.
- Any server change.

## Decisions

### 1. Move the helpers into the shell

`FP.competitions(keys)` takes event keys in match order (strings like `2026gadal`) and returns `[{ event, name, start, n }]`. Runs are split on the **raw event key**, not the display name, so `2025gacmp` followed by `2026gacmp` is two runs. `name` is the display name: capitals without the year, or `YYYY CODE` when the keys span more than one year (year = the key's leading four digits). Anomalies passes `points.map(p => p.match_key.split('_')[0])`; History passes `trend.matches.map(m => m.event)`.

`FP.competitionMarks(runs)` returns `[{ x: start - 0.5, label: name, line: i > 0 }]`, and `FP.competitionsText(runs)` returns the "COMPETITION · …" / "COMPETITIONS · … → …" line. Both views call these, so the two can't drift.

*Alternative:* copy the 15 lines into `history.js`. Rejected: the text format is now specified in two places, and a copy would drift on the first edit. `eventText`'s other use in anomalies (the `EVENT BOUNDARY` badge, which formats `"2026gadal → 2026gacmp"`) keeps a local regex; it formats a different string and is not part of this change.

### 2. Label collision in `FP.track`

In `drawOver`, keep the right edge of the last drawn label. Before drawing a label, measure it with `ctx.measureText`; if `x + 4px` is left of the previous right edge plus a 6 px gap, skip the text but keep the line. The chart records the labels it drew on each draw (`drawnMarks`, a list of labels), exposed on the object `FP.track` returns, for tests.

*Alternative:* stagger labels onto a second row. Rejected for now: it eats plot height on a 318 px chart and only matters for a one-match competition next to another; the competitions line already names everything.

*Alternative:* do it in History only, by thinning marks before passing them. Rejected: the view doesn't know pixel widths; the chart does.

### 3. History wiring

In `renderChart`, after the empty-chart early return, compute `runs` from `trend.matches`, pass `marks: FP.competitionMarks(runs)` to `FP.track`, and fill a new `<p class="meta mb-2" id="competitions">` above `#chart` with `FP.competitionsText(runs)`. On the empty path, clear it. The marks are rebuilt on every `refresh()`, so filters, metric, and per-unit/per-slot changes follow automatically (the x axis is the same matches in both groupings).

The existing reference label (`REF 65 °C`) is drawn on the reference line, not at the top, so it doesn't collide with mark labels.

### 4. Test hooks

`FP.historyChart()` gains `marks` (what History passed) and `drawn` (the chart's `drawnMarks`). `FP.anomalyMarks()` stays as is. The crowded-label scenario is tested by mounting `FP.track` directly in the page (`page.evaluate`) with 100 points and marks at x = 49.5 and 50.5 on a narrow box, then reading `drawnMarks`; neither synthetic season has a one-match competition, and adding one to the shared fixture would disturb other tests.

The year scenario is tested with a direct `page.evaluate` of `FP.competitions` and `FP.competitionsText` on the five keys (`2025gaalb` ×2, `2026gadal` ×3) on the History page, plus the History chart's own marks on the synthetic season to show the single-year case drops the year. The synthetic History season is one year only, and building a second one for a string format isn't worth the fixture time.

## Risks / Trade-offs

- [`measureText` widths differ by font and DPR] → the collision rule only ever drops text, never lines, and the competitions line names everything, so a borderline miss costs a label, not information.
- [A season with many short events gives many dashed lines over dense data] → the lines are `amberDim`, 1 px, dashed, as on the evidence chart; the canvas look accepted that there.
- [Changing the anomaly chart's names when evidence spans two years] → only possible with a baseline across a season change; the existing anomaly tests (single year) keep their expected strings.

## Migration Plan

None. Frontend only; rollback is reverting the PR.
