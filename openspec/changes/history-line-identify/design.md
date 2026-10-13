## Context

History's chart is one `FP.track` (a uPlot wrapper) with one series per unit or slot, every non-selected line in the dim line colour and the selected unit in bright amber. The track reports the hovered match index (`hover(idx)`) and the picked x (`onPick(x, event)`), but nothing about y. So `showHover` and `pickMatch` in `history.js` fall back to "the first point at this match" when no unit is selected; that is the arbitrary name the proposal describes. The browser already holds every plotted value per series (`bySeries`), so finding a line needs no query. See proposal.md for why.

## Goals / Non-Goals

**Goals:** one rule for "the line at the pointer", shared by hover and pick; emphasis that does not rebuild the chart; behaviour that works with a mouse, a trackpad, and a tap on a tablet in the pit.

**Non-Goals:** emphasising more than one hovered line; changing the track's zoom, drag-select, or wheel handling (History uses none of them).

## Decisions

1. **Nearest line by vertical pixel distance at the hovered match, within 10 px.** At the match index the cursor snaps to, compare the pointer's y with each series' point at that index, in canvas pixels (`valToPos`), and take the closest within 10 CSS px. The pixel threshold is the same on every metric and zoom, where a value threshold would not be. If the selected unit's point ties for nearest (lines overlap), the selected unit wins, so overlapping lines never pull a click away from the unit being followed. Alternative considered: uPlot's built-in series focus (`cursor.focus.prox`), which dims the others. Rejected: it uses its own proximity rule, which `onPick` would have to copy, and it changes the alpha of every series, where the canvas look keeps them all dim.

2. **`FP.track` gains `highlight: () => ({ values, stroke, width }) | null`.** The highlight is drawn in the existing `draw` hook over the series, the same pattern as `cursor()`. A hover change calls `redraw()`, not `setData`, so the chart is not rebuilt per pointer move. For y, hover reads the cursor's top offset (`u.cursor.top`) and a pick reads it from the event that `onPick` already passes, so the track needs no y callback. Other callers (Replay, anomaly evidence) don't pass `highlight` and are unchanged.

3. **Pick routing lives in History.**
   - Per unit, the nearest line is another unit's: select that unit (the same `selectUnit` path as the table).
   - Per unit, the nearest line is the selected unit's, or no line is near and a unit is selected: open Replay on that match and slot (today's behaviour).
   - Per slot, the nearest line: open Replay with that slot as the track.
   - Otherwise, show the readout only.

   Only the pointer-down pick acts; the track's drag-repeat picks (`pointermove` while held) are ignored, so a press never selects one unit and then navigates.

4. **The readout follows the same rule as the emphasis.** The nearest line, else the selected unit's point, else only the match and time. One function feeds both, so the emphasised line and the named unit can never disagree.

5. **Test hook.** `FP.historyChart()` gains `at(series, index)`, returning the page coordinates of a plotted point. Browser tests hover and click real points without copying the chart's scale maths.

No D1–D11 decision is touched (view rendering only). No logs are read; per pointer move the work is one pass over at most a few dozen series at one index, far under a frame.

## Risks / Trade-offs

- [Dense clusters: 20+ lines within a few °C sit within 10 px of each other] → the nearest one wins and is named, so the user sees exactly which unit they are on; tighter clusters need zooming the range (filters), as today.
- [A tap on a tablet selects a unit rather than showing a readout first] → intended: selecting is reversible (tap the row or line again), and the readout updates on the tap.
- [Changing a click's meaning when a unit is selected] → kept narrow: Replay still opens unless the click is clearly on another unit's line.
