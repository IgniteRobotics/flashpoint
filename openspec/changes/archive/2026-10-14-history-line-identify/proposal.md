## Why

History's main chart draws every unit's line in the same colour with no labels, so the only way to find out which line spiked is to select units one at a time in the table (issue #46, split out of #39). Worse, with no unit selected, the hover readout names whichever unit comes first at the hovered match, not the line under the pointer, so it can point at the wrong unit. This is a follow-up to roadmap phase **P4** (Views: History, docs/rewrite/06-roadmap.md §P4), whose exit question ("is FL-drive running hotter than last event?") assumes you can tell lines apart.

## What Changes

- **Hover identifies a line.** At the hovered match, the line nearest the pointer is drawn emphasised, and the readout names its unit (or slot, in per-slot mode), its value with the metric's unit, its slot, and its alignment.
- **No arbitrary names.** When no line is near the pointer, the readout gives the selected unit's value at that match, or only the match and time when no unit is selected.
- **Click selects a line.** In per-unit mode, selecting another unit's line selects that unit, exactly as clicking its table row does. Selecting the selected unit's line, or a spot near no other line, still opens that match in Replay on the unit's slot, as today.
- **Per-slot drill-through.** In per-slot mode, selecting a line opens that match in Replay with that slot as a track. Today this does nothing beyond the readout, although the drill-through requirement already says any plotted point opens Replay.
- No new colours: the chart keeps the amber CRT look from the design canvas, with dim lines and one emphasised line.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `lifetime-trends`: "History trend lines" gains hover identification and line selection; "Drill-through to Replay and raw logs" names how a chart selection picks between selecting a unit and opening Replay.

## Non-goals

- Distinct colours per line or a legend. With 20+ units, no categorical palette stays readable, and it would break the canvas's look.
- Always-on direct labels (for example, the top 3 lines). Considered and left out: placing labels without collisions is its own piece of work. Revisit if hover proves too slow in the pit.
- Keyboard navigation between chart lines. The chart stays an image whose values are in the table, and the table's unit buttons are the keyboard path.
- The NO DATA health for units with no temperature (#47).
- Any change to the query service, the API, or the device table.

## Pitfalls fixed

None from 03-pitfalls-and-performance.md; this fixes issue #46.

## Impact

- `src/flashpoint/web/static/shell.js`: `FP.track` can draw one highlighted series over the others.
- `src/flashpoint/web/static/views/history.js`: finds the nearest line on hover and on pick, emphasises it, writes the readout, and routes a pick to selecting a unit or opening Replay.
- `tests/web/test_browser.py`: browser tests on the existing synthetic season (History fixture).
- No API, query, or lake change. Closes #46.
