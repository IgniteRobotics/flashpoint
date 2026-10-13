## Why

Each view filters on its own. Replay has season, robot, and event in its left rail. History has six selects in its top band. Anomalies has only a unit chip. Narrowing to one event in Replay and switching to History shows all time again, so students re-pick the same filters on every view and can't tell which view is filtered. On a pit laptop the side rails also scroll with the page: scrolling the Anomalies evidence moves the queue off screen, and the rails take width that the charts need.

This is follow-up UX work to roadmap P4 (docs/rewrite/06-roadmap.md, the Replay and History phase, and the replay-ux follow-up) and P5 (the Anomalies view), built to the Flashpoint UI canvas. It extends the rule from pitfall #35: filters are applied where the data is read, including the new Anomalies filters. It fixes no other numbered pitfall.

## What Changes

- **Shared filters.** Season, robot, event, and unit become one filter state for the whole app. Setting one on any view applies it on every view, and moving between views keeps it. One CLEAR FILTERS control clears them on every view. The state lives in the page address, so links keep working.
- **One filter bar.** Every view shows the same filter bar in the same place, the band under the header, with the same controls, order, labels ("All seasons", …), and clear button. Replay's filters move from the left rail to this bar. History's extra filters (match type, phase, subsystem, slot) stay in the bar after a divider and apply only to History.
- **Anomalies gains season, robot, and event filters.** An anomaly passes if its robot matches and at least one of its flagged matches is in the chosen season and event. The server applies these filters, not the browser.
- **Replay and the unit filter.** The static match index doesn't record which units played each match, so Replay shows an active unit filter as "not applied in Replay" instead of narrowing the list differently in served and static modes.
- **Independent scrolling.** On wide screens, the header and filter bar stay put, and the left rail, the main area, and the right rail each scroll on their own. On narrow screens, where the rails wrap, the page scrolls as today.
- **Minimizable rails.** Each side rail has a control that collapses it to a narrow strip at its edge and expands it again. The main area takes the width. The choice is remembered per view and side on this machine.
- **Group anomalies by device.** The Anomalies queue gets a GROUP control: none (today's list) or by device. A device is the physical unit (serial). Power anomalies group per robot's battery. Each group shows its device, count, and highest severity, and can be collapsed. The choice is kept in the address.

## Non-goals

- No change to which anomalies detection produces, to the queue's OPEN, RESOLVED, and DISMISSED tabs, or to sensitivity. Sensitivity is already shared through local storage.
- No grouping by slot or subsystem. No grouping in History's device table.
- No per-match unit inventory in the static index, so Replay doesn't apply the unit filter.
- No keyboard shortcuts for collapsing rails. No resizable (draggable) rails.
- No new views, and no restyle beyond the filter bar and rail controls.

## Capabilities

### New Capabilities
- `view-filters`: the shared filter state (season, robot, event, unit), how each view applies it, the shared filter bar, clearing, defaults, and how it is kept in the address.

### Modified Capabilities
- `match-reports`: the shared app shell gains independent rail scrolling and minimizable rails, and hosts the filter bar. Replay's match list filters follow `view-filters` and move out of the rail. Shareable links carry the shared filters.
- `lifetime-trends`: History's filters use the shared bar and state. Its extra filters stay History-only.
- `anomaly-review`: the Anomalies view takes the shared filters, applied by the query service, and gains grouping by device.

## Impact

- Frontend: `src/flashpoint/web/static/shell.js` (filter state, filter bar, rail collapse, links that carry filters), `shell.css` (viewport-height layout, rail scroll, collapsed rail), and `views/replay.js`, `views/history.js`, `views/anomalies.js`.
- API: `/api/anomalies` accepts `season`, `robot`, and `event`. `/api/filters` returns which robot and event values occur in each season, so every view can narrow robot and event to the chosen season.
- Python: `src/flashpoint/views/anomalies.py` (queue filters, which look up flagged matches' season and event in the lake) and `views/queries.py` (season combinations).
- Tests: browser tests for each view's filters, the shared state, clearing, rails, and grouping. API tests for the new anomaly filters.
- Stacked on `feature/battery-proxy` (PR #43), which carries the P5 spec archive (PR #42). Rebase onto `rewrite` once those merge.
