## 1. Shared filter state in the shell

- [ ] 1.1 Write failing browser tests (`tests/web/test_browser.py`):
  - Event carries from Replay to History;
  - Robot carries from History to Anomalies;
  - Copied address restores the filters;
  - nav tab hrefs carry the current shared filters after a filter change, with no reload.

  Then add `SHARED`, `FP.filters()`, and `FP.setFilters()` to `shell.js`. Make `FP.link` merge the shared filters unless the caller sets a key, and rebuild the nav hrefs on `setFilters` (design §1). Verify the tests pass
- [ ] 1.2 Write failing tests for the defaults: Replay default becomes shared; History opened first; No default after a clear (an address with `season=all&robot=all&event=all` lists every match in Replay). Then route Replay's `initialFilters` through "no shared key in the address" only. Verify the tests pass

## 2. One filter bar

- [ ] 2.1 Write failing API tests (`tests/web/test_api.py`): `/api/filters` returns `combos`, the distinct (season, robot, event) triples, and a hostile value in a filter query returns empty with no error. Then add the combos query to `views/queries.py` and `api.py`. Verify the API tests pass
- [ ] 2.2 Write failing browser tests: Same bar on every view (order, "all" labels, band placement, CLEAR FILTERS); Season narrows robots on every view; Nothing to clear (the control is disabled); the unit chip shows only when set and removes the unit. Then build `FP.filterBar()` in `shell.js` with its classes in `shell.css` (no inline styles; the static lint test still passes). Verify the tests pass
- [ ] 2.3 Move Replay's filters from the left rail to the band through `FP.filterBar`, and keep the "hidden by filters" and "no matches" notes in the rail. Write the failing tests Unit filter not applied and Static export (filters work from `file://` with the band placement). Update the existing Replay filter tests' selectors (keep the `#f-<key>` ids). Verify the Replay browser tests pass in served and static modes
- [ ] 2.4 Move History's band to `FP.filterBar`: SEASON replaces RANGE, and TYPE, PHASE, SUBSYSTEM, and SLOT go after the divider, marked History-only. Selecting a table row sets the shared unit; selecting it again clears it. Write failing tests: Unit chosen in History reaches Anomalies; Unit set elsewhere; History-only filter stays on History; Clear on History. Verify the History browser tests pass
- [ ] 2.5 Write failing tests: Clear on History clears Replay; Clear removes the unit; Event with no built Replay data (the value is named, not dropped); Hostile value in the address (every view shows an empty, named result). Fix any view that resets or drops the value. Verify the tests pass

## 3. Anomaly filters

- [ ] 3.1 Write failing tests (`tests/web/test_api_anomalies.py`, with `anomaly_lake.py` fixtures for two events and two robots): Event filter; Robot filter; No longer detected (an orphan uses its onset match); Hostile filter value; the badge and `open_units` are unchanged by filters. Then add `season`, `robot`, and `event` to `AnomalyViews.queue()` (a match → season/event map from a parameterised distinct query) and to `/api/anomalies` (design §3). Verify the tests pass, and that a queue request on a season-sized synthetic lake answers in under 2 s
- [ ] 3.2 Wire the Anomalies view to `FP.filterBar`, replacing the `#unit-chip` group. Pass the shared filters to `/api/anomalies`. Write failing browser tests: Nothing passes the filters (with clear); Selected anomaly outside the event (detail stays, the queue says hidden); Badge ignores the robot filter. Verify `tests/web/test_browser_anomalies.py` passes

## 4. Rails: independent scroll and minimize

- [ ] 4.1 Write failing browser tests at 1440 × 900: Scroll the evidence, keep the queue; Scroll the match list, keep the tracks. At 700 px: Narrow window (the page scrolls as one). Then add the viewport-height layout above 1100 px to `shell.css` (design §4). Verify the tests pass, and check by hand at 1366 × 768 that every column reaches its end
- [ ] 4.2 Write failing browser tests: Minimize the Replay match list (the strip is labelled MATCHES, and the track width grows); Remembered per view; Keyboard (`aria-expanded` toggles, and focus stays on the control); No storage (stub `localStorage` to throw). Then add `FP.rail()` and the `.is-collapsed` styles, and use them for every rail in Replay, History, and Anomalies (design §5). Verify the tests pass, and that the existing keyboard-only Replay test still passes with rails expanded

## 5. Group anomalies by device

- [ ] 5.1 Write failing browser tests (fixture: HIGH STEP and LOW DRIFT on `u-12345`, MED on `u-67890`, two `comp` brownouts): Two anomalies on one unit (order, counts, and highest severity); Battery group; Collapse a group (keyboard-reachable, `aria-expanded`); Link keeps the grouping (the selected anomaly's group is expanded); Unknown group value. Then add the GROUP control and the grouped queue render to `views/anomalies.js` (design §6), with its classes in `shell.css`. Verify the tests pass
- [ ] 5.2 Update the Anomalies view's header comment (URL keys `group` and the shared filters), and the History and Replay header comments for the shared keys. Verify `tests/web/test_static_lint.py` passes

## 6. Docs and gate

- [ ] 6.1 Update docs/rewrite/06-roadmap.md (a dated note under the P4 and P5 follow-ups: shared filters, rails, grouping by device) and the README's view sections (one filter bar, CLEAR FILTERS clears everywhere, minimizing rails). Verify the links resolve
- [ ] 6.2 Rebase onto `rewrite` once PRs #42 and #43 merge. Run the full gate locally (ruff, mypy, unit, corpus, browser), then open a PR into `rewrite`. Verify CI is green on all jobs
- [ ] 6.3 [HUMAN] On the pit laptop, served on `~/flashpoint-lake`: pick event GACMP in Replay, then check History and Anomalies are on GACMP. Clear on Anomalies, and check Replay lists every match. Minimize each rail on each view and reload. Group the queue by device. Record anything that feels wrong in design.md
