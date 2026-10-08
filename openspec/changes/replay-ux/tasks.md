## 1. Season and absolute start in the build (spec: match-reports, "Match list filters"; design 2)

- [x] 1.1 Write the tests first in `tests/report/test_build.py`, and confirm they fail:
  - each index entry carries `season` equal to its session's season;
  - each match payload carries `t0_us`, and `t0_us − window.t0·1e6` equals the match start (`t0_us` is the lake time of `window.t0`, per design 2);
  - the index `version` is 2;
  - a lake built at version 1 rebuilds every match on the next build. The incremental test still passes, for a second build with no change.
- [x] 1.2 Add `season` to `_summary`, add `t0_us` to the match payload, and bump `REPORT_VERSION` to 2. 1.1 passes, as do `tests/report` and `tests/test_cli_report.py`. The Q7 payload stays at most 2 MB.

## 2. Envelopes over any window (spec: match-reports, "Windowed detail in served mode"; design 4)

- [x] 2.1 Write the tests first in `tests/report/test_envelope.py`:
  - `window_between(start_us, end_us)` gives about 1000 buckets with widths a whole 10 ms, and never narrower than 10 ms;
  - a 2 s window holds the 4 ms 150 A spike at max 150 A in the right bucket;
  - each bucket's min and max equal those of the silver samples in its range (synthetic silver);
  - temperature points carry the last reading before the window in at its start.
- [x] 2.2 Implement `window_between` in `report/envelope.py`, and factor the build's series assembly into a function that both the build and the API call. 2.1 passes, and `tests/report` stays green with byte-identical Q7 output (compared against a pre-change build in the test).

## 3. `GET /api/envelope/<match_key>` (spec: match-reports, "Windowed detail in served mode")

- [x] 3.1 Write the tests first in `tests/web/test_api.py`:
  - a valid window returns `{window, series, battery, temps, not_logged}`, with keys matching the stored payload;
  - each of these is a 400 with a one-line reason: `from >= to`, a window wholly outside the match, a non-finite value, an unknown or repeated parameter;
  - an unbuilt key is a 404;
  - a path-traversal key returns no file;
  - with the silver partition removed, the response is `available: false` with a reason.
- [x] 3.2 Implement the route in `web/api.py`. It reads the built data file header (contained path) and queries only that session's silver partition. 3.1 passes, and so does `tests/web/test_server.py`.
- [x] 3.3 Add a corpus budget test in `tests/web/test_api_corpus.py`: a 2 s Q7 window answers in under 1 s with peak memory under 500 MB, and its buckets match silver min and max. Run it with the golden-corpus marker, and record the measured numbers in the PR.

## 4. `FP.track` range, select, and zoom gestures (spec: match-reports, "Timeline zoom"; design 3)

- [x] 4.1 Write the browser tests first in `tests/web/test_browser.py` against a synthetic match:
  - shift-drag on a track fires a selection with the dragged range;
  - a plain drag still moves the cursor and leaves the range unchanged;
  - ctrl+wheel over a track zooms around the pointer, while a plain wheel scrolls the page and does not zoom;
  - `setX` moves the x scale, and the y axis re-ranges to the visible data.
- [x] 4.2 Implement `setX`, `onSelect` (with a drawn selection band), and `onZoom` (ctrl/meta+wheel and Safari `gesturechange`) in `shell.js`. 4.1 passes, and the History browser tests stay green.

## 5. Match list filters (spec: match-reports, "Match list filters")

- [x] 5.1 Extend the synthetic season fixture (`tests/views/season.py`) to cover two seasons, two robots, and three events, with known start times. Then write the browser tests first, one per spec scenario:
  - the default is the most recent event;
  - filters combine, and the event list narrows by season;
  - nothing matches, and the clear control works;
  - the selected match is filtered out;
  - an unknown value in the address is named;
  - an index older than version 2 disables the season filter with the rebuild note;
  - season and robot carry over from a History link.
- [x] 5.2 Implement the filter bar in `views/replay.js`: a pure `filterEntries()` function, the defaults, and the empty and hidden-selection notices. The controls are real `<select>`s and buttons, reachable by keyboard with a visible focus ring. 5.1 passes, and the keyboard-only test still reaches every match.
- [x] 5.3 Static export: extend `test_static_lint` / the export browser test so that filtering a two-event export from file://, with the network blocked, lists only the chosen event and makes no request. The test passes.

## 6. Timeline zoom in Replay (spec: match-reports, "Timeline zoom")

- [x] 6.1 Write the browser tests first:
  - shift-drag from T+95 to T+99 s puts every track and the marker strip on that window, the brownout marker has the same x position on each track, and the header reads "T+95.0 – T+99.0 s";
  - zoom-in, zoom-out, and reset work with the buttons and with the `+`, `-`, and `0` keys;
  - the window clamps to the full match window and to 0.5 s;
  - hidden markers are counted on each side, and selecting an off-window marker pans the window to it;
  - track range readouts describe the visible window;
  - the overlay follows the window.
- [x] 6.2 Implement `ui.view` and `setView()` in `views/replay.js`: charts, `placeMarkers()` with hidden counts, range readouts, the header label, and the zoom buttons. 6.1 passes, and the existing Replay browser tests (scrub, markers, compare) stay green.
- [x] 6.3 Add the static resolution note: when zoomed past the stored bucket width, the header reads "bucket resolution (N ms)". A browser test against a static export passes.

## 7. Served detail on zoom (spec: match-reports, "Windowed detail in served mode")

- [x] 7.1 Write the browser tests first against a served synthetic lake:
  - zooming to 2 s triggers one envelope request after the debounce, and the tracks redraw with a narrower bucket width;
  - a burst of wheel zooms leaves only the last response applied;
  - with silver removed, the tracks keep the stored buckets and the header says finer data is unavailable;
  - zooming back out past half the match uses stored data with no request.
- [x] 7.2 Implement the fetch in `views/replay.js`: a 150 ms debounce, the stale-response counter, and a merged view object so that `trackData()` and `valueAt()` work unchanged. The overlay stays at stored resolution, and the header names both resolutions. 7.1 passes.

## 8. Shareable links (spec: match-reports, "Shareable view links")

- [x] 8.1 Write the browser tests first:
  - copying the address with robot `comp` and window T+95 – T+99 s, then opening it in a new page, restores both;
  - `z=300,20` opens on the full window;
  - the existing restore-from-link test still passes.
- [x] 8.2 Write the filters and `z` into `FP.setState` (omit `z` at the full window) and read them in `mount()`. 8.1 passes.

## 9. Docs and full verification

- [ ] 9.1 Update `docs/rewrite/06-roadmap.md`: mark Replay UX done, and note the windowed-detail endpoint and the overlay-at-stored-resolution limit. Add one paragraph to the ADR-0006 amendment on served-only windowed detail and `REPORT_VERSION` 2. Update the `05-target-architecture.md` §7 Replay notes if they describe the fixed x axis. Check that the docs render, and that their links resolve.
- [ ] 9.2 Run the whole suite before the PR: `pytest`, `pytest -m browser`, the golden-corpus e2e, `ruff`, and `mypy --strict`. All must pass. Paste the summary into the PR. Then check by hand on the corpus lake: find Q7 by year, robot, and event in two clicks, and zoom to the T+97 s brownout.
