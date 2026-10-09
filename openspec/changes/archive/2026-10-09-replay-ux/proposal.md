## Why

Replay works, but it doesn't scale past one event. The match list is one long column grouped by event, with no way to narrow it. The timeline always shows the whole match, so a two-second brownout is a few pixels wide. This change implements **Replay UX**, a follow-up to P4 in `docs/rewrite/06-roadmap.md` ("Replay UX (S, follow-up to P4)"). It builds on D6 (Replay with a static export) and D8 (link to AdvantageScope, don't rebuild it), described in `05-target-architecture.md` §7 and ADR-0006.

## What Changes

- **Match list filters.** Season, robot, and event filters sit above the Replay match list. They combine, live in the page address with the rest of Replay's state, and default to the most recent event. They work the same way in a static export, over the matches the export contains. An empty result says so and offers to clear the filters.
- **Season in the match index.** Each index entry records its season, so the filter doesn't have to guess it from the event key. The report version goes up, so the next `flashpoint report` rebuilds the index.
- **Timeline zoom.** Zoom in and out with buttons, Ctrl/⌘ + scroll or a pinch on a track, and shift-drag to select a range. A reset control returns to the full match. All tracks, the marker strip, the phase bands, and the scrub cursor share one time window. The window is stored in the page address. Plain drag still scrubs.
- **Detail on zoom (served only).** In served mode, zooming past the match envelope's resolution fetches a finer envelope for the visible window from silver. It has the same min, max, and mean buckets, and spikes still survive. Static exports stop at bucket resolution and say so. Single-sample detail stays in AdvantageScope.

## Capabilities

### New Capabilities
- None.

### Modified Capabilities
- `match-reports`:
  - adds the requirements **Match list filters**, **Timeline zoom**, and **Windowed detail in served mode**;
  - modifies **Shareable view links** so that the filters and the zoom window are encoded in the page address.

## Impact

- `src/flashpoint/web/static/views/replay.js`: adds the filter bar, the zoom controls, and the shared x window. `shell.js` `FP.track` gains an x range that can be set and an optional shift-drag selection.
- `src/flashpoint/report/build.py`: adds `season` to the index summary and bumps `REPORT_VERSION`.
- `src/flashpoint/report/envelope.py`: windowed envelopes can be reused for any time range.
- `src/flashpoint/web/api.py`: adds a new read-only endpoint, `GET /api/envelope/<match_key>`, with explicit parameters. Unknown parameters are refused, as with the other endpoints.
- `tests/web/test_browser.py`, `tests/web/test_api.py`, `tests/report/test_build.py`, `tests/report/test_envelope.py`.
- `docs/rewrite/06-roadmap.md` and the ADR-0006 amendment: records Replay UX as done and adds the windowed-detail endpoint.
- No new dependencies.
- No pitfalls are fixed directly. Windowed detail must keep the spike guarantee that fixed #25, and filters must keep working from file:// (#59).

## Non-goals

- **Import status view.** That is a separate roadmap item.
- **Live and Pit views.**
- Free-text match search, saved filter presets, and filtering by match type or marker level.
- Changing the stored envelope budget (about 1000 buckets, at most 2 MB) or the static export's payload.
- Showing single samples in Replay. That stays in AdvantageScope (D8).
- Zooming the y axis, or zooming each track on its own.
- Filters or zoom in History. History keeps its own filters.
