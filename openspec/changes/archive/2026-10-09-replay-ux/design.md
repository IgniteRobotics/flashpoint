## Context

See proposal.md (Why) and the `match-reports` delta spec for the behaviour required.

The current state that shapes the approach:
- **Index:** `report/build.py` writes `data/matches.js` (`FP.index`). Each entry has `key`, `label`, `event`, `robot`, `start_utc`, `status`, alignment, marker counts, and `file`, but no season. A match data file already carries `season` and `session_id`. It doesn't carry the match start as an absolute lake time.
- **Fingerprint:** `REPORT_VERSION = 1` is part of every match's fingerprint, so bumping it rebuilds every match on the next `flashpoint report`.
- **Replay's x axis:** `views/replay.js` draws each track with `FP.track` (uPlot) over the match's own grid, `window.t0 + i·width`. The x scale is fixed to the full window.
- **Drag:** a pointer drag on a plot calls `onPick`, which is the scrub cursor.
- **Markers:** they are positioned with `u.valToPos(m.t, 'x')`, so they already follow the x scale.
- **Page address:** state lives in the hash (`FP.state`/`FP.setState`), one flat namespace shared by every view. History already uses `season`, `robot`, and `event`.
- **Static mode:** loads data through `<script>` tags (#59) and has no API.

Decisions in play:
- **D6, ADR-0006:** about 1000-bucket envelopes, at most 2 MB, with a static export.
- **D7, ADR-0013:** one local app, with a read-only DuckDB API under `serve`.
- **D8, ADR-0008/0014:** single-sample detail stays in AdvantageScope.

This change doesn't deviate from any of these, so it needs no new ADR. The ADR-0006 amendment gains one paragraph for the served-only windowed detail.

## Goals / Non-Goals

**Goals:**
- Filters and zoom are client-side over data the page already has, so they work identically from file://.
- One x window object owns the time axis, and every time-positioned element reads from it.
- Windowed detail reuses the build's envelope code. A zoomed track uses the same bucketing as a built one.

**Non-Goals:**
- No change to stored payloads beyond `season` in the index and `t0_us` in the match file.
- No caching layer on the server. No prefetching of neighbouring windows.
- The overlay match stays at its stored resolution when zoomed.

## Decisions

### 1. Filters run in the browser over the index; the URL keys are `season`, `robot`, `event`
- **How:** the filter bar is built from the distinct values in `FP.indexData.matches`. Filtering is a pure function, `(entries, filters) → entries`, so the static export gets it for free.
- **URL keys:** the keys are the same names History uses. Going from History to Replay carries the season and robot across, which is what a coach wants after a drill-through.
- **Defaults:** the default (most recent event, or the selected match's event) applies only when none of the three keys is in the address. History-supplied filters are kept, and the event stays "all" unless History set it.
- **Most recent event:** the event of the entry with the latest `start_utc`. Entries without a start time sort by key.
- **Alternative:** a server-side filter endpoint. Rejected: static mode has no API, and the index is small (one row per match).
- **Alternative:** prefixed keys (`rs`, `rr`, `re`). Rejected: they lose the History hand-off and give the same idea two spellings.

### 2. Season comes from the session, recorded in the index; `REPORT_VERSION` → 2
- **Change:** `_summary` adds `season`. The match payload adds `t0_us`, the absolute lake time of `window.t0` (needed by decision 4).
- **Version bump:** this forces one full rebuild. That fits the existing 5 s per match build budget: about 1 min for a 12-match event.
- **Old indexes:** an old index, or one whose `version` is below 2, disables the season filter. Its note reads "Rebuild with `flashpoint report` to filter by season". Robot and event still work.
- **Alternative:** parse the season from the event key (`2026gacmp` → 2026). Rejected: the spec forbids it, and off-season or unkeyed events can break it.

### 3. One shared x window; `FP.track` gets a settable range and an opt-in range select
- **Window state:** Replay holds `ui.view = {from, to}` in match seconds, clamped to the match window, at least 0.5 s wide. Every change goes through one `setView()`:
  - each chart gets `u.setScale('x', {min, max})`;
  - `placeMarkers()` reruns, hides markers outside the window, and updates the left and right hidden counts;
  - the track range readouts are recomputed over the visible indices;
  - the header label is updated, and so is `z` in the address (two decimals, omitted at full window).
- **`FP.track` additions** (generic, so History can use them later):
  - `setX(min, max)` on the returned handle;
  - `onSelect(x0, x1)`, fired by shift-drag. During the drag a translucent band is drawn in the `draw` hook. A drag without Shift still calls `onPick`, so scrubbing is unchanged;
  - `onZoom(factor, x)`, fired by `wheel` with `ctrlKey || metaKey` (Chromium and Firefox report a trackpad pinch this way) and by Safari `gesturechange`. Default scrolling is prevented only in those cases. Plain wheel scrolling still scrolls the page.
- **Zoom steps:** the buttons and the `+`/`-` keys zoom ×2 around the cursor, or around the window centre when no cursor is set. The wheel zooms by `exp(-deltaY·0.002)` around the pointer.
- **Y axis:** the y axis auto-ranges to the visible window, which is uPlot's default when the x scale is set. Reference lines still widen it.
- **Alternative:** uPlot's built-in drag-to-zoom (`cursor.drag.x`). Rejected: it takes plain drag away from scrubbing, and it is per-chart, so it would need a sync layer anyway.
- **Alternative:** a separate overview or brush strip. Rejected for size S. It can be added later, on top of `setView()`.

### 4. Windowed detail: `GET /api/envelope/<match_key>?from=<s>&to=<s>`
- **Validation:**
  - the server opens the built data file for the key (`data/<stem>.js`, path-contained as in `/api/match`) and reads `session_id`, `season`, `t0_us`, and the window;
  - it parses `from` and `to` as finite floats with `from < to`;
  - it clamps to the match window and refuses (400) anything that lies wholly outside it;
  - `_single` refuses unknown and repeated parameters;
  - an unbuilt key is a 404.
- **Envelope:** `envelope.window_between(start_us, end_us, buckets=BUCKETS)` gives the same 10 ms width step, with `match_start_us` from `t0_us`. The response reuses `slot_envelopes`, `battery_envelope`, `temperature_points`, and `to_payload` over the session's silver partition. It contains `{window, series, battery, temps, not_logged}`, the same shape and keys as the stored payload, so `trackData()` works unchanged on a merged view object.
- **Silver missing:** if the partition is missing, the response is a 200 with `{"available": false, "reason": ...}`. The header shows the reason.
- **Session scope:** only the primary session is read, as in the build. Multi-session matches already render the primary session's series.
- **Client:**
  - requests detail only when `to - from < 0.5 · full span`;
  - debounces by 150 ms and drops stale responses with a request counter;
  - keeps the last detail response while it still covers the view at no worse resolution;
  - always shows the stored buckets until the detail arrives.
- **Budget:** under 1 s per request and under 500 MB peak on the reference laptop for a 2 s Q7 window. DuckDB reads one session partition with a `t_us` range predicate, so the cost depends on the window and the partition, not the lake. A corpus test measures the budget. A window can be the whole match at most, so the worst case equals the existing build query, which is already under 5 s and 500 MB.
- **Alternative:** ship a second, finer envelope tier in each data file. Rejected: it breaks the 2 MB budget (ADR-0006: a fixed 10 ms grid measured 20 MB for Q7).
- **Alternative:** DuckDB-WASM over Parquet in the browser. Rejected: it adds a large vendored dependency for a served-only feature. ADR-0006 already lists it to revisit if payload limits bite.

### 5. The overlay stays at stored resolution
- The overlay is still resampled onto the primary's grid with `valueAt`. When the primary is detailed, the overlay shows step-held stored buckets in the window.
- The header's resolution note names both matches when they differ.
- Fetching overlay detail would double the requests for a rare case. It can be added later.

## Risks / Trade-offs

- **[Shared hash keys]** History filters leaking into Replay may surprise a user. → The filters are visible and clearable, and the defaults apply only when no filter key is present. This is the intended hand-off.
- **[Rebuild]** The version bump makes the first `flashpoint report` after upgrading rebuild every match. → The release note says so. The cost is bounded by the build budget.
- **[Pinch gestures]** Pinch detection differs by browser. Safari uses `gesture*` events, the others use ctrl+wheel. → Buttons and the keyboard are the primary path. Gestures are an extra. The browser tests cover ctrl+wheel only (Chromium).
- **[Fast zoom traffic]** Repeated wheel zooms could flood `serve`. → The client debounces and drops stale responses. Each request reads one partition. The server is local, single-user, and read-only.
- **[Detail mixed with stored data]** Detail buckets and the stored step-held overlay can look inconsistent. → The overlay style is already distinct (dashed), and the header names each resolution.

## Migration Plan

1. Merge. Users run `flashpoint report`. The version bump rebuilds every match and the index, which adds `season` and `t0_us`.
2. Re-export static folders with `flashpoint report --static` to get filters with season.
3. Rollback: revert the change. Indexes built at version 2 still load in version-1 code, because the extra keys are ignored.
