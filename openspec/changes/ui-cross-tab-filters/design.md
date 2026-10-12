## Context

The app is one page of classic scripts with no build step (D6, D7, ADR-0013): `shell.js` owns the router, the `h()` builder, and URL state in the hash. Each view registers itself and reads `FP.state()` when it mounts. The CSP forbids inline styles, so every layout need is a class in `shell.css`. Three views exist:

- **Replay** (static and served): filters season, robot, and event in the browser over the match index, writes them to the hash (using `all` for "no filter"), and defaults to the latest event when the hash has none.
- **History** (served): six selects in the band (`season` labelled RANGE), applied as DuckDB predicates behind `/api/*`. `unit` is a *selection* (emphasised line plus right rail), not a row filter.
- **Anomalies** (served): `unit` is a real filter applied by `AnomalyViews.queue()` in Python over the anomaly table (hundreds of rows, not the feature table).

`.layout` is a wrapping flex row (`--rail-left: 220px`, `--rail-right: 320px`, main `560px` minimum), and the whole document scrolls. Nav tab links are bare `#view=<id>`, so filters are dropped on every view switch.

## Goals / Non-Goals

**Goals:**
- One owner for the shared filter state (the shell), so views can't drift apart again.
- One filter-bar builder used by all three views.
- Rail scroll and collapse handled once in the shell and CSS, not per view.

**Non-Goals:**
- No new frameworks or vendored JS. No build step.
- No change to how History queries the lake beyond the season combinations. Anomaly filtering stays in the anomaly view layer.

## Decisions

### 1. Shared filters live in the hash, and the shell carries them across views
`shell.js` gets `SHARED = ['season', 'robot', 'event', 'unit']` and two helpers: `FP.filters()` reads them from the hash, and `FP.setFilters(patch)` writes them. `FP.link(state)` merges in the current shared filters unless the caller sets that key, so every drill-through link and nav tab keeps them. The header's nav hrefs are rebuilt by `FP.setFilters`, because `setState` uses `replaceState` and fires no `hashchange`.

"No filter" is written as `all`, as Replay does today. An address with no shared key at all means "fresh": only then does Replay apply its default. History and Anomalies send `all` to the API as no value.

*Alternatives:* sessionStorage (rejected: addresses stop being shareable, and two windows would overwrite each other's filters); a cookie (same problems, plus it's sent to the server for no reason).

### 2. One filter-bar builder in the shell
`FP.filterBar({ options, combos, extras, notApplied, onChange })` returns the band contents: the SEASON, ROBOT, and EVENT selects, the UNIT chip, a divider, the view's extras, and CLEAR FILTERS. Season narrowing uses `combos`, the distinct (season, robot, event) triples:
- Replay builds them from the index entries (it works offline and static).
- History and Anomalies get them from `/api/filters`, which gains a `combos` list (`SELECT DISTINCT season, robot, event FROM features`: tens of rows).

Replay's filters move from the left rail to the band. The rail keeps the match list and the "hidden by filters" note.

Clearing calls `onChange` with every shared key set to `all` and every extra key set to null. Every view runs one re-render path for any filter change.

*Alternative:* each view keeps its own bar and follows a style guide. Rejected: this is how they drifted.

### 3. Anomaly filters run in `AnomalyViews.queue()`, with match facts from the lake
`queue()` gains `season`, `robot`, and `event`. It loads a `match_key → (season, event)` map once per request from `SELECT DISTINCT match_key, season, event FROM features` (a small result, parameterised) and keeps an item if its robot matches and any flagged key (or the onset key for an orphan) maps into the chosen season and event. `/api/anomalies` accepts the three keys. `badge()` and `open_units()` stay unfiltered.

This complies with #35: the feature table is never loaded whole. Only its distinct match keys are read, and the anomaly list is already read whole by design (P5).

*Alternative:* store season and event on each anomaly row at detection time. Rejected for now: it needs a detection schema change and a re-run for a filter that can be derived.

### 4. Viewport-height layout above one breakpoint
At `min-width: 1100px` (220 + 320 + 560), `body` is `height: 100vh`, `#view` is `flex: 1; min-height: 0`, `.layout` stops wrapping, and `.rail-l`, `.main`, and `.rail-r` each get `overflow-y: auto; min-height: 0`. Below 1100 px the current wrapping layout and page scroll are kept. The header and band never scroll because they sit outside `.layout`.

*Alternative:* `position: sticky` rails with page scroll. Rejected: a rail taller than the viewport can't be scrolled to its end without scrolling the page, which is the bug being fixed.

### 5. Rail collapse is a shell wrapper with per-view, per-side storage
`FP.rail(side, label, ...children)` builds the rail with a header minimize button (`aria-expanded`, `aria-controls`) and a collapsed strip (a button with a vertical label via `writing-mode`). Collapsed rails get `.is-collapsed` (`flex-basis: var(--rail-collapsed)`, 36 px). Their content gets `hidden`, so it leaves the tab order. The state is stored in `localStorage` under `flashpoint.rail.<view>.<side>`, with try/catch like the CRT and sensitivity keys. Charts already resize through `FP.track`'s `ResizeObserver`. Below the breakpoint, a collapsed rail shows as a one-line bar.

*Alternative:* a global setting per side. Rejected: Replay's left rail is the match list, which people keep open, while History's right rail is a detail panel they may want gone.

### 6. Grouping is client-side over the filtered queue
The queue response already carries `subject`, `subject_key`, `serial`, `robot`, `role`, `slot_id`, `severity`, and `last_start` for each item. Group key: `power:<robot>` for power subjects, otherwise `subject_key`. The label is the role of the group's most recently flagged item, plus its serial or "legacy id". The group order is computed from the items. The collapsed-group set lives in the view's `ui` object (not persisted). `group=device` is written to the hash.

*Alternative:* group on the server. Rejected: the server would gain a second response shape for data the browser already has.

### 7. Replay does not apply the unit filter
The static index has no per-match unit list. Adding one means a `REPORT_VERSION` bump and a full rebuild for a minor filter. Showing the chip as "not applied in Replay" keeps static and served behaviour the same.

No D1–D11 decision changes. No ADR is needed.

## Risks / Trade-offs

- [Replay's default leaks into History: opening Replay first narrows History to the latest event] → This is intended (the spec's "Replay default becomes shared"). The bar always shows it, and one press clears it.
- [Stacked on unmerged PRs #42 and #43, which touch the same view files] → Rebase onto `rewrite` after they merge, before opening this PR.
- [Existing browser tests assume Replay's filters are in `nav.rail-l` and History's season label is RANGE] → Update the selectors in the same tasks that move the controls. Keep the `#f-<key>` ids.
- [Viewport-height layout can clip content on short laptop screens (768 px tall)] → Each column scrolls, so nothing becomes unreachable. Check at 1366 × 768.
- [`/api/anomalies` now reads the feature table on every queue request] → A distinct over the match keys of a season-sized gold table takes milliseconds in DuckDB. Budget: the queue still answers in under 2 s, the History refresh budget.

## Migration Plan

UI and API only. There is no data migration, no `REPORT_VERSION` bump, and no detection re-run. Old addresses keep working: they hold the same keys, and History's `season` key is unchanged (only the label changes from RANGE to SEASON). Roll back by reverting the PR.
