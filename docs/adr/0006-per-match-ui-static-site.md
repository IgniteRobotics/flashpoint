# 0006. Per-match UI: static site generated from the lake

- Status: Proposed
- Date: 2026-10-04
- Refs: 05 §7, D6; pitfalls #25, #37, #56, #59

## Context
Events often have no internet. The power-tracking static Plotly site already works offline, but its payload is about 27 MB per match, it drops spikes, and it has injection bugs.

## Decision
Generate a static per-match site from silver and gold:
- at most 2 MB per match, with min/max envelope downsampling;
- vendored JS and escaped content;
- a bundled `flashpoint serve`.

Reuse the power-tracking UX.

## Consequences
- No server is needed at events, and the site can be shared as a folder.
- Interactivity is limited to what is pre-computed.

## Alternatives considered
- **Streamlit:** needs a server, and its shared-cache pitfalls (#35, #58) bit the team.
- **DuckDB-WASM over Parquet:** promising. Revisit if payload limits bite.

## Amendment (2026-10-05, P4)
Implemented as **Replay**, one view of the local app (ADR-0013), with a static export:
- `flashpoint report` builds per-match data incrementally from silver, gold, and meta; `flashpoint report --static OUT [--no-raw]` writes a folder that opens from file://.
- Envelopes of about **1000 buckets** (bucket width rounded up to 10 ms) with min, max, and mean, so spikes survive; temperature as change points; 3 significant figures. Q7 is about 1.07 MB; a 2 MB guard lowers the bucket count instead of dropping data. The fixed-10 ms plan in 05 §7 measured 20 MB for Q7 and was dropped.
- Data ships as **script files** (`data/<match_key>.js` calling `FP.register`), which load from file:// where `fetch()` does not (#59).
- **uPlot** (about 50 KB) replaces Plotly (3.5 MB). The canvas sets the UX; the power-tracking SPA is reference only.
- Raw logs are offered as downloads for AdvantageScope (ADR-0008); nothing is launched.

## Amendment (2026-10-08, Replay UX)
Replay zooms on a shared timeline window. The stored payload is unchanged apart from `season` in the index and `t0_us` (the lake time of `window.t0`) in each match file, so `REPORT_VERSION` is 2 and every match rebuilds once. Finer detail is **served only**: `GET /api/envelope/<match_key>?from=&to=` reads the built file's session and window, then bins that session's silver partition over the clamped window with the build's own envelope code (about 1000 buckets, never under 10 ms, min/max/mean, so spikes survive). It refuses unknown, repeated, non-finite, or out-of-match parameters, and answers `available: false` when silver is gone. Static exports keep the stored buckets and say "bucket resolution (N ms)"; a second, finer tier in each file was rejected (it breaks the 2 MB budget), and DuckDB-WASM stays the thing to revisit if served-only detail is not enough.
