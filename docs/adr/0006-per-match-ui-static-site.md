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
