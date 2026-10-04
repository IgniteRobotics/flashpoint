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
