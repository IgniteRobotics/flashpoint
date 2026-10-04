# 0002. Transform engine: Polars (with DuckDB SQL)

- Status: Proposed
- Date: 2026-10-04
- Refs: 04 §3, D2; pitfalls #14, #31, #36

## Context
pandas object-dtype strings, row loops, and exact-key outer merges caused both the slowness and the wrong joins (#14, #36). The vendor-run PDS-H benchmark shows pandas about 90× slower than Polars streaming at SF-10.

## Decision
Use Polars (lazy, streaming) for transforms, with `join_asof` and an explicit tolerance for aligning signals. Use DuckDB SQL for queries and gold aggregation. pandas is allowed only at the edges (notebook display).

## Consequences
- Memory-bounded streaming over full logs is possible.
- The as-of join is first class.
- Smaller ecosystem than pandas, and students learn a new API. Porting power-tracking's analyzer means a rewrite, not a copy.

## Alternatives considered
- **pandas:** familiar, but too slow and too easy to get wrong.
- **DuckDB only:** viable, but Python-side numeric work reads more naturally in Polars.
