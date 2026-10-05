# 0007. Lifetime UI: marimo apps over DuckDB

- Status: Superseded by [0013](0013-views-one-local-app.md) (2026-10-05)
- Date: 2026-10-04
- Refs: 04 §3, D7

## Context
Lifetime trends per slot and per unit need ad-hoc filtering across seasons. They should also be easy for students to extend.

## Decision
Build marimo notebooks, deployable as apps, that query gold with DuckDB SQL. Filters are pushed down into the queries.

## Consequences
- Pure-Python, git-friendly files that can be reviewed in PRs. Runs locally offline.
- Needs a Python runtime to view (unlike the static site).

## Alternatives considered
- **Grafana + DuckDB plugin:** good for alerting and dashboards, but needs a glibc Linux server and an unsigned plugin.
- **Streamlit:** see ADR-0006.
