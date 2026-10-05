# 0013. Views: one local app in the canvas shell

- Status: Proposed
- Date: 2026-10-05
- Supersedes: [0007](0007-lifetime-ui-marimo.md)
- Refs: openspec change `p4-views-and-reports` (design.md), Josh's canvas "Flashpoint UI Prototypes" (2026-10-05), D6, D7, D8; pitfalls #35, #37, #56, #58, #59

## Context
The canvas designs five boards (Live, Replay, Pit, History, Anomalies) that share one shell: an amber-on-black CRT look, VT323 and IBM Plex type, and one navigation bar. marimo (ADR-0007) can't render that shell. Separate tools for per-match and lifetime views would give students two looks and two ways in.

## Decision
`flashpoint serve` runs **one local, read-only web app** with every view in one shell:
- stdlib `ThreadingHTTPServer` on 127.0.0.1 by default (`--host` shares it on the pit network with a warning); no web framework, no auth;
- a thin JSON API over parameterised DuckDB queries (`views/queries.py`) for History; it reads gold and the meta Parquet snapshots only, never the SQLite ledger;
- a plain-script front end (no build step, no Node): `tokens.css` and `shell.css` from the canvas, `shell.js` with a view registry, hash-routed URL state, a text-only DOM builder `h()`, and a uPlot wrapper; one file per view;
- vendored uPlot (MIT) and fonts (OFL), so nothing loads from the network.

A new view is one file in `web/static/views/` plus one `FP.view({...})` call. P4 ships Replay and History; Live and Pit (they read the robot) and Anomalies (P5) slot in later. Replay also exports as a static folder (ADR-0006, amended).

## Consequences
- One look and one navigation across all views; deep links (`#view=replay&m=…&t=…`) work across them.
- History needs the server (it queries the lake); the static export carries Replay only and hides History.
- Hand-written JavaScript is the area the team likes least; it is kept small (no framework, one DOM helper, uPlot for every chart) and covered by Chromium tests served and from file://.
- Students still get marimo for exploration: `notebooks/` holds an example over `views/queries.py`, but it isn't installed or required.

## Alternatives considered
- **marimo apps (ADR-0007):** Python-only, but can't render the designed shell, and the views would split across two tools.
- **Vite + React + FastAPI (the canvas handoff's proposal):** a build toolchain and a second runtime on every pit laptop; more than P4 needs.
- **Streamlit:** server-side shared state (#35, #58).
