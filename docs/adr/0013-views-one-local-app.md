# 0013. Views: one local app in the canvas shell

- Status: Proposed
- Date: 2026-10-05
- Amended by: [0014](0014-guarded-advantagescope-launch.md) (the AdvantageScope launch), [0015](0015-anomaly-triage-writes.md) (triage writes: the app is read-only except for appended triage rows), [0016](0016-live-view-robot-connection.md) (Live CAN: an outbound, subscribe-only robot connection), [0017](0017-config-writes-and-setup-actions.md) (configuration saves and setup actions), and the openspec change `team-branding` (the look is set per team; see Consequences)
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
- *Amended 2026-11 (team-branding):* the look is now set per team. `theme.toml` holds the team name and a palette (a preset, or custom colours). `theme.css` (served by `flashpoint serve`, or written into a static export) overrides the role-named colour tokens in `tokens.css` (`--accent*`, `--warn`, the surfaces), so every view still shares one look. Amber is only the default. The header and page title show the team name and number. A view also needs its own `<script>` tag in `index.html`, marked `data-needs-api` when it needs the server, which the static export uses to leave it out.

## Alternatives considered
- **marimo apps (ADR-0007):** Python-only, but can't render the designed shell, and the views would split across two tools.
- **Vite + React + FastAPI (the canvas handoff's proposal):** a build toolchain and a second runtime on every pit laptop; more than P4 needs.
- **Streamlit:** server-side shared state (#35, #58).
