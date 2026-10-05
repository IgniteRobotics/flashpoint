## Why

The Streamlit and PyGWalker viewer shares cached state across users (#35, #58). It loads entire tables and launches AdvantageScope on the server. The power-tracking static site works offline, but at about 27 MB of JSON per match it drops spikes and has injection bugs (#25, #37, #56, #59). Students and drive coaches need answers in two clicks at an event, with no internet. Roadmap phase **P4**.

## What Changes

The views follow Josh's design canvas, "Flashpoint UI Prototypes" (2026-10-05). P4 builds two of its boards, **Replay** and **History**, as **one local web app** started by `flashpoint serve`. They share a styling shell, so later views (Live, Pit, Anomalies) slot in without restyling.

- **Replay** (per match):
  - built from silver and gold;
  - tracks with a scrub cursor, a readout at the cursor, and rule-based event markers;
  - a compare overlay;
  - at most 2 MB of precomputed min/max envelopes per match;
  - escaped content and vendored JS;
  - an "Open in AdvantageScope" download of the raw logs.
  
  `flashpoint report --static` exports Replay as a folder that opens offline with no server.
- **History** (lifetime, per unit and per slot, across matches, events, seasons, and robots): served from a local read-only API, with filters run as DuckDB queries, not client-side.
- **Device panel**: odometry (hours, Wh, thermal cycles, stall time) and a lifeline of the slots that serial has occupied. Odometry counts **every** session, practice included, through a new per-session usage table (gold only covers match-keyed sessions)
- Check the stall-time threshold against the corpus (it reads 0 s for every Q7 motor)

## Capabilities

### New Capabilities
- `match-reports`: the Replay view, its envelope data and size budget, static export, compare overlay, rule markers, and the shared app shell
- `lifetime-trends`: the History view, by unit and by slot, over a local read-only query API
- `unit-odometry`: per-session usage, cumulative usage, and history of each physical unit

### Modified Capabilities
None.

## Non-goals

- Re-implementing AdvantageScope features (D8)
- Authentication or multi-tenant hosting
- Pit-server dashboards (cut 2026-10-05; revisit after P5)
- The design's Live and Pit boards (a later change: they read the robot, not the lake) and its Anomalies board (P5)
- Notes, swap logging, and resolve actions: P4 views are read-only
- Design metrics with no data source yet: pit spin-test current, faults per match, firmware events, batteries

## Impact

New `src/flashpoint/web/` (server, API, and front end) and `src/flashpoint/report/` (envelopes and static export), plus `notebooks/` for student exploration. Supersedes ADR-0007 (marimo lifetime UI; new ADR-0013). The power-tracking SPA (`archive/static-site`) is reference only; the canvas sets the UX. Supersedes `viz.py` and `gw_config.json` (removed by `retire-legacy-code` stage 2d).
