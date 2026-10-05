## Why

The Streamlit and PyGWalker viewer shares cached state across users (#35, #58). It loads entire tables and launches AdvantageScope on the server. The power-tracking static site works offline, but at about 27 MB of JSON per match it drops spikes and has injection bugs (#25, #37, #56, #59). Students and drive coaches need answers in two clicks at an event, with no internet. Roadmap phase **P4**.

## What Changes

- A **per-match static report** built from silver and gold:
  - at most 2 MB per match, using min/max envelopes;
  - escaped content, vendored JS, and a bundled `serve` command;
  - an "Open in AdvantageScope" download of the raw wpilog.
- A **lifetime trends app** (per slot and per unit, across matches, events, and robots). Filters run as queries rather than client-side filtering
- A **unit page**: odometry (hours, Wh, thermal cycles, stall time) and the slots that serial has occupied. Odometry counts **every** session, practice included, through a new per-session usage table (gold only covers match-keyed sessions)
- Check the stall-time threshold against the corpus (it reads 0 s for every Q7 motor)

## Capabilities

### New Capabilities
- `match-reports`: static, offline, per-match report contents, size budget, compare mode
- `lifetime-trends`: cross-match views by slot and by unit, plus filters
- `unit-odometry`: per-session usage, cumulative usage, and history of each physical unit

### Modified Capabilities
None.

## Non-goals

- Re-implementing AdvantageScope features (D8)
- Authentication or multi-tenant hosting
- Pit-server dashboards (cut 2026-10-05; revisit after P5)

## Impact

New `src/flashpoint/report/`, `notebooks/`. Reuses the power-tracking SPA UX (from `archive/static-site` and `origin/feature/power-tracking`). Supersedes `viz.py` and `gw_config.json` (removed by `retire-legacy-code` stage 2d).
