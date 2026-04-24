# Static Site Generator — Design Spec

**Date:** 2026-04-23
**Status:** Approved

## Overview

Replace (or supplement) the PDF report with a persistent static site that accumulates match data over time. Each CLI run adds one match to the site. The browser app lets you view any single match or compare any two matches side-by-side using interactive Plotly charts.

---

## Architecture

```
python -m utils match.hoot --site ./site
```

The Python side serializes match data to JSON and updates a manifest. The browser side is a static SPA that fetches those files — no server required.

**Output directory structure:**
```
site/
  index.html          ← SPA shell (written once on first run, never regenerated)
  manifest.json       ← updated on every run: [{id, duration, n_motors}, ...]
  data/
    2024-FIN-Q1.json  ← full match data per run
    2024-FIN-Q2.json
    ...
```

**Per-run flow:**
1. CLI loads `.hoot` → existing `hoot_loader` / `analyzer` pipeline (unchanged)
2. New `utils/site_builder.py` serializes the `Match` object to JSON
3. `manifest.json` is rewritten from the full contents of `site/data/` (no stale entries)
4. If `index.html` does not exist, it is written from a bundled template
5. CLI prints the site path

---

## Data Model

Each `data/<match_id>.json`:

```json
{
  "match_id": "2024-FIN-Q1",
  "duration": 148.2,
  "timestamps": [0.0, 0.02, ...],
  "motors": {
    "device-1": {
      "name": "FR Drive",
      "motor_voltage": [...],
      "stator_current": [...],
      "motor_power": [...],
      "supply_voltage": [...],
      "supply_current": [...],
      "supply_power": [...],
      "rotor_velocity": [...],
      "device_temp": [...],
      "motor_energy": [...],
      "stats": {
        "peak_motor_w": 312.4, "p95_motor_w": 280.1,
        "avg_motor_v": 11.8,   "max_motor_v": 12.4,
        "max_stator_a": 38.2,  "avg_stator_a": 12.1,
        "max_supply_v": 12.6,  "avg_supply_v": 12.1,
        "max_supply_a": 18.2,  "avg_supply_a": 8.4,
        "peak_supply_w": 220.5,"p95_supply_w": 198.2,
        "avg_temp": 48.2,      "max_temp": 61.1
      }
    }
  },
  "totals": {
    "motor_power": [...],
    "supply_power": [...],
    "supply_current": [...],
    "motor_energy": [...],
    "supply_energy": [...]
  }
}
```

- Null/unavailable fields (e.g. `supply_current` when not present) are omitted entirely
- Arrays are plain JSON lists of floats
- Stats are pre-computed by `site_builder.py` from the `Match` object
- Size estimate: ~3–5 MB per match at 50 Hz / 150s / 8 motors

---

## CLI Changes

`--output` (PDF) and `--site` are independent flags. Both can be used together.

```bash
# Site only
python -m utils match.hoot --site ./site

# PDF only (existing behavior, unchanged)
python -m utils match.hoot --output report.pdf

# Both
python -m utils match.hoot --site ./site --output report.pdf
```

**Rules:**
- `--output` still defaults to `report.pdf` when neither flag is given (backwards compatibility)
- Re-running on the same match ID overwrites `data/<match_id>.json`
- `manifest.json` is always rebuilt from disk contents — never partially updated
- `index.html` is only written if it does not already exist

All existing flags (`--motor-names`, `--threshold`, `--cache-dir`) apply to both output modes.

---

## SPA Behavior

**On load:** Fetches `manifest.json`, populates the "+ Add match" picker. No match selected by default.

**Match chips:**
- Up to 2 matches active at a time. Adding a third replaces the oldest.
- Chip colors: blue (first match), teal (second match) — applied consistently to table headers, chart series, and labels throughout.
- Removing a chip clears that match from all tabs immediately.

**Single-match vs comparison mode:**

| Element | 1 match | 2 matches |
|---------|---------|-----------|
| Stats table | Full-width | 50/50 split |
| Energy summary | Single column | Both columns |
| Tab 3 power charts | Single chart | Side-by-side |
| Tab 3 supply current overlay | Hidden | Shown below |
| Tab 4 W/RPS | Single match grids | Both matches stacked |

---

## Tab Content

| Tab | Content |
|-----|---------|
| **Stats & Tables** | Per-match motor stats table (all columns: motor V, stator A, motor W, supply V, supply A, supply W, avg/max temp with heat coloring) + match energy summary table |
| **Motor & Supply Charts** | Motor power, supply power, stator current, supply current heatmaps — side-by-side per match |
| **Total Power** | Total robot power chart (motor + supply) per match; supply current overlay comparison (2-match only) |
| **Supply W/RPS** | Per-motor W/RPS time-series subplots, per match, stacked |

**Excluded from site (vs PDF):** Cumulative power/energy graphs.

---

## New Module: `utils/site_builder.py`

Responsibilities:
- `serialize_match(match, motor_names) -> dict` — converts `Match` to the JSON schema above
- `write_match(match_dict, site_dir)` — writes `data/<match_id>.json`
- `update_manifest(site_dir)` — scans `data/` and rewrites `manifest.json`
- `ensure_index(site_dir)` — writes `index.html` from bundled template if not present

The bundled `index.html` template lives at `utils/templates/index.html` and is read at runtime by `ensure_index`. Plotly.js loaded from CDN.

**Visual style:** Black background (`#000` or near-black). The team logo at `media/logo.png` is displayed in the top bar. All chart backgrounds and page background match.

---

## Files Changed

| File | Change |
|------|--------|
| `utils/site_builder.py` | New — serialization + site management |
| `utils/templates/index.html` | New — SPA shell with Plotly charts |
| `utils/cli.py` | Add `--site` flag, route to `site_builder` |
| `utils/__init__.py` | No change |
| `utils/reporter.py` | No change |
| `utils/plotter.py` | No change |
