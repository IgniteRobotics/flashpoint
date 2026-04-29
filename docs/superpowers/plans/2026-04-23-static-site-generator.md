# Static Site Generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `--site` flag to the CLI that serializes match data to JSON and builds a persistent static SPA with interactive Plotly charts, accumulating one match per run.

**Architecture:** A new `site_builder.py` module handles serialization of `Match` objects to JSON and manages the site directory (manifest, index). The SPA (`utils/templates/index.html`) is a self-contained HTML file with embedded JS that fetches match JSON files and renders four tabbed views using Plotly.js. The existing PDF pipeline (`reporter.py`, `plotter.py`) is untouched.

**Tech Stack:** Python 3.11+, numpy, Plotly.js (CDN), vanilla JS (no framework), pytest

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `utils/site_builder.py` | Create | Serialize Match→JSON, write files, manage manifest |
| `utils/templates/index.html` | Create | Full SPA: chips, 4 tabs, Plotly charts |
| `utils/cli.py` | Modify | Add `--site` flag, route to site_builder |
| `tests/utils/test_site_builder.py` | Create | Unit tests for serialization and file management |

---

## Task 1: Match Serialization

**Files:**
- Create: `utils/site_builder.py`
- Create: `tests/utils/test_site_builder.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/utils/test_site_builder.py
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from utils.models import Match, MotorData
from utils.site_builder import serialize_match


def _make_motor(n: int = 10, has_supply: bool = True, has_temp: bool = True) -> MotorData:
    t = np.linspace(0, 2, n)
    return MotorData(
        motor_voltage=np.full(n, 12.0),
        stator_current=np.full(n, 10.0),
        motor_power=np.full(n, 120.0),
        motor_energy=np.linspace(0, 0.1, n),
        supply_voltage=np.full(n, 12.5) if has_supply else None,
        supply_current=np.full(n, 8.0) if has_supply else None,
        supply_power=np.full(n, 100.0) if has_supply else None,
        supply_energy=np.linspace(0, 0.08, n) if has_supply else None,
        rotor_velocity=np.full(n, 5.0) if has_supply else None,
        device_temp=np.full(n, 50.0) if has_temp else None,
    )


def _make_match() -> Match:
    motors = {
        "TalonFX-1": _make_motor(),
        "TalonFX-2": _make_motor(has_supply=False, has_temp=False),
    }
    totals = _make_motor()
    return Match(
        match_id="2024-FIN-Q1",
        timestamps=np.linspace(0, 2, 10),
        motors=motors,
        totals=totals,
    )


def test_serialize_match_top_level_fields():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    assert result["match_id"] == "2024-FIN-Q1"
    assert abs(result["duration"] - 2.0) < 0.01
    assert len(result["timestamps"]) == 10
    assert isinstance(result["timestamps"][0], float)


def test_serialize_match_motor_arrays_present():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    m1 = result["motors"]["TalonFX-1"]
    assert len(m1["motor_power"]) == 10
    assert len(m1["supply_current"]) == 10
    assert len(m1["device_temp"]) == 10


def test_serialize_match_optional_arrays_omitted_when_none():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    m2 = result["motors"]["TalonFX-2"]
    assert "supply_current" not in m2
    assert "supply_power" not in m2
    assert "device_temp" not in m2


def test_serialize_match_motor_name_used_when_provided():
    match = _make_match()
    names = {"TalonFX-1": "FR Drive", "TalonFX-2": "FL Drive"}
    result = serialize_match(match, motor_names=names)
    assert result["motors"]["TalonFX-1"]["name"] == "FR Drive"


def test_serialize_match_motor_id_used_as_name_when_no_map():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    assert result["motors"]["TalonFX-1"]["name"] == "TalonFX-1"


def test_serialize_match_stats_computed():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    stats = result["motors"]["TalonFX-1"]["stats"]
    assert abs(stats["peak_motor_w"] - 120.0) < 0.01
    assert abs(stats["avg_motor_v"] - 12.0) < 0.01
    assert abs(stats["avg_temp"] - 50.0) < 0.01


def test_serialize_match_stats_temp_none_when_no_temp():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    stats = result["motors"]["TalonFX-2"]["stats"]
    assert stats["avg_temp"] is None
    assert stats["max_temp"] is None


def test_serialize_match_stats_supply_none_when_no_supply():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    stats = result["motors"]["TalonFX-2"]["stats"]
    assert stats["peak_supply_w"] is None
    assert stats["max_supply_a"] is None


def test_serialize_match_totals_present():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    assert len(result["totals"]["motor_power"]) == 10
    assert len(result["totals"]["supply_power"]) == 10


def test_serialize_match_is_json_serializable():
    match = _make_match()
    result = serialize_match(match, motor_names=None)
    # Must not raise
    json.dumps(result)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /path/to/flashpoint
pytest tests/utils/test_site_builder.py -v 2>&1 | head -20
```
Expected: `ModuleNotFoundError` or `ImportError` for `site_builder`

- [ ] **Step 3: Implement `serialize_match()`**

```python
# utils/site_builder.py
from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np

from .models import Match, MotorData


def _arr(a: np.ndarray | None) -> list[float] | None:
    return a.tolist() if a is not None else None


def _motor_stats(data: MotorData, is_total: bool = False) -> dict:
    has_supply = data.supply_power is not None
    has_temp = data.device_temp is not None and not is_total
    return {
        "peak_motor_w":  float(data.motor_power.max()),
        "p95_motor_w":   float(np.percentile(data.motor_power, 95)),
        "avg_motor_v":   float(data.motor_voltage.mean()),
        "max_motor_v":   float(data.motor_voltage.max()),
        "max_stator_a":  float(data.stator_current.max()),
        "avg_stator_a":  float(data.stator_current.mean()),
        "max_supply_v":  float(data.supply_voltage.max())   if has_supply else None,
        "avg_supply_v":  float(data.supply_voltage.mean())  if has_supply else None,
        "max_supply_a":  float(data.supply_current.max())   if has_supply else None,
        "avg_supply_a":  float(data.supply_current.mean())  if has_supply else None,
        "peak_supply_w": float(data.supply_power.max())     if has_supply else None,
        "p95_supply_w":  float(np.percentile(data.supply_power, 95)) if has_supply else None,
        "avg_temp":      float(data.device_temp.mean()) if has_temp else None,
        "max_temp":      float(data.device_temp.max())  if has_temp else None,
    }


def _serialize_motor(motor_id: str, data: MotorData, name: str) -> dict:
    d: dict = {
        "name": name,
        "motor_voltage":  data.motor_voltage.tolist(),
        "stator_current": data.stator_current.tolist(),
        "motor_power":    data.motor_power.tolist(),
        "motor_energy":   data.motor_energy.tolist(),
        "stats": _motor_stats(data),
    }
    for attr in ("supply_voltage", "supply_current", "supply_power", "supply_energy",
                 "rotor_velocity", "device_temp"):
        val = getattr(data, attr)
        if val is not None:
            d[attr] = val.tolist()
    return d


def serialize_match(
    match: Match,
    motor_names: dict[str, str] | None,
) -> dict:
    """Convert a Match to a plain JSON-serializable dict."""
    motors = {
        mid: _serialize_motor(mid, data, (motor_names or {}).get(mid, mid))
        for mid, data in match.motors.items()
    }

    totals: dict = {
        "motor_power":  match.totals.motor_power.tolist(),
        "motor_energy": match.totals.motor_energy.tolist(),
    }
    for attr in ("supply_power", "supply_current", "supply_energy"):
        val = getattr(match.totals, attr)
        if val is not None:
            totals[attr] = val.tolist()

    return {
        "match_id":   match.match_id,
        "duration":   float(match.timestamps[-1]),
        "timestamps": match.timestamps.tolist(),
        "motors":     motors,
        "totals":     totals,
    }
```

- [ ] **Step 4: Run tests and confirm they pass**

```bash
pytest tests/utils/test_site_builder.py -v
```
Expected: all 10 tests PASS

- [ ] **Step 5: Commit**

```bash
git add utils/site_builder.py tests/utils/test_site_builder.py
git commit -m "feat: add site_builder serialize_match"
```

---

## Task 2: Site File Management

**Files:**
- Modify: `utils/site_builder.py`
- Modify: `tests/utils/test_site_builder.py`
- Create: `utils/templates/` (directory)

- [ ] **Step 1: Create the templates directory**

```bash
mkdir -p utils/templates
touch utils/templates/.gitkeep
```

- [ ] **Step 2: Add failing tests for file management**

Append to `tests/utils/test_site_builder.py`:

```python
from utils.site_builder import ensure_index, update_manifest, write_match


def test_write_match_creates_json_file(tmp_path: Path):
    match = _make_match()
    data = serialize_match(match, motor_names=None)
    write_match(data, tmp_path)
    expected = tmp_path / "data" / "2024-FIN-Q1.json"
    assert expected.exists()
    loaded = json.loads(expected.read_text())
    assert loaded["match_id"] == "2024-FIN-Q1"


def test_write_match_overwrites_existing(tmp_path: Path):
    match = _make_match()
    data = serialize_match(match, motor_names=None)
    write_match(data, tmp_path)
    write_match(data, tmp_path)  # second write — must not raise
    assert (tmp_path / "data" / "2024-FIN-Q1.json").exists()


def test_update_manifest_lists_all_data_files(tmp_path: Path):
    (tmp_path / "data").mkdir()
    for name in ("2024-FIN-Q1", "2024-FIN-Q2"):
        (tmp_path / "data" / f"{name}.json").write_text(
            json.dumps({"match_id": name, "duration": 148.0, "n_motors": 8})
        )
    update_manifest(tmp_path)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    ids = {m["id"] for m in manifest}
    assert ids == {"2024-FIN-Q1", "2024-FIN-Q2"}


def test_update_manifest_entry_fields(tmp_path: Path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "2024-FIN-Q1.json").write_text(
        json.dumps({"match_id": "2024-FIN-Q1", "duration": 148.2, "motors": {"a": {}, "b": {}}})
    )
    update_manifest(tmp_path)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    entry = manifest[0]
    assert entry["id"] == "2024-FIN-Q1"
    assert abs(entry["duration"] - 148.2) < 0.01
    assert entry["n_motors"] == 2


def test_ensure_index_writes_html(tmp_path: Path, monkeypatch):
    # Patch the template path to a minimal HTML string
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    ensure_index(tmp_path)
    assert (tmp_path / "index.html").exists()
    assert "TEMPLATE" in (tmp_path / "index.html").read_text()


def test_ensure_index_does_not_overwrite(tmp_path: Path, monkeypatch):
    template_path = tmp_path / "tpl.html"
    template_path.write_text("<html>TEMPLATE</html>")
    monkeypatch.setattr("utils.site_builder._TEMPLATE_PATH", template_path)
    monkeypatch.setattr("utils.site_builder._LOGO_PATH", None)
    (tmp_path / "index.html").write_text("EXISTING")
    ensure_index(tmp_path)
    assert (tmp_path / "index.html").read_text() == "EXISTING"
```

- [ ] **Step 3: Run tests to confirm they fail**

```bash
pytest tests/utils/test_site_builder.py::test_write_match_creates_json_file -v
```
Expected: `ImportError` — `write_match` not defined yet

- [ ] **Step 4: Implement file management functions**

Append to `utils/site_builder.py`:

```python
_TEMPLATE_PATH: Path | None = Path(__file__).parent / "templates" / "index.html"
_LOGO_PATH: Path | None = Path(__file__).parent.parent / "media" / "logo.png"


def write_match(match_dict: dict, site_dir: Path) -> None:
    data_dir = site_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out = data_dir / f"{match_dict['match_id']}.json"
    out.write_text(json.dumps(match_dict))


def update_manifest(site_dir: Path) -> None:
    data_dir = site_dir / "data"
    entries = []
    for f in sorted(data_dir.glob("*.json")):
        d = json.loads(f.read_text())
        entries.append({
            "id":       d["match_id"],
            "duration": d.get("duration", 0.0),
            "n_motors": len(d.get("motors", {})),
        })
    (site_dir / "manifest.json").write_text(json.dumps(entries))


def ensure_index(site_dir: Path) -> None:
    site_dir.mkdir(parents=True, exist_ok=True)
    index = site_dir / "index.html"
    if index.exists():
        return
    if _TEMPLATE_PATH and _TEMPLATE_PATH.exists():
        shutil.copy(_TEMPLATE_PATH, index)
    if _LOGO_PATH and _LOGO_PATH.exists():
        shutil.copy(_LOGO_PATH, site_dir / "logo.png")
```

- [ ] **Step 5: Run all site_builder tests**

```bash
pytest tests/utils/test_site_builder.py -v
```
Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add utils/site_builder.py utils/templates/ tests/utils/test_site_builder.py
git commit -m "feat: add site_builder file management"
```

---

## Task 3: CLI `--site` Flag

**Files:**
- Modify: `utils/cli.py`

- [ ] **Step 1: Add `--site` argument and routing**

In `utils/cli.py`, add the import at the top with other imports:

```python
from . import trimmer, analyzer, reporter, hoot_loader, site_builder
```

Add the argument after the `--output` argument (around line 27):

```python
    parser.add_argument(
        "--site", type=Path, default=None, metavar="DIR",
        help="Output site directory (accumulates matches; creates dir if needed)",
    )
```

Replace the final section of `main()` (the `build_report` call, lines 86-88) with:

```python
    names_config = _names.load(args.motor_names) if args.motor_names else None

    if args.site is None and args.output == Path("report.pdf"):
        # default behavior: write PDF
        print(f"Building report → {args.output}")
        reporter.build_report(matches, args.output, per_motor=args.per_motor_graphs, names_config=names_config)
        print("Done.")
        return

    if args.output != Path("report.pdf") or (args.site is None and args.output != Path("report.pdf")):
        print(f"Building report → {args.output}")
        reporter.build_report(matches, args.output, per_motor=args.per_motor_graphs, names_config=names_config)
        print("Done.")

    if args.site is not None:
        for match in matches:
            mn = _names.resolve(match.match_id, names_config) if names_config else None
            match_dict = site_builder.serialize_match(match, mn)
            site_builder.write_match(match_dict, args.site)
        site_builder.update_manifest(args.site)
        site_builder.ensure_index(args.site)
        print(f"Site updated → {args.site / 'index.html'}")
```

- [ ] **Step 2: Simplify the routing logic**

The above logic is hard to follow. Replace the entire final block with this cleaner version:

```python
    names_config = _names.load(args.motor_names) if args.motor_names else None

    build_pdf = args.site is None or args.output != Path("report.pdf")
    if build_pdf:
        print(f"Building report → {args.output}")
        reporter.build_report(matches, args.output, per_motor=args.per_motor_graphs, names_config=names_config)
        print("Done.")

    if args.site is not None:
        for match in matches:
            mn = _names.resolve(match.match_id, names_config) if names_config else None
            match_dict = site_builder.serialize_match(match, mn)
            site_builder.write_match(match_dict, args.site)
        site_builder.update_manifest(args.site)
        site_builder.ensure_index(args.site)
        print(f"Site updated → {args.site / 'index.html'}")
```

- [ ] **Step 3: Smoke-test the CLI help**

```bash
python -m utils --help
```
Expected: `--site DIR` appears in the output

- [ ] **Step 4: Commit**

```bash
git add utils/cli.py utils/site_builder.py
git commit -m "feat: add --site flag to CLI"
```

---

## Task 4: SPA Shell — Structure, CSS, Chip Management, Tab Switching

**Files:**
- Create: `utils/templates/index.html`

This task builds the skeleton: topbar with logo + chips, tab bar, content container, and all JS state management. No chart rendering yet — tab content areas are empty placeholders.

- [ ] **Step 1: Write `utils/templates/index.html`**

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Flashpoint — Match Reports</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:        #000000;
    --bg2:       #0d0d0d;
    --bg3:       #141414;
    --border:    #252525;
    --text:      #e0e0e0;
    --muted:     #666;
    --a-color:   #7986cb;
    --a-bg:      #1a237e;
    --a-border:  #3949ab;
    --b-color:   #80cbc4;
    --b-bg:      #00332d;
    --b-border:  #00695c;
    --hot-bg:    #3a0a0a;
    --hot-text:  #ff8a80;
  }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: 14px;
    min-height: 100vh;
  }

  /* ── Topbar ── */
  #topbar {
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    padding: 8px 20px;
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
  }
  #logo { height: 32px; object-fit: contain; }
  .chip {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 10px 4px 12px;
    border-radius: 20px; font-size: 12px; font-weight: 500;
    cursor: default; user-select: none;
  }
  .chip.match-a { background: var(--a-bg); color: var(--a-color); border: 1px solid var(--a-border); }
  .chip.match-b { background: var(--b-bg); color: var(--b-color); border: 1px solid var(--b-border); }
  .chip-x { cursor: pointer; opacity: 0.7; font-size: 15px; line-height: 1; }
  .chip-x:hover { opacity: 1; }

  #add-btn {
    background: transparent;
    border: 1px dashed #333;
    color: var(--muted);
    padding: 4px 14px;
    border-radius: 20px;
    font-size: 12px;
    cursor: pointer;
    position: relative;
  }
  #add-btn:hover { border-color: #555; color: #999; }

  #match-picker {
    display: none;
    position: absolute;
    top: calc(100% + 6px);
    left: 0;
    background: var(--bg3);
    border: 1px solid var(--border);
    border-radius: 6px;
    min-width: 220px;
    z-index: 100;
    max-height: 300px;
    overflow-y: auto;
  }
  #match-picker.open { display: block; }
  .picker-item {
    padding: 8px 14px;
    cursor: pointer;
    font-size: 12px;
    border-bottom: 1px solid var(--border);
    display: flex; justify-content: space-between;
  }
  .picker-item:last-child { border-bottom: none; }
  .picker-item:hover { background: var(--bg2); }
  .picker-item.loaded { opacity: 0.4; pointer-events: none; }
  .picker-meta { color: var(--muted); font-size: 11px; }

  /* ── Tabs ── */
  #tabbar {
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    padding: 0 20px;
    display: flex;
  }
  .tab-btn {
    padding: 10px 18px;
    font-size: 13px;
    color: var(--muted);
    cursor: pointer;
    border: none;
    background: transparent;
    border-bottom: 2px solid transparent;
    color: var(--muted);
    white-space: nowrap;
  }
  .tab-btn:hover { color: var(--text); }
  .tab-btn.active { color: var(--a-color); border-bottom-color: var(--a-color); }

  /* ── Content ── */
  #content { padding: 20px; }
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }

  /* ── Empty state ── */
  #empty-state {
    display: flex; flex-direction: column;
    align-items: center; justify-content: center;
    min-height: 60vh; gap: 12px; color: var(--muted);
  }
  #empty-state p { font-size: 15px; }
</style>
</head>
<body>

<div id="topbar">
  <img id="logo" src="logo.png" alt="Flashpoint" onerror="this.style.display='none'">
  <div id="chips"></div>
  <div style="position:relative">
    <button id="add-btn">+ Add match</button>
    <div id="match-picker"></div>
  </div>
</div>

<div id="tabbar">
  <button class="tab-btn active" data-tab="stats">Stats &amp; Tables</button>
  <button class="tab-btn" data-tab="charts">Motor &amp; Supply Charts</button>
  <button class="tab-btn" data-tab="power">Total Power</button>
  <button class="tab-btn" data-tab="wrps">Supply W/RPS</button>
</div>

<div id="content">
  <div id="empty-state">
    <p>No match loaded</p>
    <p style="font-size:12px">Use "+ Add match" to load a match</p>
  </div>
  <div id="tab-stats"  class="tab-panel"></div>
  <div id="tab-charts" class="tab-panel"></div>
  <div id="tab-power"  class="tab-panel"></div>
  <div id="tab-wrps"   class="tab-panel"></div>
</div>

<script>
// ── State ──────────────────────────────────────────────────────────────────
const state = {
  matches: [],      // [{id, data}] max 2
  manifest: [],     // [{id, duration, n_motors}]
  activeTab: 'stats',
};

// ── Boot ───────────────────────────────────────────────────────────────────
async function boot() {
  try {
    const r = await fetch('manifest.json');
    state.manifest = await r.json();
  } catch {
    state.manifest = [];
  }
  buildPicker();
}

// ── Picker ─────────────────────────────────────────────────────────────────
function buildPicker() {
  const el = document.getElementById('match-picker');
  el.innerHTML = state.manifest.length === 0
    ? '<div class="picker-item" style="pointer-events:none;color:#555">No matches found</div>'
    : state.manifest.map(m => {
        const loaded = state.matches.some(x => x.id === m.id);
        return `<div class="picker-item${loaded ? ' loaded' : ''}" onclick="addMatch('${m.id}')">
          <span>${m.id}</span>
          <span class="picker-meta">${m.n_motors} motors · ${m.duration.toFixed(0)}s</span>
        </div>`;
      }).join('');
}

document.getElementById('add-btn').addEventListener('click', e => {
  e.stopPropagation();
  document.getElementById('match-picker').classList.toggle('open');
});
document.addEventListener('click', () => {
  document.getElementById('match-picker').classList.remove('open');
});

// ── Match management ───────────────────────────────────────────────────────
async function addMatch(id) {
  document.getElementById('match-picker').classList.remove('open');
  if (state.matches.some(m => m.id === id)) return;
  if (state.matches.length >= 2) state.matches.shift();
  const r = await fetch(`data/${id}.json`);
  const data = await r.json();
  state.matches.push({ id, data });
  render();
}

function removeMatch(idx) {
  state.matches.splice(idx, 1);
  render();
}

// ── Tabs ───────────────────────────────────────────────────────────────────
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    state.activeTab = btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    renderActiveTab();
  });
});

// ── Render ─────────────────────────────────────────────────────────────────
function render() {
  renderChips();
  buildPicker();
  const hasMatch = state.matches.length > 0;
  document.getElementById('empty-state').style.display = hasMatch ? 'none' : 'flex';
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  if (hasMatch) {
    document.getElementById(`tab-${state.activeTab}`).classList.add('active');
    renderActiveTab();
  }
}

function renderChips() {
  const el = document.getElementById('chips');
  const labels = ['match-a', 'match-b'];
  el.innerHTML = state.matches.map((m, i) =>
    `<span class="chip ${labels[i]}">${m.id}
       <span class="chip-x" onclick="removeMatch(${i})">✕</span>
     </span>`
  ).join('');
}

function renderActiveTab() {
  if (state.matches.length === 0) return;
  const renderers = {
    stats:  renderStatsTab,
    charts: renderChartsTab,
    power:  renderPowerTab,
    wrps:   renderWrpsTab,
  };
  renderers[state.activeTab]?.();
}

// ── Tab renderers (stubs — filled in subsequent tasks) ─────────────────────
function renderStatsTab()  { document.getElementById('tab-stats').innerHTML  = '<p style="color:#555;padding:20px">Stats coming soon</p>'; }
function renderChartsTab() { document.getElementById('tab-charts').innerHTML = '<p style="color:#555;padding:20px">Charts coming soon</p>'; }
function renderPowerTab()  { document.getElementById('tab-power').innerHTML  = '<p style="color:#555;padding:20px">Power coming soon</p>'; }
function renderWrpsTab()   { document.getElementById('tab-wrps').innerHTML   = '<p style="color:#555;padding:20px">W/RPS coming soon</p>'; }

boot();
</script>
</body>
</html>
```

- [ ] **Step 2: Run a match through the CLI to test the shell**

```bash
python -m utils path/to/match.hoot --site /tmp/fp-site
open /tmp/fp-site/index.html
```
Expected: page loads, logo visible, "+ Add match" opens picker with the match, chip appears and can be removed

- [ ] **Step 3: Commit**

```bash
git add utils/templates/index.html
git commit -m "feat: add SPA shell with chip management and tab switching"
```

---

## Task 5: Stats Tab

**Files:**
- Modify: `utils/templates/index.html`

Replace the `renderStatsTab` stub with the full implementation.

- [ ] **Step 1: Replace `renderStatsTab` in `index.html`**

Replace:
```js
function renderStatsTab()  { document.getElementById('tab-stats').innerHTML  = '<p style="color:#555;padding:20px">Stats coming soon</p>'; }
```

With:

```js
function renderStatsTab() {
  const panel = document.getElementById('tab-stats');
  const STAT_COLS = [
    ['Motor',         null,            null],
    ['Max Motor V',   'max_motor_v',   2],
    ['Avg Motor V',   'avg_motor_v',   2],
    ['Max Stator A',  'max_stator_a',  2],
    ['Avg Stator A',  'avg_stator_a',  2],
    ['Peak Motor W',  'peak_motor_w',  1],
    ['P95 Motor W',   'p95_motor_w',   1],
    ['Max Supply V',  'max_supply_v',  2],
    ['Avg Supply V',  'avg_supply_v',  2],
    ['Max Supply A',  'max_supply_a',  2],
    ['Avg Supply A',  'avg_supply_a',  2],
    ['Peak Supply W', 'peak_supply_w', 1],
    ['P95 Supply W',  'p95_supply_w',  1],
    ['Avg Temp °C',   'avg_temp',      1],
    ['Max Temp °C',   'max_temp',      1],
  ];

  function statsTable(matchData, colorClass) {
    const motorOrder = Object.keys(matchData.motors);
    const thead = `<tr>${STAT_COLS.map(([h]) => `<th>${h}</th>`).join('')}</tr>`;

    function statCell(stats, key, decimals) {
      if (key === null) return '';
      const val = stats[key];
      if (val === null || val === undefined) return '—';
      return val.toFixed(decimals);
    }

    function rowHot(stats, key) {
      if (key === 'avg_temp' && stats.avg_temp !== null && stats.avg_temp > 55) return ' class="hot"';
      if (key === 'max_temp' && stats.max_temp !== null && stats.max_temp > 65) return ' class="hot"';
      return '';
    }

    const motorRows = motorOrder.map(mid => {
      const motor = matchData.motors[mid];
      const cells = STAT_COLS.map(([h, key, dec]) => {
        if (h === 'Motor') return `<td>${motor.name}</td>`;
        return `<td${rowHot(motor.stats, key)}>${statCell(motor.stats, key, dec)}</td>`;
      }).join('');
      return `<tr>${cells}</tr>`;
    });

    const totalStats = _totalStats(matchData);
    const totalRow = `<tr class="total-row">${STAT_COLS.map(([h, key, dec]) => {
      if (h === 'Motor') return `<td>TOTAL</td>`;
      if (['avg_temp','max_temp'].includes(key)) return `<td>—</td>`;
      return `<td>${statCell(totalStats, key, dec)}</td>`;
    }).join('')}</tr>`;

    return `
      <h4 class="match-heading ${colorClass}">${matchData.match_id} — Motor Stats</h4>
      <div class="table-wrap">
        <table><thead>${thead}</thead><tbody>${motorRows.join('')}${totalRow}</tbody></table>
      </div>`;
  }

  const cols = state.matches.length === 2 ? 'grid-cols-2' : 'grid-cols-1';
  const tables = state.matches.map((m, i) =>
    `<div>${statsTable(m.data, i === 0 ? 'color-a' : 'color-b')}</div>`
  ).join('');

  const energyTable = _energyTable();

  panel.innerHTML = `
    <div class="stats-grid ${cols}">${tables}</div>
    ${energyTable}`;
}

function _totalStats(matchData) {
  const t = matchData.totals;
  const hasSup = t.supply_power !== undefined;
  const last = arr => arr[arr.length - 1];
  return {
    peak_motor_w:  Math.max(...t.motor_power),
    p95_motor_w:   _p95(t.motor_power),
    avg_motor_v:   null, max_motor_v:   null,
    max_stator_a:  null, avg_stator_a:  null,
    max_supply_v:  null, avg_supply_v:  null,
    max_supply_a:  null, avg_supply_a:  null,
    peak_supply_w: hasSup ? Math.max(...t.supply_power) : null,
    p95_supply_w:  hasSup ? _p95(t.supply_power)        : null,
    avg_temp: null, max_temp: null,
  };
}

function _p95(arr) {
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.floor(sorted.length * 0.95);
  return sorted[Math.min(idx, sorted.length - 1)];
}

function _energyTable() {
  const motorIds = Object.keys(state.matches[0].data.motors);
  const matchCols = state.matches.map((m, i) =>
    `<th class="${i === 0 ? 'color-a' : 'color-b'}">${m.id}</th>`
  ).join('');

  const rows = motorIds.map(mid => {
    const cells = state.matches.map(m => {
      const e = m.data.motors[mid]?.motor_energy;
      return `<td>${e ? e[e.length - 1].toFixed(3) + ' Wh' : '—'}</td>`;
    }).join('');
    const name = state.matches[0].data.motors[mid].name;
    return `<tr><td>${name}</td>${cells}</tr>`;
  }).join('');

  const totalCells = state.matches.map(m => {
    const e = m.data.totals.motor_energy;
    return `<td>${e[e.length - 1].toFixed(3)} Wh</td>`;
  }).join('');

  return `
    <h4 class="match-heading color-base" style="margin-top:24px">Match Energy Summary — Motor Energy (Wh)</h4>
    <div class="table-wrap" style="max-width:500px">
      <table><thead><tr><th>Motor</th>${matchCols}</tr></thead>
      <tbody>${rows}<tr class="total-row"><td>TOTAL</td>${totalCells}</tr></tbody></table>
    </div>`;
}
```

- [ ] **Step 2: Add the supporting CSS to the `<style>` block in `index.html`**

Append inside the `<style>` tag:

```css
  /* ── Stats tab ── */
  .stats-grid { display: grid; gap: 20px; }
  .grid-cols-2 { grid-template-columns: 1fr 1fr; }
  .grid-cols-1 { grid-template-columns: 1fr; }

  .match-heading { font-size: 11px; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px; }
  .color-a    { color: var(--a-color); }
  .color-b    { color: var(--b-color); }
  .color-base { color: var(--a-color); }

  .table-wrap { overflow-x: auto; margin-bottom: 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 11px; }
  th { background: var(--bg3); color: #888; padding: 5px 8px; text-align: left;
       border-bottom: 1px solid var(--border); font-weight: 500; white-space: nowrap; }
  td { padding: 4px 8px; border-bottom: 1px solid var(--bg3); white-space: nowrap; }
  tr:hover td { background: var(--bg3); }
  tr.total-row td { color: var(--a-color); font-weight: 600; border-top: 1px solid #2a2a4a; }
  td.hot { background: var(--hot-bg) !important; color: var(--hot-text); }

  /* ── Section heading ── */
  .section-label {
    font-size: 11px; text-transform: uppercase; letter-spacing: 0.8px;
    color: var(--muted); margin: 20px 0 10px;
  }
```

- [ ] **Step 3: Smoke-test stats tab**

Load the site in a browser, add one and then two matches, verify:
- Stats table renders with correct motor rows
- Temperature cells above 55/65°C are highlighted red
- Energy summary shows correct Wh values
- TOTAL row appears at the bottom

- [ ] **Step 4: Commit**

```bash
git add utils/templates/index.html
git commit -m "feat: implement stats and energy tables in SPA"
```

---

## Task 6: Motor & Supply Charts Tab (Heatmaps)

**Files:**
- Modify: `utils/templates/index.html`

Replace `renderChartsTab` stub with Plotly heatmaps.

- [ ] **Step 1: Replace `renderChartsTab` in `index.html`**

Replace:
```js
function renderChartsTab() { document.getElementById('tab-charts').innerHTML = '<p style="color:#555;padding:20px">Charts coming soon</p>'; }
```

With:

```js
function renderChartsTab() {
  const panel = document.getElementById('tab-charts');
  const HEATMAP_METRICS = [
    { key: 'motor_power',    label: 'Motor Power',    unit: 'W',  required: true  },
    { key: 'supply_power',   label: 'Supply Power',   unit: 'W',  required: false },
    { key: 'stator_current', label: 'Stator Current', unit: 'A',  required: true  },
    { key: 'supply_current', label: 'Supply Current', unit: 'A',  required: false },
  ];

  let html = '';
  const divIds = [];

  HEATMAP_METRICS.forEach(metric => {
    const applicable = state.matches.filter(m =>
      metric.required || Object.values(m.data.motors).some(md => md[metric.key] !== undefined)
    );
    if (applicable.length === 0) return;

    html += `<div class="section-label">${metric.label}</div><div class="heatmap-row">`;
    applicable.forEach((m, i) => {
      const divId = `hm-${metric.key}-${i}-${Date.now()}`;
      divIds.push({ divId, match: m, metric, colorClass: i === 0 ? 'color-a' : 'color-b' });
      html += `<div>
        <div class="match-heading ${i === 0 ? 'color-a' : 'color-b'}">${m.id}</div>
        <div id="${divId}" style="height:${Math.max(160, Object.keys(m.data.motors).length * 28)}px"></div>
      </div>`;
    });
    html += '</div>';
  });

  panel.innerHTML = html;

  const plotLayout = {
    paper_bgcolor: '#000', plot_bgcolor: '#111',
    font: { color: '#ccc', size: 10 },
    margin: { t: 10, b: 40, l: 90, r: 60 },
    xaxis: { title: 'Time (s)', color: '#888', gridcolor: '#222' },
    yaxis: { color: '#888' },
  };

  divIds.forEach(({ divId, match, metric }) => {
    const motorIds = Object.keys(match.data.motors);
    const z = motorIds.map(mid => match.data.motors[mid][metric.key] || new Array(match.data.timestamps.length).fill(0));
    const yLabels = motorIds.map(mid => match.data.motors[mid].name);
    Plotly.newPlot(divId, [{
      type:        'heatmap',
      z:           z,
      x:           match.data.timestamps,
      y:           yLabels,
      colorscale:  'Plasma',
      showscale:   true,
      colorbar:    { title: { text: metric.unit, font: { color: '#888' } }, tickfont: { color: '#888' } },
    }], { ...plotLayout }, { responsive: true, displayModeBar: false });
  });
}
```

- [ ] **Step 2: Add heatmap CSS to the `<style>` block**

Append inside `<style>`:

```css
  /* ── Charts tab ── */
  .heatmap-row { display: grid; gap: 16px; margin-bottom: 8px; }
  .heatmap-row:has(> div:nth-child(2)) { grid-template-columns: 1fr 1fr; }
```

- [ ] **Step 3: Smoke-test charts tab**

Load site with one and two matches. Verify:
- Four heatmap sections appear (motor power, supply power, stator current, supply current)
- With 2 matches: each section shows two heatmaps side-by-side
- Plotly zoom/pan works
- Motor names appear on Y axis

- [ ] **Step 4: Commit**

```bash
git add utils/templates/index.html
git commit -m "feat: implement heatmap charts tab in SPA"
```

---

## Task 7: Total Power Tab + W/RPS Tab

**Files:**
- Modify: `utils/templates/index.html`

Replace the final two stubs.

- [ ] **Step 1: Replace `renderPowerTab` in `index.html`**

Replace:
```js
function renderPowerTab()  { document.getElementById('tab-power').innerHTML  = '<p style="color:#555;padding:20px">Power coming soon</p>'; }
```

With:

```js
function renderPowerTab() {
  const panel = document.getElementById('tab-power');
  const matchColors = ['#7986cb', '#80cbc4'];
  let html = '<div class="section-label">Total Robot Power — per match</div><div class="power-row">';
  const divIds = [];

  state.matches.forEach((m, i) => {
    const divId = `power-${i}-${Date.now()}`;
    divIds.push({ divId, match: m, color: matchColors[i] });
    html += `<div>
      <div class="match-heading ${i === 0 ? 'color-a' : 'color-b'}">${m.id}</div>
      <div id="${divId}" style="height:220px"></div>
    </div>`;
  });
  html += '</div>';

  const hasOverlay = state.matches.length === 2 &&
    state.matches.some(m => m.data.totals.supply_current !== undefined);
  const overlayId = `power-overlay-${Date.now()}`;
  if (hasOverlay) {
    html += `<div class="section-label">Match Comparison — Supply Current (overlaid)</div>
             <div id="${overlayId}" style="height:260px"></div>`;
  }

  panel.innerHTML = html;

  const baseLayout = {
    paper_bgcolor: '#000', plot_bgcolor: '#111',
    font: { color: '#ccc', size: 10 },
    margin: { t: 10, b: 40, l: 60, r: 20 },
    xaxis: { title: 'Time (s)', color: '#888', gridcolor: '#222' },
    yaxis: { title: 'Power (W)', color: '#888', gridcolor: '#222' },
    legend: { font: { color: '#aaa' } },
  };

  divIds.forEach(({ divId, match, color }) => {
    const t = match.data;
    const traces = [];
    if (t.totals.motor_power) {
      traces.push({ type: 'scatter', x: t.timestamps, y: _smooth(t.totals.motor_power, t.timestamps),
        name: 'Motor Power', line: { color: '#9fa8da', width: 1.5 } });
    }
    if (t.totals.supply_power) {
      traces.push({ type: 'scatter', x: t.timestamps, y: _smooth(t.totals.supply_power, t.timestamps),
        name: 'Supply Power', line: { color: color, width: 1.5 } });
    }
    Plotly.newPlot(divId, traces, { ...baseLayout }, { responsive: true, displayModeBar: false });
  });

  if (hasOverlay) {
    const traces = state.matches.map((m, i) => ({
      type: 'scatter',
      x: m.data.timestamps,
      y: _smooth(m.data.totals.supply_current, m.data.timestamps),
      name: m.id,
      line: { color: matchColors[i], width: 1.5 },
    }));
    const overlayLayout = { ...baseLayout,
      yaxis: { title: 'Supply Current (A)', color: '#888', gridcolor: '#222' },
    };
    Plotly.newPlot(overlayId, traces, overlayLayout, { responsive: true, displayModeBar: false });
  }
}

function _smooth(arr, timestamps) {
  const windowS = 1.0;
  if (timestamps.length < 2) return arr;
  const dt = (timestamps[timestamps.length - 1] - timestamps[0]) / (timestamps.length - 1);
  const w = Math.max(1, Math.round(windowS / dt));
  const out = new Array(arr.length);
  for (let i = 0; i < arr.length; i++) {
    let sum = 0, count = 0;
    for (let j = Math.max(0, i - Math.floor(w/2)); j <= Math.min(arr.length-1, i + Math.floor(w/2)); j++) {
      sum += arr[j]; count++;
    }
    out[i] = sum / count;
  }
  return out;
}
```

- [ ] **Step 2: Replace `renderWrpsTab` in `index.html`**

Replace:
```js
function renderWrpsTab()   { document.getElementById('tab-wrps').innerHTML   = '<p style="color:#555;padding:20px">W/RPS coming soon</p>'; }
```

With:

```js
function renderWrpsTab() {
  const panel = document.getElementById('tab-wrps');
  let html = '';
  const divIds = [];

  state.matches.forEach((m, i) => {
    const colorClass = i === 0 ? 'color-a' : 'color-b';
    const qualifying = Object.entries(m.data.motors).filter(
      ([, md]) => md.supply_power !== undefined && md.rotor_velocity !== undefined
    );
    if (qualifying.length === 0) return;

    const divId = `wrps-${i}-${Date.now()}`;
    const nrows = Math.ceil(qualifying.length / 2);
    divIds.push({ divId, match: m, qualifying, nrows });
    html += `<div class="section-label ${colorClass}">Supply W/RPS — ${m.id}</div>
             <div id="${divId}" style="height:${nrows * 180}px;margin-bottom:16px"></div>`;
  });

  panel.innerHTML = html || '<p style="color:#555;padding:20px">No W/RPS data available (requires supply + velocity signals)</p>';

  const VEL_MIN = 1.0;
  const SMOOTH_S = 3.0;

  divIds.forEach(({ divId, match, qualifying, nrows }) => {
    const ncols = 2;
    const subplots = qualifying.map((_, idx) => `xy${idx > 0 ? idx + 1 : ''}`);
    const traces = qualifying.map(([mid, md], idx) => {
      const vel = _smooth(md.rotor_velocity, match.data.timestamps, SMOOTH_S);
      const sp  = _smooth(md.supply_power,   match.data.timestamps, SMOOTH_S);
      const ratio = vel.map((v, j) => v >= VEL_MIN ? sp[j] / v : null);
      const axSuffix = idx > 0 ? String(idx + 1) : '';
      return {
        type: 'scatter', x: match.data.timestamps, y: ratio,
        name: md.name, connectgaps: false,
        xaxis: `x${axSuffix}`, yaxis: `y${axSuffix}`,
        line: { width: 0.8 }, showlegend: false,
      };
    });

    const layout = {
      paper_bgcolor: '#000', plot_bgcolor: '#111',
      font: { color: '#ccc', size: 9 },
      margin: { t: 10, b: 30, l: 55, r: 10 },
      grid: { rows: nrows, columns: ncols, pattern: 'independent', roworder: 'top to bottom' },
    };
    qualifying.forEach(([, md], idx) => {
      const axSuffix = idx > 0 ? String(idx + 1) : '';
      layout[`xaxis${axSuffix}`] = { title: 'Time (s)', color: '#888', gridcolor: '#222' };
      layout[`yaxis${axSuffix}`] = { title: 'W/RPS', color: '#888', gridcolor: '#222' };
      layout[`annotations`] = layout[`annotations`] || [];
      // motor name as subplot title approximation via annotation
    });
    // Add motor name annotations
    qualifying.forEach(([, md], idx) => {
      const row = Math.floor(idx / ncols);
      const col = idx % ncols;
      layout.annotations = layout.annotations || [];
      layout.annotations.push({
        text: md.name, showarrow: false, font: { color: '#aaa', size: 10 },
        xref: 'paper', yref: 'paper',
        x: col === 0 ? 0.22 : 0.78,
        y: 1 - (row / nrows) - 0.01,
      });
    });

    Plotly.newPlot(divId, traces, layout, { responsive: true, displayModeBar: false });
  });
}
```

Note: the `_smooth` function in `renderPowerTab` only takes 2 args. The W/RPS tab needs a 3-arg version. Update the signature:

Replace the `_smooth` function written in Task 7 Step 1:

```js
function _smooth(arr, timestamps, windowS = 1.0) {
  if (timestamps.length < 2) return arr;
  const dt = (timestamps[timestamps.length - 1] - timestamps[0]) / (timestamps.length - 1);
  const w = Math.max(1, Math.round(windowS / dt));
  const out = new Array(arr.length);
  for (let i = 0; i < arr.length; i++) {
    let sum = 0, count = 0;
    for (let j = Math.max(0, i - Math.floor(w/2)); j <= Math.min(arr.length-1, i + Math.floor(w/2)); j++) {
      sum += arr[j]; count++;
    }
    out[i] = sum / count;
  }
  return out;
}
```

- [ ] **Step 3: Add power + W/RPS CSS to the `<style>` block**

Append inside `<style>`:

```css
  /* ── Power + W/RPS tabs ── */
  .power-row { display: grid; gap: 16px; margin-bottom: 8px; }
  .power-row:has(> div:nth-child(2)) { grid-template-columns: 1fr 1fr; }
```

- [ ] **Step 4: Smoke-test both tabs**

Load site with 1 and 2 matches. Verify:
- Total Power: smoothed motor + supply power lines visible per match
- Total Power: supply current overlay chart appears with 2 matches only
- W/RPS: grid of subplots per match, near-zero velocity gaps appear as breaks in the line
- W/RPS: shows "No W/RPS data" message when velocity signals absent

- [ ] **Step 5: Commit**

```bash
git add utils/templates/index.html
git commit -m "feat: implement total power and W/RPS tabs in SPA"
```

---

## Task 8: End-to-End Validation

- [ ] **Step 1: Run the full test suite**

```bash
pytest tests/utils/test_site_builder.py -v
```
Expected: all tests PASS

- [ ] **Step 2: Generate a multi-match site**

```bash
python -m utils match1.hoot match2.hoot --site ./site --motor-names motors.toml
open ./site/index.html
```

Walk through each tab with 1 match loaded:
- Stats: full-width table, energy summary with 1 column
- Charts: single heatmap per metric
- Power: single power chart, no overlay
- W/RPS: subplots for qualifying motors

Then add the second match and verify:
- Stats: 50/50 split, energy summary gains second column
- Charts: 2-column heatmaps
- Power: side-by-side + supply current overlay appears below
- W/RPS: second match stacked below first

- [ ] **Step 3: Verify re-running same match overwrites cleanly**

```bash
python -m utils match1.hoot --site ./site
# Reload browser — data should be fresh, manifest should still list both matches
```

- [ ] **Step 4: Final commit**

```bash
git add .
git commit -m "feat: static site generator complete"
```
