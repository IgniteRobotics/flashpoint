# Offline Dependencies — Design Spec

**Date:** 2026-04-25
**Status:** Approved

## Overview

Bundle Plotly.js as a static package asset so the generated site works with no internet connection. Plotly.js is the only external dependency in the current SPA template. At FRC competitions, there is no reliable internet access.

---

## Architecture

One new static asset; minimal changes to three existing locations.

```
utils/
  static/
    plotly-2.35.2.min.js   ← committed to repo, downloaded once manually
  site_builder.py           ← ensure_index copies it to site/
  templates/index.html      ← script src changed from CDN to local path
```

After each CLI run, the site directory contains:

```
site/
  index.html
  plotly.min.js             ← copied by ensure_index on every run
  manifest.json
  logo.png
  data/
    <match_id>.json
    ...
```

---

## Changes

### `utils/static/plotly-2.35.2.min.js`

Downloaded once and committed to the repo:

```bash
mkdir -p utils/static
curl -o utils/static/plotly-2.35.2.min.js \
  https://cdn.plot.ly/plotly-2.35.2.min.js
```

Never regenerated. To upgrade Plotly, download a new version, commit it, update the constant and template src.

### `utils/site_builder.py`

Add a `_PLOTLY_PATH` module-level constant alongside the existing `_TEMPLATE_PATH` and `_LOGO_PATH`:

```python
_PLOTLY_PATH: Path | None = Path(__file__).parent / "static" / "plotly-2.35.2.min.js"
```

Update `ensure_index` to copy it unconditionally (always re-copied, unlike `index.html`):

```python
def ensure_index(site_dir: Path) -> None:
    site_dir.mkdir(parents=True, exist_ok=True)
    if _PLOTLY_PATH and _PLOTLY_PATH.exists():
        shutil.copy(_PLOTLY_PATH, site_dir / "plotly.min.js")
    index = site_dir / "index.html"
    if index.exists():
        return
    if _TEMPLATE_PATH and _TEMPLATE_PATH.exists():
        shutil.copy(_TEMPLATE_PATH, index)
    if _LOGO_PATH and _LOGO_PATH.exists():
        shutil.copy(_LOGO_PATH, site_dir / "logo.png")
```

Plotly is always overwritten to ensure the version in the site dir stays in sync with the package. `index.html` remains write-once.

### `utils/templates/index.html`

Swap the CDN `<script>` for a local reference:

```html
<!-- before -->
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>

<!-- after -->
<script src="plotly.min.js"></script>
```

---

## Tests

Two additions to `tests/utils/test_site_builder.py`:

| Test | Assertion |
|------|-----------|
| `test_ensure_index_copies_plotly` | `site/plotly.min.js` exists after `ensure_index` |
| `test_ensure_index_overwrites_plotly` | plotly is re-copied even when `site/plotly.min.js` already exists |

Both tests monkeypatch `_PLOTLY_PATH` to a small fixture file (same pattern as existing `_TEMPLATE_PATH` tests).

---

## Files Changed

| File | Change |
|------|--------|
| `utils/static/plotly-2.35.2.min.js` | New — downloaded and committed |
| `utils/site_builder.py` | Add `_PLOTLY_PATH`; update `ensure_index` |
| `utils/templates/index.html` | Swap CDN src for local `plotly.min.js` |
| `tests/utils/test_site_builder.py` | Add 2 tests for Plotly copy behavior |

No other files change. The CLI, models, and PDF pipeline are untouched.
