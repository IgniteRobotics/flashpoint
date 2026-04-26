# Design: Logo/Header Title + `--hoot-dir` Flag

**Date:** 2026-04-26
**Branch:** feature/power-tracking

---

## Feature 1: Logo + Flashpoint Title in Topbar

### Problem

The topbar in `index.html` displays the logo image (`logo.png`) but has no text title. `media/logo.png` is an icon-only mark — no wordmark — so the app has no visible name when the logo loads.

### Design

Add `<span id="app-title">Flashpoint</span>` immediately after the `<img id="logo">` element in the topbar.

**HTML change** (`utils/templates/index.html`):
```html
<img id="logo" src="logo.png" alt="Flashpoint" onerror="this.style.display='none'">
<span id="app-title">Flashpoint</span>
```

**CSS addition:**
```css
#app-title {
  font-size: 16px;
  font-weight: 600;
  color: var(--text);
  letter-spacing: 0.5px;
}
```

The topbar is already a flex row with `gap: 12px` — no structural changes needed. The span slots in naturally between the logo and the chips.

### Scope

- Edit `utils/templates/index.html` only.
- No changes to `site_builder.py` — logo copy logic already exists.

---

## Feature 2: `--hoot-dir` Flag for Directory-Based Site Rebuild

### Problem

The current CLI requires explicit `.hoot` file paths as positional arguments. There's no way to point it at a directory and have it discover and process all hoot files automatically. Motor naming always requires `--motor-names` even though `motors.toml` is the universal config.

### Design

**New flags in `cli.py`:**

| Flag | Type | Description |
|------|------|-------------|
| `--hoot-dir PATH` | `Path` | Directory to glob for `**/*.hoot` files recursively |
| `--clean` | `bool` | Delete `site_dir/data/*.json` before processing |

**Behavioral changes:**

- `files` positional arg changes from `nargs="+"` to `nargs="*"`.
- Runtime validation: exactly one of `files` (non-empty) or `--hoot-dir` must be provided. Both together is an error.
- `--clean` is only meaningful with `--site`; warn and ignore otherwise.
- When `--hoot-dir` is active, `motors.toml` is auto-loaded from the package directory (`Path(__file__).parent / "motors.toml"`) without requiring `--motor-names`. An explicit `--motor-names` still takes precedence.

**Clean behavior:** deletes `site_dir/data/*.json` before processing, then rebuilds manifest. Existing static assets (`index.html`, `plotly.min.js`, `logo.png`) are left untouched.

**Merge behavior (default, no `--clean`):** new match JSONs are written, manifest is regenerated from all present JSONs. Previously built matches not present in the current hoot dir are preserved.

### Scope

- Edit `cli.py` only — no changes to `site_builder.py`, `hoot_loader.py`, or other modules.
- `--site` remains required when using `--hoot-dir`.

---

## Files Changed

| File | Change |
|------|--------|
| `utils/templates/index.html` | Add `#app-title` span + CSS |
| `utils/cli.py` | Add `--hoot-dir`, `--clean`; make `files` optional |
