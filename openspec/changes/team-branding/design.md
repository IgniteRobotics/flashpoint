## Context

See proposal.md for the motivation and specs/team-branding/spec.md for the behaviour. The facts that shape the design:

- **Tokens:** `web/static/tokens.css` defines every colour as a CSS custom property, named after amber (`--amber`, `--amber-hi`, `--amber-mid`, `--amber-dim`, `--amber-muted`, `--prose`, `--hot`, `--on-amber`, surfaces, lines). Its comments record each text shade's contrast (10.9:1 down to 5.1:1).
- **Charts:** uPlot can't read CSS variables, so `FP.tokens()` (`shell.js:82`) reads them once with `getComputedStyle` and caches them. Charts follow whatever the CSS says at first read.
- **Hard-coded colours:** 19 `rgba(255,176,0,…)` / `rgba(255, 176, 0, …)` values across `tokens.css`, `shell.css` and the views (glows, grid, overlays).
- **Header:** `shell.js:711` writes the fixed text `telemetry // ignite robotics`.
- **Static export** (`report/export.py`) copies `static/` and writes `mode.js` (`window.FP_MODE`). The served app gets `mode.js` from the server.
- **`team-setup`** (unarchived) gives the configuration home, bundled defaults, `init`, the setup-check registry and the guarded `PUT /api/config/<section>` save with tomlkit and backups. This change plugs into those, so it is applied after `team-setup`.

## Goals / Non-Goals

**Goals:**
- One function turns a theme into a token map. Python owns it, and both the server and the export use it.
- With the `amber` preset, the CSS is byte-for-byte identical to today's colour values, so the default look can't regress.
- Validation is the same code for `init`, the Setup view, loading, and the preset self-test.

**Non-Goals:**
- No runtime theme switching without a reload. Charts cache tokens, and a reload is simpler than invalidating every chart.
- No colour-blindness simulation. Severity already uses shape and label, not hue alone.

## Decisions

### D-a. Role-named tokens
Rename the colour tokens to roles:

| Today | New |
|---|---|
| `--amber` | `--accent` |
| `--amber-hi` | `--accent-hi` |
| `--amber-mid` | `--accent-mid` |
| `--amber-dim` | `--accent-dim` |
| `--amber-muted` | `--accent-muted` |
| `--hot` | `--warn` |
| `--on-amber` | `--on-accent` |
| `--prose` | unchanged |

`FP.tokens()` keys follow (`amber` → `accent`). The 19 hard-coded rgba values become `--glow-*` and `--grid-line` tokens built from the accent's RGB.
- *Why:* a student reading `--amber` on a blue theme will be confused, and the rename is mechanical (one sed plus review).
- *Alternative:* keep the names. Rejected for that confusion. A test greps the static files for any `amber` token or raw `rgba(255,176,0` left behind.

### D-b. Derivation in OKLCH
- Convert primary, background and warning to OKLCH.
- **Text shades:** keep the primary's hue, and step lightness and chroma by fixed offsets, measured from today's amber scale so that the `amber` preset reproduces it.
- **Surfaces:** the background's hue with small lightness steps (today's +0.5–3%).
- **Lines:** the primary's hue at low lightness.
- Convert back to sRGB hex, clamped to gamut.
- **The `amber` preset is a stored token map, not derived.** That guarantees "identical to today" even where the derivation would round differently. Every other preset is derived and stored as its three input colours.
- *Alternative:* HSL. Rejected: HSL lightness isn't perceptual, so blue and yellow derive very different contrast at the same "lightness".

### D-c. Validation and suggestions
- Contrast uses the WCAG 2 relative-luminance ratio, matching the 4.5:1 in the shell spec.
- "Nearest passing colour": binary-search the OKLCH lightness of the primary (or the warning colour), keeping hue and chroma, until every rule passes. Return it as hex, or none if no lightness passes.
- **Built-in warning colours** are tried in order: `#FF5A1F` (today's hot), `#00C2FF`, `#FF3DAE`, `#FFE14D`.

### D-d. Delivery to the browser
- **Served:** the server answers `GET /theme.css` with a `:root{…}` block of colour tokens. `index.html` links it after `tokens.css`, so it overrides it before first paint, with no flash.
- **Name:** goes into `mode.js`, as `window.FP_MODE.team = {name, number}`. The header renders it with `h()`, as text only. The page title becomes `Flashpoint · <name>`.
- **Static export:** writes the same `theme.css` and `mode.js` content into the export.
- **Failure:** if the theme fails to load, the server serves an empty `theme.css` (the defaults in `tokens.css` apply), and the setup check reports it.

### D-e. Configuration file
`theme.toml` in the configuration root:

```toml
team_name = "Ignite Robotics"   # optional, ≤ 40 characters
team_number = 6829              # optional, display only; defaults to acquire's team
palette = "amber"               # a preset name, or "custom"
[custom]                        # only with palette = "custom"
primary = "#00A3E0"
background = "#0A0806"          # optional
warning = "#FF5A1F"             # optional
```

It's validated with pydantic (`extra="forbid"`) plus D-c. The bundled default is `palette = "amber"` with no name.

### D-f. Setup integration
- `init` gains `--team-name` and `--palette`, and terminal prompts.
- The Setup view's Appearance section:
  - previews by applying the derived tokens to a scoped preview container (CSS variables on that element only), so the rest of the app keeps the saved theme;
  - gets derived tokens from `POST /api/theme/preview`, a guarded, read-only computation that also returns validation results;
  - saves through `PUT /api/config/theme`, the `config-editor` save pipeline (tomlkit, conflict check, backups).

### Architecture decisions
- **D6 / D7 / ADR-0013:** the shell keeps one look across views; the look is now configurable. A short note goes into ADR-0013's Consequences.
- No other D-decision is touched, and no new ADR is needed.

## Budgets
- Theme derivation and validation: < 20 ms (runs on each `theme.css` request and preview; cached by file hash).
- No new dependency: the OKLCH conversion and the WCAG maths are about 60 lines of Python.

## Risks / Trade-offs

- [The rename touches every view file] → Mechanical, with a grep test for leftovers. The browser suite runs in full afterwards. The `amber` preset's byte-identical check catches any colour change.
- [Derived shades look off for unusual hues (yellow, very saturated colours)] → Validation stops unreadable palettes. Looks are a team's call, and the preview shows them before saving.
- [Static exports carry the exporter's theme] → Intended: the export is the team's.
- [`--line-strong` is documented as 3.05:1, non-text only] → It keeps the ≥ 3:1 boundary rule, not 4.5:1.

## Migration Plan
Additive. No theme file means today's look, exactly. Rollback is a revert; delete `theme.toml` to drop a team's theme.
