## Why

Once any team can install Flashpoint (`team-setup`), every team still sees Ignite: the header reads "telemetry // ignite robotics" and the whole app is Ignite's amber. A team's students use it more when it looks like theirs, and a pit display that shows the team's name and colours reads as the team's tool, not a borrowed one. The shell already routes every colour through design tokens (`tokens.css`, `FP.tokens()` for charts), so a palette can be swapped in one place.

This follows Team setup (#24) on the roadmap (`06-roadmap.md`) and extends the one-app shell of ADR-0013 (D6/D7).

## What Changes

- **Team name in the app:** the header and page title show the team's display name and number (such as "Ignite Robotics · 6829"). With no name set, the header reads plain "telemetry".
- **Colour palette:** a team picks a preset, or gives its own primary colour and, optionally, background and warning colours. Flashpoint works out the full set of shades from them.
- **Readable by rule:** text shades must keep 4.5:1 contrast on every surface, and warnings must stay distinct from the primary colour. A palette that can't meet that is refused with the reason and the nearest colour that would pass. Ignite's amber stays the default.
- **Part of setup:**
  - `flashpoint init` asks for the team name and a preset;
  - the Setup view gets an Appearance section with a live preview, saved through the config editor's guarded, validated save.
- **Everywhere the app goes:** charts, the static export, and every view use the palette, with no view changes beyond replacing hard-coded amber values with tokens.

## Capabilities

### New Capabilities
- `team-branding`: the team display name, palette presets and custom palettes, palette validation, and applying them in served mode and in static exports.

### Modified Capabilities
None in `openspec/specs/`. The shell's "same colours across views" and 4.5:1 contrast rules (`match-reports`, "Shared app shell") still hold: the palette changes for every view at once, and validation enforces the contrast. The `init` and Setup-view additions build on the `team-setup` and `config-editor` specs, which are still in the unarchived `team-setup` change.

## Non-goals

- Team logos or images.
- Custom fonts, sizes or layout. Type and spacing stay the canvas design.
- Per-user or per-device themes, and a light mode.
- Changing the colours that carry meaning beyond warnings: severity markers keep their shapes and labels.

## Impact

- New `theme.toml` in the configuration root (bundled default: Ignite amber, no name).
- `tokens.css`: amber-named colour tokens become role-named (`--accent`, `--accent-hi`, …). The 19 hard-coded `rgba(255,176,0,…)` glows and grid lines become tokens. `FP.tokens()` keys are renamed to match.
- The server injects the palette and name. `flashpoint report --static` writes them into the export.
- `init` gains `--team-name` and `--palette`. The Setup view gains Appearance.
- Depends on `team-setup` (#24) for `init`, the configuration home and the guarded save.
- Pitfalls fixed: none.
