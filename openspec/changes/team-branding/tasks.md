## 1. Tokens without amber names

- [x] 1.1 Write a failing test that greps `web/static/` for `--amber`, `amberHi`-style `FP.tokens()` keys and raw `rgba(255,176,0` / `rgba(255, 176, 0`; rename the tokens to roles (design D-a), and replace the 19 hard-coded values with `--glow-*` and `--grid-line` tokens; verify the test passes
- [x] 1.2 Add a test that the computed colours of a test page match today's values for every token (the amber look is unchanged); verify it and `pytest -m browser` pass

## 2. Theme model and maths

- [x] 2.1 Write failing tests for the WCAG contrast ratio and the OKLCH ↔ sRGB conversion against known values; implement in `flashpoint.theme`; verify tests pass
- [x] 2.2 Write failing tests for `theme.toml` loading (preset, custom, optional keys, name ≤ 40, `#RRGGBB` only: "Bad colour"); implement the pydantic model and the bundled default; verify tests pass
- [x] 2.3 Write failing tests for derivation ("Default palette": the `amber` preset equals today's token map exactly; "Custom primary": hue kept across text shades); implement; verify tests pass
- [x] 2.4 Write failing tests for validation and suggestions ("Too dark on dark", "Warning too close to primary", "Every preset passes", ≥ 3:1 for boundary lines); implement; verify tests pass and derivation plus validation run in < 20 ms

## 3. Delivery

- [x] 3.1 Write failing tests for `GET /theme.css` (colour tokens only, cached by file hash, empty on a broken theme) and `window.FP_MODE.team` in `mode.js`; link `theme.css` in `index.html` after `tokens.css`; implement; verify tests pass
- [x] 3.2 Render the team name in the header and title with `h()` ("Name and number", "No name", "Markup in the name"); verify browser tests pass
- [x] 3.3 Write `theme.css` and the team into static exports; verify the browser test "Static export" from file:// with networking disabled
- [x] 3.4 Add a browser test "Charts follow the palette" (Replay's primary series and cursor colour equal the theme's accent); verify it passes
- [x] 3.5 Add the theme check to the setup-check registry ("Broken theme file"); verify `doctor` and the check tests pass

## 4. Setup integration (after `team-setup` is applied)

- [x] 4.1 Write failing tests for `init --team-name --palette` and the prompts ("Init with a preset"); implement; verify tests pass and `cli-reference.md` is updated (the CLI drift test passes)
- [x] 4.2 Write failing tests for `POST /api/theme/preview` (guarded, returns derived tokens and validation, writes nothing) and `PUT /api/config/theme` through the config-editor save; implement; verify tests pass
- [x] 4.3 Add the Appearance section to the Setup view: preset picker, custom colour inputs, scoped live preview (header, chart, warning), validation messages and suggested colours; verify browser tests "Preview before saving" and "Refused in the view", and keyboard reachability

## 5. Docs and wrap-up

- [x] 5.1 Add a "Team name and colours" section to the user guide, a token naming note to the developer guide, and a Consequences note to ADR-0013; add the roadmap entry in `docs/rewrite/06-roadmap.md` if it isn't there; verify `pytest tests/test_docs.py` passes
- [x] 5.2 Run the full suite (`PYENV_VERSION=flashpoint poetry run pytest`, `-m browser`) and `openspec validate team-branding --strict`; verify both pass
- [ ] 5.3 [HUMAN] Look at each preset and one custom team palette on the pit laptop and a projector; record any preset that reads poorly
