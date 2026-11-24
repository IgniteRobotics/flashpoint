## Context

See proposal.md for why, and the delta spec for the three requirements. The constraints that shape the approach, all checked on 2026-11-24 against the pinned Mermaid 12.3.0 and Zensical 0.1.3:

- **The theme draws each diagram itself.** On load it calls `mermaid.initialize()` once with its own settings, then renders each `pre.mermaid` into a **closed** shadow root. A page script can't reach into a closed shadow root, so it can't copy, restyle or measure the drawn SVG. CSS custom properties do inherit into it, and the theme's own `themeCSS` reads them (`--md-mermaid-*`).
- **Our copy of Mermaid loads before the theme's bundle** (`docs/overrides/main.html`, the `scripts` block from #117), on pages with a diagram only. A script placed right after it runs before the theme calls `initialize()`.
- **Mermaid 12.3.0 defaults:**
  - ELK layout, and flowchart labels wrapping at 120 px;
  - HTML labels in `foreignObject`, which Safari and the iOS app clip at the bottom;
  - its own font (trebuchet), while the theme draws labels in the docs font, so boxes are sized for the wrong text;
  - `classDef` can't take `var()`: that's a parse error.
- **The column** is 648 px wide in a 1280 px window. The pipeline's natural width is 1909 px (34 %). Top to bottom it's 432 px (100 %).
- **ADR-0019's feature subset** (amended 2026-11-23) already allows the `scripts` override, `extra_css` and `extra_javascript`.

## Goals / Non-Goals

**Goals:**
- Every requirement checked by a test that fails on a regression: scale, zoom, colours in both palettes, legends, labels inside their boxes.
- Diagram sources stay plain Mermaid that GitHub still draws: no tool-specific syntax that breaks there.

**Non-Goals:**
- Restyling diagrams on GitHub. GitHub ignores our stylesheet, so the `docs/rewrite/` copies show Mermaid's default colours. They still get the layout rules.
- Pan and zoom inside the opened diagram. Opening it at window size is enough at today's sizes.

## Decisions

### D1. The scale check measures what the reader sees
- A browser test opens each built page that has a diagram in a 1280 × 800 window, with shadow roots forced open (the `OPEN_SHADOW_ROOTS` hook in `tests/docs_site.py`).
- For each diagram it reads the drawn width and the natural width (the SVG `viewBox`), and fails below 0.8. The failure names the page, the diagram's position and the percentage.
- Turning those measurements into failures is a pure function, unit-tested on made-up numbers.
- A browser fixture (a static page with the old left-to-right pipeline in a 648 px column) proves the measurement catches a wide diagram.
- *Alternative:* estimate width from the source (count boxes in a row). Rejected: layout, label length and fonts decide the width, and only drawing shows it.

### D2. One small script carries the notation
- `docs/overrides/assets/javascripts/diagrams.js` loads right after the vendored Mermaid, inside the same `{% if %}`.
- It wraps `mermaid.initialize` so the theme's one call also gets:
  - `htmlLabels: false`;
  - `fontFamily` set to the font the theme draws in (the computed `--md-mermaid-font-family`);
  - `flowchart.wrappingWidth` raised so authors' own line breaks decide the lines;
  - a `themeCSS` block colouring the kind classes from custom properties.
- It also makes each render wait for `document.fonts.ready`, so boxes are measured in the loaded font.
- *Alternatives:*
  - **Settings at the top of each diagram:** repeated in every diagram, and colours can't go there.
  - **Calling `initialize` ourselves:** the theme's later call replaces our settings.
  - **Changing the theme's bundle:** not allowed under ADR-0019, and lost on every update.

### D3. Kinds are classes; colours come from the docs palette
- Authors tag boxes with five classes: `step`, `data`, `robot`, `outside` (the dashed outline) and, in state diagrams, `error`.
- Each diagram's source declares `classDef outside stroke-dasharray:5 4`, so GitHub shows the dashed outline too. No colour goes in the source.
- `docs/overrides/assets/stylesheets/diagrams.css` (through `extra_css`) defines `--fp-diagram-*` from Material's palette variables:
  - each kind has a base colour from the palette: `step` the primary colour, `data` the code-highlight green (`--md-code-hl-string-color`), `robot` and `outside` the neutral `--md-default-fg-color--light`, `error` the code-highlight red (`--md-code-hl-number-color`). Primary and accent are both indigo in the default palette (`#4051b5`, `#526cfe`), too close to tell two kinds apart, so `data` doesn't use the accent (found while applying, 2026-11-24);
  - the box fill is `color-mix()` of the base colour into `--md-default-bg-color`; `outside` has no fill;
  - the outline is the full colour.

  The theme redefines those variables when the toggle changes, and they reach into the shadow root, so the diagram follows the toggle without a redraw.
- **Legends:** an `md_in_html` block (`<div class="fp-legend" markdown>`) right below the diagram, with one swatch per kind, styled from the same variables. A unit test reads each page's Markdown and checks that every kind class the diagram uses is named in its legend.
- *Alternative:* the Ignite brand palette. Rejected on 2026-11-24 (docs theme).

### D4. Zoom redraws the diagram from its source
- `diagrams.js` runs before the theme touches the page. It wraps each `pre.mermaid` in a `<div class="fp-diagram">` that keeps the source text and takes focus, `role="button"` and a label.
- `zoom.js` treats `.fp-diagram` as zoomable. On open it calls `mermaid.render()` on the kept source into the dialog, sized to fit the window whole, on a card in the page's background colour (the backdrop is near-black in both palettes, and light-palette labels are dark).
- Whole beats larger: the top-to-bottom pipeline (430 × 1244 px) fits an 800 px window only at 57 %, so it opens as an overview. A diagram that fits, like the file lifecycle, opens larger than on the page. The spec says so (amended 2026-11-24, Josh's call).
- The redrawn SVG lives in the page, not a shadow root, and takes its colours from the same variables, so it matches the palette.
- Closing works as for images.
- *Alternatives:*
  - **Copying the drawn SVG:** impossible with a closed shadow root.
  - **Forcing shadow roots open in production:** patches a browser API for every page, to work around the theme.

### D5. Content
- **Pipeline:** `flowchart TB` with kind classes and a legend. "Logs you copied by hand" is `outside`; inbox, raw, the tiers, anomalies and Replay data are `data`; the commands are edge labels.
- **New lifecycle diagram** (`stateDiagram-v2`) next to "What happens to a `.hoot` file":
  - states: raw, converted, bronze, failed, quarantined;
  - "failed" is retried by the next run; a file that crashes owlet 3 times is quarantined;
  - failed and quarantined are `error`;
  - step 2's prose is trimmed so it doesn't repeat the diagram.
- **Developer guide:** a "Diagrams" section with the rules, the kinds, a legend example and the 80 % check.

### D6. No ADR, and the D-decisions
- Everything sits inside ADR-0019's subset as amended on 2026-11-23: the `scripts` override, `extra_css`, `extra_javascript`. No new amendment.
- The change touches none of D1–D11 (docs only). Nothing reads logs, so there's no memory or time budget beyond the tests: about 15 s of browser tests on top of #117's.

## Risks / Trade-offs

- **[The wrapper depends on how the theme calls Mermaid]** → Zensical is pinned exactly, and the scale, colour and zoom tests run on every bump. The Material 9.8 fallback calls Mermaid the same way.
- **[`color-mix()` needs Safari 16.2, Chrome 111 or Firefox 113]** → Each kind declares a plain fill first, so an older browser still gets a distinct, if flatter, colour.
- **[Plain SVG labels drop Markdown formatting such as bold]** → Docs diagrams don't use it. The label test catches a label that overflows.
- **[GitHub shows the classes without colours]** → Accepted: the boxes and the dashed outline still read, and the docs site is where the legend applies.
- **[Wider wrapping can push a diagram over 80 %]** → The scale check catches it. Authors break their own lines.

## Migration Plan

Docs only. It deploys with the next `dev` deploy. Roll back by reverting the PR and redeploying `dev`.
