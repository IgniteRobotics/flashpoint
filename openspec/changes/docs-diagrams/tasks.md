## 1. Diagrams fit the column (design D1)

- [ ] 1.1 Test first, in new files. Unit: a pure function turns (page, position, natural width, drawn width) measurements into problems, failing below 0.8 and naming the page, the position and the percentage (made-up numbers, including exactly 0.8). Browser (`browser` marker): a fixture page draws today's left-to-right pipeline with the vendored Mermaid in a 648 px column, and the measurement reports it at about 34 %. Then every built docs page with a diagram is measured in a 1280 × 800 window with shadow roots forced open (`tests/docs_site.py`). Verify: the unit and fixture cases pass, and the site case fails on today's pipeline, naming `under-the-hood/how-it-works`.
- [ ] 1.2 Redraw the pipeline as `flowchart TB` with the same boxes, labels and edges. Verify: 1.1 passes, and so do `tests/test_docs_diagrams.py`, the strict build and `check-site`.

## 2. One notation (design D2, D3)

- [ ] 2.1 Test first, in new files. Unit: each docs page's diagram source names only the five kind classes, and every class it uses is named in an `fp-legend` block right below it. A fixture with a missing legend entry fails, naming the page and the kind. Browser:
  - in both palettes, switched with the page's own toggle, the pipeline's `step`, `data` and `robot` boxes have three different fills, and the fills change with the toggle;
  - every label is plain SVG text (no `foreignObject`) whose box lies inside its node's box;
  - the console is clean.

  Verify: these fail before 2.2.
- [ ] 2.2 Add `diagrams.js` (wrap `mermaid.initialize`: `htmlLabels: false`, the theme's font, wider wrapping, the kind `themeCSS`; renders wait for `document.fonts.ready`), load it in `main.html` right after the vendored Mermaid inside the same `{% if %}`, and add `diagrams.css` (the `--fp-diagram-*` colours from the theme's palette variables, with plain fallbacks, and the legend styles) through `extra_css`. Tag the pipeline's boxes and add its legend. Verify: 2.1 and 1.1 pass, and a screenshot of How Flashpoint works in each palette shows the kinds apart.
- [ ] 2.3 Add the file-lifecycle `stateDiagram-v2` (raw, converted, bronze, failed, quarantined; failed and quarantined `error`) beside "What happens to a `.hoot` file", with its legend, and trim step 2's prose so the two agree. Verify: 1.1 and 2.1 pass on the new diagram, and the states and retry rule match the ingest statuses (`src/flashpoint/ingest.py`) and `SUSPECT_ATTEMPTS` (`src/flashpoint/readers/reasons.py`).

## 3. Diagrams open full-frame (design D4)

- [ ] 3.1 Browser test first, in a new file:
  - clicking the pipeline opens a dialog whose diagram fills the window, whole and wider than on the page;
  - Esc closes it at the same scroll position, with focus back on the diagram, and Enter opens it from the keyboard;
  - opened in the dark palette, its fills match the page's dark fills;
  - from the offline archive with the network blocked, it opens and no request leaves the archive's folder.

  Verify: it fails before 3.2.
- [ ] 3.2 In `diagrams.js`, wrap each `pre.mermaid` in a focusable `.fp-diagram` that keeps its source. In `zoom.js`, open `.fp-diagram` by drawing its source again with `mermaid.render()` into the dialog, sized to the window. Verify: 3.1 passes, and `tests/test_docs_zoom.py` still passes.

## 4. Rules and records

- [ ] 4.1 Add a "Diagrams" section to the developer guide: the 80 % limit, top to bottom, about four across, split big diagrams, the five kinds with a legend example, and how to run the check. Verify: the link and drift checks pass, and the section names the test that enforces the limit.
- [ ] 4.2 Record the decision in `docs/rewrite/04-ecosystem-and-tools.md`: diagrams are Mermaid, checked at 80 % in the docs column, with D2, LikeC4 and Graphviz considered and why they lost. Verify: the link check passes.

## 5. Gate and wrap-up

- [ ] 5.1 Run the full gate: `ruff`, `mypy src tests tools`, `pytest`, `-m browser`, the strict build of both configs, `check-site`, and `openspec validate docs-diagrams --strict`. Verify: all pass.
- [ ] 5.2 [HUMAN] After the next `dev` deploy, Josh opens How Flashpoint works on a phone and a laptop, in both palettes, and opens each diagram full-frame. Verify: his OK is recorded on the PR.
- [ ] 5.3 Run `openspec archive docs-diagrams` as the PR's last commit. Verify: `openspec validate --all` passes.
