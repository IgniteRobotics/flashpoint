## Why

Since #117 the docs draw Mermaid diagrams, but the one they have can't be read. How Flashpoint works shows the pipeline as a left-to-right chain 1909 px wide. The theme shrinks it into the 648 px column, so it draws at 34 % on a laptop (labels about 5 px) and 16 % on a phone. The diagrams still to come will hit the same wall: a file's lifecycle, Live CAN's connection, and the architecture. This change makes docs diagrams readable at column width, zoomable, and consistent, before the 2027-01-04 announcement links these pages.

Roadmap: the "MVP public release" section of `docs/rewrite/06-roadmap.md` (#68), as docs polish after `docs-visual-polish` (#116). Issue #120. It fixes no pitfall from `03-pitfalls-and-performance.md`.

## What Changes

- **Layout rules and a scale check.** Docs diagrams run top to bottom, with at most about four boxes side by side, and big ones are split. A browser test fails any docs diagram drawn under 80 % of its natural size in the docs column, naming the page and the diagram. The rules go in the developer guide, for `docs/rewrite/` too.
- **The pipeline redrawn** top to bottom, and **a new file-lifecycle diagram** on How Flashpoint works (raw, converted, quarantined, failed and retried). It replaces part of step 2's prose.
- **Diagram zoom.** Click-to-zoom opens a diagram full-frame too, not only images and video.
- **One notation:**
  - boxes are coloured by kind (a step, data, the robot, something outside Flashpoint), in the docs theme's colours, following the light/dark toggle;
  - each diagram that uses colour gets a legend;
  - labels are drawn as plain SVG text, so Safari and the iOS app don't clip them;
  - Mermaid sizes each box for the font the docs actually draw it in.

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `project-documentation`: adds three requirements: diagrams fit the docs column, diagrams open full-frame, and one diagram notation. They're ADDED under new names. "Diagrams render as pictures" and "Images open full-frame" stay as they are.

## Non-goals

- The Ignite brand palette for diagrams. Colours come from the docs theme (decided 2026-11-24).
- Another diagram tool: D2, LikeC4, Graphviz, draw.io. Each adds a toolchain to the docs build, none renders on GitHub, and D2's dark mode follows the computer, not the toggle.
- An architecture diagram on the docs site. It waits for a change that documents the architecture from the code. The diagrams in `docs/rewrite/` aren't redrawn here; they follow the rules when next edited.
- A Live CAN sequence diagram. Drawn today it's about 75 % in the column, so it waits until it can pass the check.
- Checking diagrams at phone width. The check runs at desktop width; top-to-bottom layouts are what help phones.

## Impact

- **`docs/guide/under-the-hood/how-it-works.md`:** the pipeline redrawn, the lifecycle diagram added, legends.
- **`docs/guide/under-the-hood/developer-guide.md`:** a short "Diagrams" section with the rules.
- **`docs/overrides/`:**
  - `main.html` loads a small diagram script next to the vendored Mermaid, on pages with a diagram;
  - a diagram stylesheet holds the kind colours;
  - `zoom.js` and `zoom.css` handle diagrams.
- **`mkdocs.yml`:** one more `extra_css` entry. That's within ADR-0019's feature subset as amended on 2026-11-23, so no new amendment.
- **New browser tests** for the scale check, diagram zoom, and colours in both palettes, all in new files. The existing diagram and zoom tests keep passing.
- **No new dependencies:** the vendored Mermaid 12.3.0 and its pin stay as they are.
