## Why

The first live deploy of the docs site (2026-11-23) showed four visual defects that a new reader hits right away. The README's three screenshots are squeezed into a three-column table. The pipeline diagram on How Flashpoint works shows as Mermaid source, not a picture. Every animation is under 4 seconds, a loading screen followed by the end state, so nobody can follow the flow. And a 1440×900 screenshot shrunk to the page column can't be read, with no way to enlarge it. These pages are what the 2027-01-04 announcement links, so they need fixing before the `0.3.0rc1` outside-mentor beta (~2026-12-07).

Roadmap: the "MVP public release" section of `docs/rewrite/06-roadmap.md` (#68), as polish on `mvp-documentation` (#82). Issue #116. It fixes no pitfall from `03-pitfalls-and-performance.md`.

## What Changes

- **README and docs home screenshots:** one per row, full width, each under a line naming the view and what it shows, in place of the README's three-column table and the home page's grid. (The home page was added after the first `dev` deploy of this change showed its grid tiling them 2 + 1.)
- **Diagrams:** the `mermaid` fence is turned on, and a Mermaid diagram renders as a picture on the site and in the offline archive. The Mermaid library is served from the docs site itself (fetched by checksum at build), not from unpkg.com, and only on pages that have a diagram.
- **Paced animations:** each flow is recorded at a human pace. The clip starts with the view already loaded, holds on the starting screen, outlines each element just before it's clicked, pauses after each step, and holds on the result. Each animation runs 6 to 20 seconds. The capture rejects a clip that is too short, or whose first frame is the loading screen.
- **Image zoom:** on every docs page, clicking a screenshot, GIF or animation opens it at the full size of the browser window. Clicking again, pressing Esc, or the close button returns to the page. A small script and stylesheet in the docs theme do this, with no third-party library, so it works in the offline archive too.

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `project-documentation`: adds four requirements: README screenshots in a single column with descriptions, diagrams that render online and offline, paced animations, and click-to-zoom images. They are ADDED rather than MODIFIED, because the README, media and offline requirements they sit beside are still in `mvp-documentation`'s unarchived delta.

## Non-goals

- Zoom in the README on GitHub or PyPI. GitHub already opens a README image full size when you click it, and PyPI allows no scripts.
- Zoom in the app (Replay, History and the others) or in the hosted demo.
- New diagrams, or redrawing the existing one.
- Changing what any animation shows (its manifest entry and steps). Only the pacing changes.
- The theme's built-in lightbox (glightbox) and video player. Both load from public CDNs, and the offline archive can't.

## Impact

- `README.md`: the screenshot table becomes three captioned rows. `tests/test_docs_readme.py` keeps passing, and a new test checks the layout.
- `mkdocs.yml` (shared by `mkdocs.dev.yml`, `mkdocs.pre.yml` and `mkdocs.offline.yml`): the `mermaid` custom fence, plus the zoom script and stylesheet. `docs/overrides/main.html` loads Mermaid on pages with a diagram.
- `tools/docs-media.py`: paced recording, a new `vendor` step that fetches the pinned Mermaid build by SHA-256, and duration and first-frame checks.
- `.github/workflows/ci.yml` (`docs-site`) and `docs.yml` run the vendor step before the build.
- New browser tests for zoom and diagram rendering, and new unit tests for the pacing checks and the vendor step, all in new files.
- Archive: the four requirements have new names, so this change can archive before or after `mvp-documentation`.
