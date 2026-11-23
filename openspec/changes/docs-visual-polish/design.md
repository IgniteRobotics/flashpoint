## Context

See proposal.md (Why) and the delta spec for the four requirements. The constraints that shape the approach:

- **ADR-0019, the feature-subset rule:** `mkdocs.yml` uses only features both Zensical and Material 9.8 support, and allows template overrides but no plugins. Today's subset lists `pymdownx.superfences` with no options and an override of the `announce` block only.
- **Our superfences config replaces the theme's default.** Zensical's built-in config has `custom_fences: [{name: mermaid, class: mermaid}]`. Ours lists `pymdownx.superfences` bare, so the pipeline block comes out as highlighted code.
- **The theme loads Mermaid and its lightbox (GLightbox) from unpkg.com, but only when they aren't already loaded:** `typeof mermaid == "undefined" ? load("https://unpkg.com/mermaid@12/...")`, and the same for `GLightbox`. A local copy loaded first is used instead.
- **The offline archive** (`mkdocs.offline.yml`, `use_directory_urls: false`, no web fonts) must work from disk with no network ("Offline copy of the docs", in `mvp-documentation`'s delta).
- **The theme has no `navigation.instant`.** Every page is a full load, so a script can run once per page.
- **Recording today** (`tools/docs-media.py record`): Playwright records from the moment the browser context is created. `page.goto`, the steps (one instant `click()`) and `HOLD_MS = 1500` follow. The four flows measure 1.7 s to 3.6 s, and frame 0 is the app's `Loading Flashpoint…` notice (`#app .notice` in `index.html`).

## Goals / Non-Goals

**Goals:**
- Fix each defect in a way the docs build checks automatically: README layout by a unit test, diagram rendering and zoom by browser tests, and pacing by checks in `capture`.
- No new runtime host: everything the site needs is served from the site itself.

**Non-Goals:**
- Changing the feature subset beyond what these four fixes need. ADR-0019's list is amended in place (see D5), with no new ADR.
- A general asset vendoring system. One pinned file is fetched.

## Decisions

### D1. README: captioned rows
Each of Replay, History and Anomalies (beta) becomes a short paragraph naming the view and what it shows, followed by its image on its own line. The bullet list that described the views today merges into those captions, so nothing is said twice. Live CAN (experimental) stays a text line, because the spec caps the README at three screenshots. A unit test (new file) refuses an image inside a table row, naming the line, and checks that the line before each view image names the view.
*Alternative:* an HTML `<img width=…>` grid. Rejected: GitHub and PyPI both render plain Markdown images full width, and HTML would need `pin-readme-media.py` to keep handling the `<img>` form.

### D2. Mermaid: the fence plus a pinned local renderer
- Add `custom_fences: [{name: mermaid, class: mermaid, format: !!python/name:pymdownx.superfences.fence_code_format}]` to `mkdocs.yml`, Material's documented form, which Zensical also reads.
- A new `tools/docs-media.py vendor` step downloads one exact Mermaid release's `mermaid.min.js` (version and SHA-256 pinned in the tool, like the samples-v1 zips) into `docs/guide/assets/vendor/`, which is gitignored. It exits non-zero on a checksum mismatch, naming the file and both sums.
- `docs/overrides/main.html` overrides the `scripts` block: when the page's HTML contains `class="mermaid"`, it emits a `<script>` for the local copy before `{{ super() }}`. The theme then finds `mermaid` defined and never calls unpkg. If the template engine can't test `page.content`, the fallback is a front-matter flag (`diagram: true`) on the pages that have one.
- `check-site` gains a check: a built page with a `mermaid` block and no `assets/vendor/mermaid.min.js` in the site fails the build. This is how "can't provide the renderer" fails before deploy.
- The `docs-site` CI job and `docs.yml`'s build job run `vendor` before the build.

*Alternatives:*
- **Let the theme load unpkg.** Rejected: the offline archive shows the diagram as source, and the site depends on a third-party host.
- **Commit a pre-rendered SVG.** Rejected: one fixed colour scheme in a site with a light/dark toggle, a node toolchain to regenerate it, and drift between the source and the picture.
- **Commit `mermaid.min.js` (about 3 MB).** Rejected: a binary-sized blob in git history. Fetched by checksum, the build is reproducible without it.

### D3. Paced, trimmed recordings
- **Start:** `record` notes the time the start view is ready: the loading notice is gone and the manifest's start condition is met. After the context closes, it trims the WebM from that point (`ffmpeg -ss`, re-encoding VP8), so frame 0 is the loaded view.
- **Pace:** steps get a `pace` flag (on only when recording). A paced step:
  1. holds the start view for `START_HOLD_MS` (2 s);
  2. for each click, scrolls the target into view and outlines it with an injected highlight class for `MARK_MS` (0.8 s);
  3. clicks, then pauses for `STEP_PAUSE_MS` (1.5 s).

  The end state is held for `HOLD_MS`, raised to 3 s. Screenshots run the same steps unpaced, so they aren't slowed down.
- **Checks:** `check_animation` adds `ANIMATION_MIN_S = 6` next to the existing 20 s maximum. A first-frame check samples frame 0 and fails when it's near-uniform (the loading notice is one short line on a flat background). The threshold is calibrated on today's clips, which must fail, and on the paced ones, which must pass.
- **Too-short flows:** `replay-first-match` has a single click. If its paced clip is still under 6 s, its steps gain a hover over the match list before the click. That's a pacing change, not a change to what the flow shows.

*Alternative:* `page.screencast` / CDP frame capture from the ready point. Rejected: more code than one trim, and Playwright's recorder is already proven in CI.

### D4. Zoom: a small theme script, not GLightbox
- `docs/overrides/assets/javascripts/zoom.js` and `assets/stylesheets/zoom.css` are added through `extra_javascript` and `extra_css`.
- On load, the script marks each `img` and `video` in `.md-content` that isn't inside a link or an icon: `tabindex="0"`, `role="button"`, and an `aria-label` from the alt text.
- **Open:** a click, Enter or Space opens a single shared `<dialog>` with `showModal()`. It holds a clone of the element: an image at `max-width: 100vw; max-height: 100vh; object-fit: contain`, or a video with `autoplay loop muted playsinline`. The alt text is the caption.
- **Close:** a click anywhere in the dialog, the close button, or Esc (the dialog's native cancel). Focus goes back to the source element. A modal dialog doesn't scroll the page, so the scroll position holds.

*Alternative:* the theme's GLightbox. Rejected: it loads from unpkg (vendoring it is a second pinned file), its video slides need Plyr from another CDN, and by default clicking the opened image zooms it rather than closing it, which fails "and vice versa".

### D5. ADR-0019's feature subset is amended, not deviated from
The `mermaid` custom fence, `extra_javascript`/`extra_css`, and an override of the `scripts` block are all features Material 9.8 supports with the same config. The fallback to Material stays a dependency swap. ADR-0019's subset list gets these three items added, with the date.

## Risks / Trade-offs

- **The paced GIFs grow:** an 8–12 s clip at 12 fps and 960 px can pass the README GIF's 3 MB budget. → Mitigation: `capture` already retries over-budget GIFs at 10 fps and 800 px. If that's still over, the GIF (README only) drops to 8 fps. The site uses the WebM, whose 2 MB budget holds at these lengths.
- **A Mermaid major version could change rendering** → it's pinned by version and checksum, and bumped by a deliberate PR (the same policy ADR-0019 sets for the mike fork).
- **The near-uniform first-frame heuristic could miss a loading screen that changes** → the trim (D3) is the real fix. The check is a backstop, calibrated in a test against today's loading frame.
- **The zoom script could fight theme updates** → it touches only `.md-content` media and its own dialog, and a browser test covers open and close on every Zensical bump.
- **Capture time:** pacing adds about 10 s per flow, about 40 s per deploy. That's well inside the 20 min deploy budget (8.7: 5 min today). No full-log processing changes, so memory budgets are unaffected.

## Migration Plan

Docs-only. It deploys with the next nightly or manual `dev` deploy. To roll back, revert the PR and redeploy `dev`.
