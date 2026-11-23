## 1. README screenshots, one per row (design D1)

- [x] 1.1 Test first, in a new file (`tests/test_docs_readme_layout.py`, fixtures in `tests/fixtures/readme/`): an image inside a table row fails, naming the line. Each of the Replay, History and Anomalies screenshots sits alone in its paragraph, after a line naming its view, and the Anomalies line carries "beta". Verify: the fixture cases pass, and the README case fails on today's README.
- [x] 1.2 Rewrite the README's screenshot table and view list as three captioned rows (Live CAN (experimental) stays a text line). Run `audience-style:de-slop` on the changed lines. Verify: 1.1 passes, and so do `tests/test_docs_readme.py`, `tests/test_docs_maturity.py` and a `0.3.0rc1` pin plus `check-release-links.py` on a scratch copy.

## 2. Diagrams render, served by the site itself (design D2)

- [x] 2.1 Test first, in a new file: `docs-media.py vendor` writes the pinned `mermaid.min.js` when its SHA-256 matches, and exits non-zero naming the file and both sums when it doesn't (fake download, as in `test_docs_media_samples.py`). `check-site` fails a built site whose page has a `mermaid` block and no `assets/vendor/mermaid.min.js`. Verify: both fail before 2.2.
- [x] 2.2 Add the `vendor` subcommand (pick the exact Mermaid 12 release the theme expects, and pin its version and SHA-256), gitignore `docs/guide/assets/vendor/`, and extend `check-site`. Verify: 2.1 passes.
- [x] 2.3 Add the `mermaid` custom fence to `mkdocs.yml`, and the `scripts` block override in `docs/overrides/main.html` that loads the local copy only on pages with a diagram (front-matter fallback if `page.content` isn't testable). Verify: the strict build passes, and only `how-it-works` HTML references `assets/vendor/mermaid.min.js`.
- [x] 2.4 Browser test (new file, `browser` marker): build the site and the offline archive, then open How Flashpoint works over loopback and from `file://` with every other host blocked. The pipeline diagram is an SVG, its source text isn't visible, the console is clean, and no request goes to unpkg.com. A page without a diagram doesn't request the renderer. Verify: it passes.
- [x] 2.5 Run `vendor` in the `docs-site` CI job and in `docs.yml`'s build job, before the build. Verify: the workflow tests pass, and a `docs.yml` dry run on the PR builds with the diagram. Done: #117's dry run (run 45259899070) vendored the renderer, made 15 of 15 entries, and `check-site` found 0 diagrams without a renderer.

## 3. Paced animations (design D3)

- [x] 3.1 Test first, in a new file: `check_animation` fails a clip under 6 s, naming the entry and its length. The first-frame check fails a synthetic clip with a near-uniform first frame (ffmpeg `color` source plus one line of text) and passes a busy frame. The step helpers mark and pause only when `pace` is on (fake page records the calls). Verify: they fail before 3.2.
- [x] 3.2 Implement the trim from the ready point, the paced step helpers (`START_HOLD_MS`, `MARK_MS`, `STEP_PAUSE_MS`, `HOLD_MS` 3 s), `ANIMATION_MIN_S`, and the first-frame check. Calibrate its threshold on today's live clips (they must fail) and the new ones (they must pass). Verify: 3.1 passes, and the existing `tests/tools/test_docs_media.py` cases (unit and `-m browser`) still pass.
- [x] 3.3 Run a full local `capture` against the corpus and samples-v1. Verify: all four flows are 6 to 20 s, start on a loaded view, show each click marked, and fit the WebM and GIF budgets. Write the durations and sizes into this task. If `flow-ingest-to-replay` is still short, add the hover step (design D3) and re-run. Result (local capture, 2026-11-23, 15 of 15 entries): `flow-ingest-to-replay` 7.4 s, WebM 295 KB, GIF 2971 KB; `flow-history-motor` 7.3 s, 305 KB, 2511 KB; `flow-anomaly-triage` 9.7 s, 315 KB, 2142 KB; `flow-live-can` 6.2 s, 250 KB, 2596 KB (no clicks, so no marks). Reel 30.6 s. First frames 77–87 % one shade (today's loading frames: 99.7 %). No hover step needed. Note: Replay already shows Q7's details before the click, so that flow's click changes little on screen; changing what it shows is a non-goal here.

## 4. Click to zoom (design D4)

- [x] 4.1 Browser test first, in a new file: on the built home page, clicking the Replay screenshot opens a dialog whose image fills the viewport (whole frame visible), and clicking it closes the dialog at the same scroll position. Enter opens and Esc closes, with focus back on the image. The Why page's animation opens and keeps playing (`currentTime` advances). The logo and linked images don't open. From the offline archive with the network blocked, zoom works and makes no outside request. Verify: it fails before 4.2.
- [x] 4.2 Add `zoom.js` and `zoom.css` under `docs/overrides/assets/` through `extra_javascript` and `extra_css`. Verify: 4.1 passes, the strict build and `check-site` pass, and the page works in the dark palette (screenshot check).

## 6. Docs home screenshots, one per row (found at the 5.3 check)

- [x] 6.1 Test first, in a new file (`tests/test_docs_page_layout.py`, fixtures in `tests/fixtures/docs-layout/`): an image in a `grid` block or a table row on any docs page fails, naming the page and line; the docs home's Replay, History and Anomalies screenshots each sit alone after a line naming the view, Anomalies with "beta". Verify: the fixture cases pass, and the home page fails today.
- [x] 6.2 Rewrite the docs home's screenshot grid and summary sentence as three captioned rows (the README's wording). Verify: 6.1 passes, the strict build and `check-site` pass, the zoom and diagram browser tests pass, and a screenshot of the built home page shows one screenshot per row.

## 5. Decision docs and wrap-up

- [x] 5.1 Amend ADR-0019's feature subset with the `mermaid` custom fence, `extra_javascript`/`extra_css` and the `scripts` override (dated), and update the `docs/rewrite/` toolchain table if it lists the subset. Verify: the link check passes, and a mentor confirms the amendment. Confirmed by Josh (mentor), 2026-11-23.
- [x] 5.2 Run the full gate: `ruff`, `mypy src tests tools`, `pytest`, `-m browser`, the strict build, `check-site`, and `openspec validate docs-visual-polish --strict`. Verify: all pass.
- [x] 5.3 [HUMAN] After the next `dev` deploy, Josh checks the live README, How Flashpoint works, the Why page's animation and zoom on a phone and a laptop. Verify: his OK is recorded on the PR. Done: Josh checked the dev deploy (runs 45261839482 and 45270249189, after #117 and #118) and approved it on 2026-11-23 ("looks good").
- [ ] 5.4 Run `openspec archive docs-visual-polish` as the PR's last commit. The four requirements are ADDED under new names, so they append to `project-documentation` whether or not `mvp-documentation` has archived. Verify: `openspec validate --all` passes.
