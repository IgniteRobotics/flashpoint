## Why

At announce (2027-01-04), the docs decide whether an outside team tries Flashpoint and stays. Today the docs are Markdown in the repo, with no images and no site, and the README opens with a dictionary definition. Nothing tells a REV, non-Pro or AdvantageKit team what they'll get. The robot-side guide never says how to make sure hoots get written. Josh's bar: really solid docs, with screenshots, animations, how-tos and a "Why Flashpoint" pitch.

This is the documentation part of the MVP public release roadmap (#68, change `mvp-public-release`), milestones M1–M4, after phases P0–P6 in `06-roadmap.md`. It documents decisions D4, D5, D6, D8, D9 and D11 (`05-target-architecture.md` §6) without changing any; #70 amends D9 in `mvp-install-and-first-run`. It covers issues #82, #83 and #84, plus the doc parts of #6, #7, #9 and #8. Pitfalls from `03`: none.

## What Changes

- **Docs site:** a versioned GitHub Pages site built from `docs/guide/`. CI deploys it on release tags, plus an unreleased `dev` version. The build is strict, and each release ships an offline copy.
- **Diátaxis structure:** Why Flashpoint; Quickstart with sample logs; Install per OS (uv, #70); a first-event tutorial; how-tos; reference (CLI, config files, views); Robot side; support matrix and limits; Troubleshooting/FAQ; offline install; privacy; Contributing.
- **Generated media:** one command captures screenshots and short animations of every view and main flow from a corpus-v1 sample lake, at a fixed viewport, theme and clock. It reruns each release; no image ships in the wheel or sdist.
- **Hosted demo:** a static Replay export of sample matches.
- **README rewrite:** a pitch, screenshots at absolute URLs (so PyPI renders them), a works-with/needs block, a short quickstart, and links to the site. The repository description, topics and homepage are set (admin).
- **Robot-side "What to log" (#83):** a ranked checklist, and samples as real files in `examples/robot-java/`, each with a "tested with" line and a compile check.
- **Support matrix and limits (#84):** device and log-setup tables (untested rows say so), first-30-minutes troubleshooting, an offline install recipe, and a privacy note.
- **Drift checks extended:**
  - site-wide link checks;
  - CLI option tables generated from the CLI;
  - every documented `flashpoint` command parses;
  - every config key and every view is documented;
  - every app docs link resolves.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `project-documentation`:
  - modified: README, user guide, automatic import guide, CLI reference, and drift checks;
  - added: the site and its structure, Why page, quickstart, tutorial, how-tos, reference, media, demo, robot side, support matrix, troubleshooting, offline install, privacy, maturity labels, repository listing.

## Non-goals

- Behaviour fixes (`mvp-install-and-first-run`, `mvp-outside-team-logs`). Code changes here are limited to help text and docs tooling.
- A robot-side library or vendordep, and C++ or Python samples.
- Translations, a blog, and hosted History or Anomalies demos (static export is Replay only).
- Claiming REV, Phoenix 5 or Systemcore support. The matrix names each as unsupported.
- The release workflow, CONTRIBUTING, issue templates, and the Chief Delphi post (#79–#81), which link to this site, media and demo.

## Impact

- New:
  - site config;
  - a `docs` Poetry group;
  - `tools/` scripts for media and the CLI reference;
  - a docs CI job and a Pages workflow;
  - an ADR for the docs toolchain.
- `docs/guide/` is restructured, `README.md` rewritten, and `examples/robot-java/` becomes a small Gradle project.
- Tests: `tests/test_docs.py` extended; `tests/test_package.py` checks no media is packaged.
- Admin: Pages from `gh-pages`, and the repository metadata.
- `mvp-install-and-first-run` (#72) takes its docs base URL and page paths from this site.
