## Context

See proposal.md for the motivation, and `specs/project-documentation/spec.md` for the behaviour. What exists today:

**Docs:**
- `docs/guide/` has nine Markdown pages: README, user guide, team setup, auto import, robot side, live CAN health, CLI reference, how it works, and developer guide (about 1,900 lines). There's no site, and no images.
- `README.md` opens with a dictionary definition and links the guides at `blob/main`.
- `media/flashpoint.png` is an unused banner.
- `pyproject.toml` `Documentation` points at `blob/main/docs/guide/README.md`.

**Drift tests (`tests/test_docs.py`):**
- relative links in the README, the deploy README and `docs/guide/*.md` (top level only);
- the README must use absolute links, and its `blob/main` paths must exist;
- every subcommand from `flashpoint.cli._parser()` must have a ``## `flashpoint <name>` `` heading in `cli-reference.md`. Three arguments have no help text today: `ingest`'s `paths` and `--profile`, and `rebuild`'s `--profile` (`cli.py:81-82,95`);
- the live CAN contract table matches `flashpoint.live.contract`.

The CI `docs` job runs these when only docs changed. Otherwise they run in `lint-test`. `ci-ok` is the only required check.

**Browser tests:** Playwright and Chromium (`pytest -m browser`, the `browser` CI job). They serve a lake with `FlashpointServer` on `127.0.0.1:0`, block every non-loopback request, and fail on console errors (`tests/web/test_browser.py`). The capture tooling reuses this pattern.

**Corpus:** corpus-v1 is a public release on this (now public) repo.

Groups from `tests/corpus/manifest.toml` (checked 2026-11-14):

| Group | Files | Notes |
|---|---|---|
| `2026-gacmp-q7` | wpilog, CANivore hoot and rio hoot | About 79 MB |
| `2026-gacmp-e10` | wpilog and four hoots (split across a restart) | About 27.5 MB. The two 19-29-15 hoots were cut short by the event's power-off and ingest with an `incomplete-read` warning (`tests/test_ingest_corpus.py:45`) |
| `2026-gacmp-p2` | wpilog only | About 56 MB |
| `2025-gacmp-q19` | CANivore and rio hoots only, no wpilog | Compliancy 13, needs owlet 25.x |
| `2025-gadal-q30` | 2025 wpilog only | |
| `2025-nofms` | wpilog with no event or match in its name | Non-FMS fallback |
| `2026-gadal-q11-empty` | a 0-byte hoot | The only corpus file ingest quarantines (`empty-file`) |
| `2026-gacmp-e10-truncated` | `…truncated-60pct.hoot` | A 60% copy of the E10 rio hoot. Ingest **succeeds** with a `short-coverage` warning; it is not quarantined |

The app marks incomplete logs with an `INCOMPLETE` badge (`web/static/shell.js:280`). Quarantined files appear only in ingest and `doctor` output, not in the app.

The repo's `config/` holds Ignite's robot files for these logs.

**Pages:** not enabled on this repo (`gh api repos/IgniteRobotics/flashpoint/pages` gives 404, checked 2026-11-14).
- `ignition-labs` publishes with MkDocs 1.6.1 and Material 9.7.7, through a Pages workflow.
- Org rulesets cover only `main` and `development`, so a `gh-pages` branch is unprotected.

**Docs links in the code today** are repo-relative strings: `setup/checks.py:44` and `:480` (`see docs/guide/auto-import.md…`) and `web/static/views/live-can.js:19`. `mvp-install-and-first-run` (#72) replaces them with `flashpoint/docs_links.py` (`docs_url(page_id, section=None)` and a read-only `GET /api/docs`; the shipped JS holds no address).

**Size limits:**
- the wheel must stay under 5 MB (`tests/test_package.py`, `release.yml`);
- the static app ships no images.

**Sibling changes this depends on:**
- `mvp-install-and-first-run`:
  - #70: the uv install;
  - #72: the docs-URL constant;
  - #73: 2027 status;
  - #74: the robot-pull "beta" label;
  - #8: the hoot-only warning.
- `mvp-outside-team-logs`:
  - #75: the non-Pro wording;
  - #76: AdvantageKit identity;
  - #77: the Anomalies beta label;
  - #78: outside-log results.
- `mvp-public-release`:
  - #79: CONTRIBUTING and Discussions;
  - #80: the release workflow and checklist;
  - #81: the announcement, which consumes this change's media.

**Architecture decisions this change documents (`05-target-architecture.md` §6), none changed here:**
- D4: owlet is fetched and cached on first use, so the offline install and "ingest one hoot at home first" pages exist.
- D5: temperature comes from Pro hoots; the matrix, README and robot-side Pro page say so (#75's wording).
- D6: the static Replay export (payload ≤ 2 MB per match) is the hosted demo.
- D8: AdvantageScope is linked, not rebuilt; the Why page says so.
- D9: per-user services. `mvp-install-and-first-run` amends D9's install tool to `uv tool install` with an ADR (its D-a); the Install pages document what it lands.
- D11: serial identity needs the robot-logged CAN inventory, so the Why page qualifies the "follows each motor" claim and outside teams get legacy epochs (matrix and robot-side pages).

## Goals / Non-Goals

**Goals:**
- One Markdown source that reads well on GitHub and builds the site, with no generator-specific syntax that GitHub renders as garbage.
- Anything that can drift from the code is generated from it, or checked against it, by a unit test that needs no network and no corpus.
- Media is regenerated for every release from public data, so screenshots never lag the UI and never show private team data.
- Switching docs generators is a dependency change, not a content rewrite.

**Non-Goals:**
- Generator plugins: no MkDocs plugins and no Zensical modules. Generated content is committed Markdown.
- A custom domain, analytics, or comments on the site.
- Hosted History or Anomalies demos. That needs a server, or a static export of those views.
- Screenshot pixel-diff testing. Freshness comes from regenerating, not comparing.

## Decisions

### D-a. Generator: Zensical, with pinned MkDocs 1.6.1 + Material 9.8.x as the fallback
**Facts checked 2026-11-14 (web; re-check at task 1.1):**
- Material for MkDocs is in maintenance mode ("no longer receives new features"). Its changelog notice says critical bug fixes and security updates continue through **2027-05-05**; 9.8.0 (2026-10-24, a security release) moved the CLI end-of-life notice to that date, and 9.8.1 shipped 2026-11-07. (An earlier announced date was 2026-11-05.) `ignition-labs` is on 9.7.7.
- MkDocs 2.0 is incompatible with Material: TOML config, and the plugin and template system replaced.
- The Material team's successor is **Zensical** (MIT; 0.1.1 on PyPI, 2026-11-10, "Development Status :: 4 - Beta"; 0.1.0 was 2026-11-05).
  - It builds existing `mkdocs.yml` projects.
  - It has built-in link and anchor validation with `zensical build --strict` (since 0.0.38).
  - It supports versioned GitHub Pages through a mike fork, a stopgap until native versioning, with every mike command unchanged. The fork is based on mike 2.2.0, gets compatibility fixes only, and is installed from GitHub (`git+https://github.com/squidfunk/mike.git`), not PyPI. So it is pinned to a commit SHA in the `docs` group, and Dependabot won't bump it.
  - It has no public module (plugin) API yet.

**Choice:** Zensical, pinned to an exact version (and the mike fork to a commit) in a new optional Poetry group `docs`. The group also holds Pillow and `imageio-ffmpeg` for the media pipeline (D-f). Config stays in `mkdocs.yml`, restricted to features that Zensical and Material 9.7 both support:
- `navigation.tabs`, `navigation.sections`, `navigation.top`;
- `search`;
- `content.code.copy`;
- `content.tabs.link` with `pymdownx.tabbed` (OS tabs);
- `admonition`, `pymdownx.details`, `pymdownx.superfences`;
- `attr_list`, `md_in_html` (the video tag), `tables`;
- `toc` with permalinks;
- the palette toggle;
- `extra.version.provider: mike`.

No plugins.

**Fallback trigger:** if the task 1.1 spike shows any of the following, switch the `docs` group to `mkdocs==1.6.1`, `mkdocs-material==9.8.*` (latest patch, pinned) and `mike==2.*` from PyPI, using the same `mkdocs.yml` and `mkdocs build --strict`:
- the strict build doesn't fail on a broken link and a broken anchor in a fixture page;
- the mike fork can't deploy two versions plus `latest` and `dev` to a scratch `gh-pages`;
- search doesn't work under a sub-path;
- content tabs don't work.

Material's fixes end 2027-05-05, after the MVP. If we fall back, move to Zensical before that date (post-MVP follow-up issue). A builder past its fixes is tolerable for a static-output tool that runs only in CI on our own content, but it is not the plan.

**Rejected:**
- Sphinx + Furo + MyST. It's maintained and stable, but it needs every page converted to MyST idioms and the navigation moved into toctrees. It has no maintained versioning (sphinx-multiversion is stale), and it's a second toolchain next to ignition-labs.
- MkDocs 2.0. No Material theme, and its ecosystem is in flux.
- Docusaurus or Starlight. Node-based, and the repo standard is Python-native.

**ADR:** this is a new decision, not a change to D1–D11. Task 1.1 writes `docs/adr/00NN-docs-site-toolchain.md`, taking the next free number at apply time, since sibling changes may also add ADRs.

### D-b. Site layout and URLs
**Source:** `docs/guide/` stays the site's `docs_dir`, so existing paths and tests keep working.

**URL:** `https://igniterobotics.github.io/flashpoint/<version>/<page>/`, with directory URLs:
- `<version>` is `0.3`, `latest` or `dev`;
- a page's path is its source path without `.md`. For example, `how-to/back-up-the-lake.md` is `/latest/how-to/back-up-the-lake/`.

**Links to repository files outside the site:** `config/`, `deploy/`, `docs/adr/` and `docs/rewrite/` aren't site pages. Links to them use absolute `https://github.com/IgniteRobotics/flashpoint/blob/main/...` URLs. `main` equals the latest release at announce (`mvp-public-release`). The existing README check, that a `blob/main` path exists, is extended to all pages.

**Contract with #72** (as `mvp-install-and-first-run` design D-b states it):
- `flashpoint/docs_links.py` holds the base URL and a map from page id (`auto-import`, `live-can-health`, …) to page path; the CLI, checks and server call `docs_url(page_id, section)`, and views get addresses from `GET /api/docs`.
- Version segment: `X.Y` for a final `X.Y.Z` install and for its pre-releases (`rcN`, `aN`, `bN`), since both tags deploy `X.Y` (D-c); `dev` for development builds (`.devN`, local).
- This change owns the page paths. It fixes them with the D-d tree in one early PR, merged by 2026-11-28, and after that nobody moves guide files (`mvp-public-release` design). If `docs_links.py` has landed before that PR, the PR updates its page-path map in the same commit; that is the only `src/` edit this change makes outside help text.
- Until the first final release with a site (0.3.0), the site root's `latest` is an alias of `dev`.

### D-c. Versioning and deploy
**Workflow:** a new `.github/workflows/docs.yml`, separate from `release.yml`, so a docs failure can't block a PyPI publish and the reverse.

| Trigger | Deploys | `latest` |
|---|---|---|
| final tag `vX.Y.Z` | `X.Y` | moves to `X.Y` when `X.Y` is the highest final version (`mike deploy --update-aliases X.Y latest`) |
| pre-release tag (`vX.Y.ZrcN`, `aN`, `bN`) | `X.Y`, with a "pre-release" banner (the same overlay as `dev`) | unchanged |
| nightly schedule, when `development` moved since the last deploy | `dev` | unchanged |
| `workflow_dispatch` | `dev` | unchanged |

`mike set-default latest`. Permissions: `contents: write` for the `gh-pages` push only.

**Why pre-release tags deploy `X.Y`:** `mvp-public-release`'s release-build gate requires README images under the version path of the version being built (major.minor), and `0.3.0rc1` (about 12-07, the outside-mentor beta build) goes to PyPI. Without a `/0.3/` deploy its package page would show broken images until 12-19. The tag-to-version-and-alias rule is one small function in `tools/docs-version.py`, unit tested (task 3.1). In-app links from the rc go to the same `/0.3/` (#72's rule follows this table), so a beta tester reads docs frozen at the build they installed.

A run goes in this order:
1. Strict build.
2. Media capture (D-f).
3. Demo export (D-h).
4. Offline archive.
5. Push.

Any failure stops the run before the push, so published versions stay as they were.

**Unreleased banner:** the `dev` build sets `extra.dev_banner` through an environment-specific config overlay (`mkdocs.dev.yml` with `INHERIT`; verified in the spike). The overlay renders an admonition banner via the theme's `announce` block. If the overlay isn't supported, a generated include at the top of each page does it.

**Why nightly for `dev`:** every deploy commits a full copy of the version to `gh-pages`. Per-push deploys would grow its history fast. `gh-pages` is a disposable deploy branch. When it passes 300 MB, a manual workflow replaces its history with one commit of the current tree.

**Pages** is enabled from the `gh-pages` branch (admin, [HUMAN]). Limits: a 1 GB site, and a soft 100 GB/month of bandwidth. See the budgets below.

**Offline archive:** a second build of the same version with `use_directory_urls: false`, so links work from `file://`, zipped as `flashpoint-docs-X.Y.zip`.
- It's attached to the GitHub Release by the release workflow (coordinated with #80), and published at `/X.Y/flashpoint-docs-X.Y.zip`.
- Search may not work from `file://`. The offline page says so, and gives `python -m http.server` as the workaround. The spec requires only pages, navigation, links and images.

### D-d. Information architecture (Diátaxis) and where today's pages go
```
docs/guide/
  index.md                       home: pitch, 3 images, demo link, "start here" cards
  why.md                         Why Flashpoint
  quickstart.md                  own logs + "Try it with sample logs"
  install/  index.md macos.md linux.md windows.md        (uv-first per #70; pipx alternative; platform floor)
  tutorials/ first-event.md team-setup.md                (team-setup.md moved here)
  how-to/   set-up-a-robot-file.md import-from-usb.md auto-import.md back-up-the-lake.md
            move-to-another-laptop.md start-a-new-season.md replay-on-a-usb-stick.md
            share-on-the-pit-network.md live-can-health.md triage-an-anomaly.md
            open-in-advantagescope.md share-logs-privately.md update-flashpoint.md
            install-offline.md team-name-and-colours.md
  robot-side/ index.md (ranked "What to log") signal-logging.md datalog.md signal-rates.md
            pro-and-temperature.md build-info.md can-inventory.md bus-status.md
            battery-and-pdh.md disk-space.md advantagekit.md
  reference/ cli.md config/{acquire,theme,report,anomaly,anomaly-causes,failure-labels,seasons,robots,units}.md
            views/{replay,history,anomalies,setup,live-can}.md
  support-matrix.md              device, log-setup and platform tables + known limits
  troubleshooting.md  faq.md  privacy.md  demo.md  contributing.md   (nav: demo under Quickstart, privacy under Reference, faq under Troubleshooting)
  under-the-hood/ how-it-works.md developer-guide.md
  assets/media/                  gitignored; filled by capture or placeholders
```

**Moved pages:**
- `user-guide.md` is split across Install, Quickstart, the how-tos and the view references.
- `auto-import.md` and `live-can-health.md` become how-tos.
- `cli-reference.md` becomes `reference/cli.md`.

**Stubs:** each old top-level file stays as a three-line "moved to `<site URL>`" stub. It's excluded from the build (`exclude_docs`) and exempt from the navigation check. This keeps links from the 0.2.0 PyPI page (`blob/main/docs/guide/...`) working, and keeps the repo-relative paths that 0.2.0's `doctor` prints (`see docs/guide/auto-import.md`) pointing at a file that says where the page went. `docs/guide/README.md` becomes the stub index, and `index.md` is the site home.

**Contributing** summarises and links the repo's `CONTRIBUTING.md` and Discussions, which `mvp-public-release` #79 adds.

### D-e. Drift checks: generate what can be generated, test the rest
All of these are in `tests/test_docs.py` and friends. They need no network and no corpus, and they run on Linux and Windows (spec "Documentation drift checks").

- **Links:** `doc_files()` becomes recursive over `docs/guide/**/*.md`, minus stubs. The `blob/main` existence check applies to every page.
- **Navigation:** parse `mkdocs.yml` `nav` with a YAML load. Every non-stub page must be listed, and every nav entry must exist. PyYAML is already in dev through types-pyyaml; check that PyYAML itself is a dev dependency, and add it if not.
- **CLI reference:** `tools/gen-cli-reference.py` walks `_parser()` and writes an options table per subcommand between `<!-- generated:cli:<name> -->` markers in `reference/cli.md`. It fails on an option with no help text. The test calls the same function and compares its output with the committed file. The hand-written purpose and examples stay outside the markers. Positional arguments get rows too. Fix the three arguments with no help in `cli.py`: `ingest`'s `paths` and `--profile`, and `rebuild`'s `--profile`.
- **Commands parse:** every line in `sh`, `console` and `powershell` blocks whose command is `flashpoint ...` (after stripping `$`, env assignments, `poetry run` and `uv run`, and joining `\` continuations) goes to `_parser().parse_args()`. `SystemExit` is caught and reported with the page and line. `<placeholder>` tokens become `1`.
- **Config keys:** each reference page under `reference/config/` must mention every field, as `` `key` ``, of what loads that file:
  - `acquire.md`: `AcquireConfig`, `BackupConfig` (`acquire/config.py`);
  - `theme.md`: `ThemeConfig`, `CustomPalette` (`theme.py`);
  - `robots.md`: `RobotConfig`, `Slot`, `Bus`, `Signal`, `SiblingGroup` (`semantics/robot_config.py`);
  - `seasons.md`: `SeasonConfig`, `SeasonRoots`;
  - `report.md`: the `ReportConfig` dataclass fields and its `thresholds` and `advantagescope` tables (`report/settings.py`);
  - `anomaly.md`: the `_Section` subclasses in `anomaly/settings.py`;
  - `anomaly-causes.md` and `failure-labels.md`: `Cause` and `Label` (`anomaly/catalogue.py`);
  - `units.md`: the header of the bundled `units/units-seed.csv`.

  They're found by name, so a new model shows up as a test edit.
- **Views:** the view list comes from `src/flashpoint/web/static/views/*.js`. Each view needs `reference/views/<view>.md` and a manifest screenshot entry.
- **Java blocks:** each fenced `java` block under `robot-side/` must be a contiguous run of lines in some `examples/robot-java/**/*.java`, with trailing whitespace ignored.
- **Tested-with:** each `.java` file under `examples/robot-java/` must match `Tested with WPILib \S+, Phoenix 6 \S+`.
- **Troubleshooting messages:** each entry's ``Message: `...` `` span is split on `<placeholders>`. Each literal piece of 8 or more characters must occur in `src/flashpoint/**/*.{py,js}`.
- **App docs links:** until #72 lands, the repo-relative docs paths in `setup/checks.py` and `live-can.js` must name files that exist. Once `docs_links.py` exists, #72's own test checks every `docs_url` page and section against the source tree's pages and heading anchors; this change's test adds only that every path in its page map is listed in the site navigation (so a page that is excluded as a stub fails).
- **Support matrix:** `docs/guide/support-matrix.toml` is the data. `tools/gen-support-matrix.py` renders the three tables into `support-matrix.md` between markers, and a test checks the output is current. Cell values are the spec's six (the vocabulary `mvp-public-release` and `mvp-outside-team-logs` use: "rough" for a Windows workaround, "not tested on real logs" for synthetic-only coverage). Each "supported" cell's evidence must name a `tests/corpus/manifest.toml` group, a path under `tests/`, or an `outside:<yyyy-mm>` entry with an issue link; a "rough" cell names its workaround and issue. The outside logs are private, so the matrix never names the team.
- **Strict site build:** a new CI job `docs-site` runs on every PR, whatever paths changed, and is added to `ci-ok`'s `needs`, so no admin change is needed. Its steps:
  1. `poetry install --only docs` (the placeholder writer is stdlib only);
  2. `tools/docs-media.py placeholders`, which writes a tiny valid PNG, GIF or WebM for every manifest entry;
  3. a strict build.

Budget: under 60 s for the build, and under 8 min for the job.

### D-f. Media pipeline (`tools/docs-media.py`)
**Manifest:** `docs/media-manifest.toml`. Each `[[shot]]` entry has:
- `id`;
- `kind`: `screenshot` or `animation`;
- `view`;
- `route`: the app hash route;
- `steps`: a named step function in the script;
- `expect`: the state the shot must show, as a CSS selector that must be visible;
- `alt`;
- optional `mask`: selectors for volatile text;
- optional `budget_kb`.

**`capture` subcommand:**
1. Check corpus-v1 with the fetch script's own manifest check. A missing file exits 2, naming it and `python tools/fetch-corpus.py`.
2. Build a sample lake in a temp folder with the real CLI:
   - `FLASHPOINT_CONFIG` is a temp copy of the repo's `config/` (Ignite's robots), plus a `theme.toml`. The header reads "Ignite Robotics · 6829" for docs shots. The demo theme reads "Sample data · 6829 robot".
   - `ingest` of the sample groups (Q7, E10, P2, Q19, Q30). The empty hoot and the truncated E10 copy are left out: the truncated copy would add a second, partial copy of E10's rio data to the demo match. E10's own power-off hoots give the real `INCOMPLETE` badge for the troubleshooting shot;
   - then `derive`, `detect` and `report`.
3. Start `FlashpointServer` on `127.0.0.1:0` (in-process, as the browser tests do), and the live fake robot (`python -m flashpoint.live.fake_robot --scenario shake`) for Live CAN.
4. Open Chromium with these fixed settings:
   - viewport 1440×900, device scale 2;
   - `color_scheme="dark"`, the default amber palette;
   - `page.clock` set to 2026-04-12T12:00:00Z;
   - the browser-tests route filter: loopback only, any console error fails the shot.
5. For each entry: navigate, run its steps, wait for `expect`, and screenshot with the masks.
6. Encode PNGs with Pillow (`docs` group). A PNG over budget is quantised to 256 colours, and still over budget it fails.

**Animations:**
- Playwright `record_video_dir` at 1280×800 gives WebM (VP8) for the site.
- The bundled ffmpeg from `imageio-ffmpeg`, a `docs` group dependency so no system install is needed, converts it to a GIF: two-pass palette, 12 fps, 960 px wide. The GIF is for the README and Chief Delphi.
- Each animation is at most 20 s and at most 3 MB, or the command fails.
- The 60 s announcement video for #81 is made by joining the flow recordings with the same ffmpeg (`docs-media.py reel`).

**Determinism:** same corpus and code give the same file names. Bytes may differ (font hinting, video encoding), which is why freshness is "regenerated every deploy", not "byte-equal".

**Output:** `build/docs-media/` (gitignored), copied into `docs/guide/assets/media/` (gitignored) for the build. The PR job `docs-media` uploads it as a workflow artifact so reviewers can see new shots. It's non-blocking, and runs on PRs touching `src/flashpoint/web/`, `docs/media-manifest.toml` or `tools/docs-media.py`.

### D-g. Where images live: generated in CI, published only to `gh-pages`
| Option | For | Against |
|---|---|---|
| Commit them to the repo | Local preview works; PR diffs show images | About 10–15 MB of binary churn per release, forever, in every clone; stale shots get committed by hand |
| Git LFS | Keeps clones small | Bandwidth quotas; raw URLs don't render on PyPI |
| GitHub Release assets | Stable absolute URLs | Clutters releases (#80 wants clean ones); the site can't use relative paths |
| **Generated in CI, pushed with the site** | Source tree has no binaries; images are versioned with their docs; history growth is confined to a disposable branch | Local preview needs the corpus, or placeholders |

The last option is chosen.

**README images** reference `https://igniterobotics.github.io/flashpoint/latest/assets/media/<id>.png`. Before `poetry build`, the release workflow's build job runs `tools/pin-readme-media.py <version>` on `README.md`:
- docs-site `/latest/` becomes `/X.Y/` (pre-releases too; D-c deploys `X.Y` for them);
- `github.com/IgniteRobotics/flashpoint/blob/main/` and `tree/main/` become `blob/vX.Y.Z/` and `tree/vX.Y.Z/`.

So each PyPI page pins its release's images and files, and passes `mvp-public-release`'s "Release build refuses moving-branch documentation links" check, which runs after it in the same job (#80 owns `release.yml`; the step is added with them). A unit test checks the rewrite on a fixture README.

**Packaging:** the wheel and sdist never contain media. Poetry excludes gitignored files from the sdist. `test_package.py` and the `release.yml` check assert that there are no `.png`, `.gif`, `.webm` or `.mp4` files.

### D-h. Hosted Replay demo
The `docs.yml` deploy runs `flashpoint report --static <site>/demo --no-raw --match <key>...` against the sample lake, with the demo theme. Static export is offline by design (D6), and its relative paths work under a sub-path.
- Budget: at most 15 MB (at most 2 MB per match per D6, five sample groups, plus the app shell).
- A non-zero exit or a missing match stops the deploy.
- `demo.md` explains the demo and links `demo/index.html`.
- A browser check in the same job opens the built demo with non-loopback requests blocked, and asserts that the matches are listed with no console errors.

### D-i. Sample bundle for "Try it with sample logs"
`tools/build-sample-bundle.py` writes `flashpoint-sample-1.zip` from corpus-v1 Q7, a full qualification match (its wpilog and two hoots; 55.4 MB), together with:
- `config/robots/2026-comp.toml`;
- `config/seasons/2026.toml` (Ignite's: the robot file's signals use its `robot` root, which the bundled default season leaves unset) and a one-line `config/acquire.toml` (`team = 6829`; without an `acquire.toml` the folder doesn't count as a configuration and nothing is mapped). Both found by running the bundle, 2026-11-21;
- a `README.txt`;
- `SHA256SUMS`.

It's published once as an asset of a `samples-v1` GitHub release, marked not-latest like the corpus ([HUMAN] maintainer upload). Its URL and SHA-256 go into `quickstart.md`.

**Why Q7, not E10** (changed 2026-11-21, Josh). E10 was first chosen as the smaller match (15.1 MB). Running the tutorial on it showed that its robot code restarted mid-match: the wpilog covers only the pre-match minutes, and the match itself is a hoot-only session. So it has no match phases, History and Anomalies stay empty, and Replay shows a robot that was never enabled. Q7 is enabled throughout, aligned by payload match, and detection finds two MED anomalies in it. Ingest notes the bus-status signals that Ignite's robot file declares and Q7's robot code didn't log yet; the Quickstart says that note is expected.

The Quickstart commands set `FLASHPOINT_CONFIG` to the bundle's `config/`, and pass `--lake` to the bundle's `lake/`. Settings files missing from that folder fall back to the bundled defaults, so the reader's own config and lake are untouched. The commands are given in `sh` and PowerShell tabs.

**Rejected:**
- A `flashpoint try` command. That's new CLI surface in a docs change; post-MVP if the beta asks for it.
- Bundle files on Pages. That's about 25 MB duplicated per version, against the 1 GB site limit.

### D-j. README
Structure:
1. One-line pitch.
2. Hero GIF (ingest → Replay → History).
3. Three screenshots.
4. "Works with / needs".
5. Quickstart: the install command (`uv tool install --python 3.13 flashpoint`, per #70), then the three commands `flashpoint init --team <n>`, `flashpoint ingest <folder>`, `flashpoint serve`. This is the "3-command quickstart" of #82; `ingest` works without `init` (it warns), but `init` sets the team and robot addresses.
6. "Try it": the demo and the sample logs.
7. Docs links.
8. Project and license.

All links are absolute. Labelled features carry their label.

**Repository metadata** (description, topics, homepage) is set with `gh repo edit` by an admin ([HUMAN]). `pyproject.toml` `Documentation` and `Homepage` point at the site.

### D-k. Robot-side samples as a compiling project
`examples/robot-java/` becomes a minimal GradleRIO project:
- `build.gradle` and `settings.gradle`;
- the Gradle wrapper;
- `vendordeps/Phoenix6-<ver>.json`;
- `src/main/java/frc/robot/{Main,Robot}.java`, wiring every sample in `robotInit()`. This is the one-file usage example;
- `frc/robot/flashpoint/` with `CanInventoryLogger`, `CanBusStatusLogger`, `BuildInfo`, `SignalRates`, `SignalLoggerSetup` and the moved `CanHealthPublisher`.

`examples/robot-java/CanHealthPublisher.java` is updated in links and in the live-can-health test. Each file carries "Tested with WPILib 2026.x.y, Phoenix 6 26.x.y".

**Compile check:** `./gradlew compileJava` (JDK 17, network for Maven) is a release-checklist step, task 7.4. A non-blocking CI workflow `robot-samples.yml` (`actions/setup-java` 17, on PRs touching `examples/`) is nice-to-have (task group 16), matching the umbrella's cutline.

**Doc claims about CTRE and WPILib behaviour are checked against the current vendor docs when written, with the date noted in the page:**
- SignalLogger auto-start with FMS, and its log paths (USB stick, else `/home/lvuser/logs`);
- `BaseStatusSignal.setUpdateFrequencyForAll` and `optimizeBusUtilization`;
- `DriverStation.startDataLog`;
- the Pro export subset.

No entry name that Flashpoint doesn't read is presented as working (#6: "planned, not read yet"). The firmware field is called out as unread (#9). The AdvantageKit page states what #76 ships, and "not tested on real logs" until #78 says otherwise.

### D-l. Content sources and ordering against the sibling changes
| Content | Needs | Written | Final |
|---|---|---|---|
| Site skeleton, IA, slugs, drift tests, CLI generator | nothing | M1 (by 11-28, so #72 can link slugs) | M1 |
| Versioned deploy (`docs.yml`, tag rules) | Pages enabled (3.3) | M1 (before `0.3.0rc1`, ~12-07) | M3 (0.3.0) |
| README pin step, metadata `Documentation` URL (13.3, 13.6) | `mvp-public-release` 4.4 link check | M1 (by 12-05, before `0.3.0rc1`) | M3 |
| Install pages | #70 install command (`mvp-install-and-first-run` 1.2–1.3) | M1 | M3 (Windows check, `mvp-public-release` 8.2) |
| Quickstart, sample bundle | samples-v1 upload | M1 | M3 |
| Troubleshooting, known limits | #72, #73, #8 messages; 2027 and hoot-only lines (`mvp-install-and-first-run` 6.4, 7.4); event-scale figure (`mvp-public-release` 8.5) | M1 skeleton | M3 |
| Tutorials, how-tos, view and config reference | — | M1–M2 | M3 screenshots |
| Robot side + samples | vendor-doc checks | M2 | M3 compile check |
| Support matrix, AdvantageKit note | #75, #76, #78 results (logs by 11-28, beta 12-14; cell values from `mvp-outside-team-logs` 7.3); Windows row (`mvp-public-release` 8.2, 12-17) | M2 (synthetic) | M3 |
| Maturity labels | robot-pull label kept or removed after the 12-07 robot session (`mvp-install-and-first-run` 8.5); Anomalies beta (#77); Live CAN experimental | M2 | M3 |
| Media pipeline | — | M2 | M3 capture from the RC UI |
| Hosted demo, versioned deploy, offline zip | 0.3.0 tag | M2 (`dev`) | M3 (0.3.0) |
| README, Why page, repo metadata | media | M2 draft | M3; polish M4 |

### Budgets
These touch full logs only through the capture and demo jobs:
- **Unit drift tests:** at most 5 s added to the unit run; no network; under 200 MB.
- **Strict site build:** at most 60 s and 1 GB RSS (placeholders, no corpus).
- **Sample lake build** for capture and demo, five corpus groups: ingest at most 4 min total in CI (Q7 alone is 30–40 s on CI today), with each `ingest` and `derive` process within the existing 1 GB per-match budget (telemetry-lake spec). Capture is at most 5 min, Chromium included. The whole `docs.yml` run is at most 20 min.
- **Media per version:** at most 12 MB in total (about 12 screenshots at 500 KB or less, 4 animations at 3 MB or less as GIF, plus WebMs at 2 MB or less each). The demo is at most 15 MB, and the offline zip at most 40 MB. That's about 70 MB per version on `gh-pages`, so the 1 GB site limit holds for more than 10 versions before old patch-less versions need pruning (D-c).
- **Wheel:** unchanged; no media, and still under 5 MB.

## Risks / Trade-offs

- [Zensical is beta, and its mike fork is a stopgap] → Same `mkdocs.yml` subset; a pinned fallback (D-a) decided in a 2 h spike in M1; no plugins to port.
- [Material's fixes end 2027-05-05 if we fall back] → Static output, CI-only builder, pinned versions; a post-MVP issue moves to Zensical before that date.
- [The mike fork is a git dependency] → Pinned to a commit; `docs-site` and `docs.yml` need git (already present on runners). If the fork breaks, the PyPI `mike==2.*` with the fallback stack is the same commands.
- [History and Anomalies look thin with five corpus matches] → Open question 2. The default is to capture what corpus-v1 gives, and the Why page shows it honestly.
- [A UI change after M3 makes screenshots stale] → Every deploy regenerates them. The `docs-media` PR artifact shows the change to reviewers.
- [Outside logs late (after 11-28)] → Matrix rows ship as "not tested on real logs" (#78 fallback), and the Why page makes no claim beyond the matrix.
- [Vendor behaviour claims wrong or changing at the 2027 kickoff] → Dated "checked against" notes and tested-with lines. The 0.3.1 kickoff patch re-checks the robot-side pages (`mvp-public-release`).
- [Old `blob/main/docs/guide/*.md` links break when pages move] → Stubs (D-d).
- [Pages not enabled in time] → An admin task in M1. Until then, the `docs-site` job still builds strictly and uploads the site as an artifact.
- [`gh-pages` history bloat] → Nightly `dev` deploys only, plus the squash workflow (D-c).
- [Docs scope is large for 7 weeks] → The fallback order: ship the structure, Quickstart, Install, troubleshooting, support matrix and robot-side first (M1–M2). Tutorials and how-tos may ship screenshot-light. Animations beyond the hero GIF are the first to cut.

## Migration Plan

1. M1: land the site skeleton with all pages moved and stubs in place. The guides' content is unchanged at first. Then rewrite section by section.
2. Enable Pages (admin), and deploy `dev` with `latest` aliased to `dev`.
3. M3: the 0.3.0 tag deploys `0.3`, moves `latest`, and the release workflow pins the README images. Tick the README and metadata tasks.
4. Rollback: `mike delete <version>`, or point `latest` back. Deleting `gh-pages` and disabling Pages returns to repo-only docs, since the Markdown still renders on GitHub.

## Open Questions

1. **Pre-release docs versions (Josh, umbrella).** D-c deploys `/0.3/` for `v0.3.0rc1` so that rc's PyPI images resolve under the umbrella's release-build gate, and #72's in-app links follow the same rule (aligned 2026-11-14). The alternative is to let the gate accept `/dev/` images and send rc links to `/dev/`. Resolved 2026-11-15: Josh confirmed deploying `X.Y` for pre-releases, as written.
2. **Sample data for media (Josh).** Should a few more 2026 GACMP/GASUW matches be published as `samples-v1` assets, so History has more than five points and Anomalies shows real findings? Default: no; capture from corpus-v1 only. Task 8.1 is written for either case.
3. **Pages squash threshold.** 300 MB is a guess; it can be tuned after the first releases.
