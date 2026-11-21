## 1. Toolchain decision (M1; decide by 2026-11-21)

- [x] 1.1 Cut `feature/82-mvp-documentation` from `development`. Spike the docs toolchain (design D-a), time-boxed to 2 h:
  - Add an optional Poetry group `docs` with Zensical pinned exactly, and its mike fork (a git dependency, not on PyPI) pinned to a commit SHA. Write a minimal `mkdocs.yml` that uses only the D-a feature subset over the current `docs/guide/`.
  - Add one scratch page with a broken link and one with a broken anchor. Check that `zensical build --strict` fails on both and names them, that OS content tabs render, and that search works when served under `/flashpoint/dev/`.
  - Check that the mike fork deploys `0.2`, `0.3`, `latest` and `dev` to a local bare `gh-pages` repo, and that `INHERIT` overlays work for the `dev` banner.
  - Re-check the facts against the live pages: Material's fixes end 2027-05-05 and its latest patch (9.8.1 on 2026-11-14), the Zensical version (0.1.1 on 2026-11-14), and the mike fork's install source.
  - Record each result, with versions and dates, here. Pick Zensical or the pinned MkDocs 1.6.1 + Material 9.8.x fallback by the D-a trigger.
  - Verify: the results are recorded, and the scratch pages are removed.

  **Results (2026-11-20, macOS, Python 3.11, Chromium via Playwright). Decision: Zensical; no fallback trigger fired.**
  - Facts: Zensical **0.1.3** (PyPI 2026-11-20; 0.1.2 was 2026-11-17; still "Development Status :: 4 - Beta"). Material **9.8.2** (2026-11-19, a security fix in the privacy plugin, which we don't use); its README states end of life **2027-05-05**. The mike fork is `github.com/squidfunk/mike` (a fork, default branch `master`), head `2d4ad799442f4592db8ad53b179bfb33db8c69ac` (2026-04-17), which reports itself as `2.2.0+zensical-0.1.0`. PyPI `mike` is 2.2.0 (upstream, MkDocs only).
  - `docs` group: `zensical = "0.1.3"` and `mike` as a git dependency at that SHA, `optional = true`; `poetry install --with docs` installs both. The minimal `mkdocs.yml` (repo root, `docs_dir: docs/guide`) uses only the D-a subset and builds with no warnings about unknown keys.
  - Strict build: a scratch page with a link to a missing page and one with a missing anchor (next to a valid anchor) fail `zensical build --strict` with exit 1, "page does not exist" / "anchor does not exist" and `file:line:col` for each; the valid anchor isn't flagged. The output ends with a Python traceback (`RuntimeError: Aborted because --strict flag is set`), which is noisy but harmless in CI.
  - Today's guide fails strict with 24 issues, all `../` links to files outside `docs_dir` (`docs/rewrite/`, `docs/adr/`, `notebooks/`, `deploy/`, `config/units/`). Group 2 turns these into `blob/main` URLs (D-b) before the build is a CI gate.
  - OS tabs (`pymdownx.tabbed`, alternate style): three labelled tabs render; clicking each shows only its own block.
  - Search under `/flashpoint/dev/` (served with `http.server` from a parent dir): the index (`search.json`) loads, a query finds the page, and the result opens `/flashpoint/dev/#real-heading`.
  - mike fork against a local bare repo: `deploy --push 0.2`, `0.3`, `--update-aliases 0.3 latest`, `set-default --push latest`, `deploy -F mkdocs.dev.yml dev` all succeed. `gh-pages` holds `0.2/ 0.3/ dev/ latest`(a git symlink, mike's default alias type) `versions.json` and a root redirect. In the browser the root lands on `/flashpoint/latest/`, every version shows the selector listing `dev, 0.3, 0.2`, `latest` shows `0.3`, and canonical URLs are `/flashpoint/<version>/` (mike appends the version, so `site_url` must stay `/flashpoint/`).
  - mike's deploy runs `zensical build --clean` without `--strict`, so `docs.yml` runs the strict build as its own first step (D-c's order already does).
  - `INHERIT` overlay: `mkdocs.dev.yml` with `INHERIT: mkdocs.yml` and `extra.dev_banner` merges `extra` (the mike provider survives) and, with a `theme.custom_dir` override of the `announce` block, shows the banner on `dev` only. A template override is not a plugin; it's added to the feature subset in the ADR.
  - Scratch pages, the scratch site and the overlay test lived outside the repo and are removed; `docs/guide/` is unchanged.
  - Missed by the spike, found in 2.4: the strict build does **not** fail on a missing image or video (it builds clean). Not a D-a fallback trigger, but the spec requires it, so `tools/docs-media.py check-site` checks the built site after the strict build (ADR-0019 updated).
- [x] 1.2 Write `docs/adr/00NN-docs-site-toolchain.md` (the next free number) with the decision, the facts and their dates, the feature-subset rule, the fallback trigger and the rejected options. Add it to `docs/adr/README.md`. Verify: `pytest tests/test_docs.py` passes.
- [x] 1.3 Update `docs/rewrite/`:
  - add the docs site and toolchain to `04-ecosystem-and-tools.md`;
  - add the docs tooling (`tools/docs-media.py`, `tools/gen-cli-reference.py`, `tools/gen-support-matrix.py`) to `05-target-architecture.md` §5;
  - the MVP section of `06-roadmap.md` already has a docs line (`mvp-public-release`); change it only if the toolchain decision changes what it says.

  Verify by reading the diff, and that the link check passes.

## 2. Site skeleton and page moves (M1; merged by 2026-11-28, before #72's docs links)

- [x] 2.1 Test first: make `doc_files()` recursive over `docs/guide/**/*.md`, exempt "moved" stubs (a fixed first line), and apply the `blob/main` path-exists check to every page. Add fixtures under `tests/fixtures/docs/`: a nested page with a broken sibling link, a stub, and a `blob/main` link to a missing path. Verify: the fixture tests pass, and the run against the real tree passes.
  Done 2026-11-20. The stub first line is `# This page moved`. The check also reports a relative link on a site page that leaves `docs/guide/` (D-b: such links break on the site; Zensical's strict build misses non-`.md` targets). The 45 such links in today's guide became `blob/main` (`tree/main` for folders) URLs here, so the real-tree run passes.
- [x] 2.2 Test first: the navigation check. Parse the `mkdocs.yml` `nav`. Every non-stub page under `docs/guide/` must be listed, and every entry must exist. Use a fixture `mkdocs.yml` with one unlisted page and one missing entry. Verify: the fixture test reports both by name. The real-tree test fails until 2.3 (expected).
  Done 2026-11-20. The check also reports a stub listed in the nav and a stub missing from `exclude_docs` (stubs must not build: `robot-side.md` would collide with `robot-side/index.md`).
- [x] 2.3 Create the D-d tree:
  - move `team-setup.md` to `tutorials/`, `auto-import.md` and `live-can-health.md` to `how-to/`, `cli-reference.md` to `reference/cli.md`, and `how-it-works.md` and `developer-guide.md` to `under-the-hood/`;
  - leave a "moved" stub at every old path;
  - create empty titled pages for every other D-d page, each with a one-line purpose;
  - write `mkdocs.yml` with the spec's navigation order;
  - update `CLI_REFERENCE` and `LIVE_GUIDE` in `tests/test_docs.py`, and every relative link;
  - if #72's `flashpoint/docs_links.py` has already merged, update its page-path map in the same commit (design D-b); otherwise post the final paths on #72 (5.2).

  Rebase over any guide edits the sibling changes have merged (#70's install text, the Live CAN status paragraph from `mvp-public-release` 5.2). After this lands, nobody else moves guide files. Verify: `pytest tests/test_docs.py` passes, including 2.2, and the strict build passes locally.

  Done 2026-11-20 (branched from `development` after #70 and 5.2 merged). Decisions:
  - `robot-side.md` moved to `robot-side/index.md` (not in the move list, but D-d's `robot-side/index.md` would build to the same `/robot-side/` URL); its stub stays.
  - `user-guide.md` stays where it is and is listed under Quickstart until groups 6, 10 and 11 split it; `docs_links.py` still links two of its anchors.
  - The old `README.md` became `index.md` (the home until 13.5); `README.md` is now the stub index. The nav starts with `Home: index.md`, then the spec's 11 sections in order.
  - `docs_links.py` had merged (#103), so its `PAGES` paths changed in this commit; 5.2's comment on #72 isn't needed.
  - Tests changed for the move: the guide path constants in `tests/test_docs.py` and `AUTO_IMPORT` in `tests/acquire/test_maturity_label.py`.
  - Repo links: `README.md` and `CONTRIBUTING.md` `blob/main` links and `deploy/README.md` relative links point at the new paths.
- [x] 2.4 Write a minimal `docs/media-manifest.toml`: one screenshot entry for each view, with alt text. Add `tools/docs-media.py placeholders`, which writes a tiny valid PNG, GIF or WebM for each entry into `docs/guide/assets/media/` (gitignored). Write a unit test that the placeholders match the manifest's file names. Verify: the test passes, and the strict build with placeholders passes.

  Done 2026-11-20. Placeholders need only the standard library (a 1x1 PNG and GIF, and a 475-byte one-frame VP8 WebM made once with ffmpeg; a frameless WebM doesn't load in Chromium). All three decode in Chromium. The strict build doesn't catch a missing image, so `docs-media.py check-site <site>` does (fails without placeholders, passes with them), and the unit link check requires `assets/media/` links to name manifest files. The five view reference pages show their screenshot with the manifest's alt text.
- [x] 2.5 Add the `docs-site` CI job:
  - `poetry install --only docs`;
  - placeholders;
  - strict build;
  - upload the built site as an artifact.

  It runs on every PR and push, whatever paths changed, and is added to `ci-ok`'s `needs`. Verify: the job is green on the PR, under 8 min, and a pushed commit with a broken anchor turns it red (then revert).

  Done 2026-11-20 on #106. It also runs `docs-media.py check-site site` after the strict build (2.4). Green in 23 s on 71cc6f0; the broken-anchor commit 574c70e failed it at "Strict build" (run 44859966879), and 3e32ca4 reverts it. Install uses `--only docs --no-root`: the build doesn't need the package.

## 3. Versioned deploy (M1; live before `0.3.0rc1`, ~2026-12-07)

- [x] 3.1 Test first: `tools/docs-version.py` maps a tag and the published versions to (version, move `latest`?, banner). Cases: `v0.3.0` with nothing published gives `0.3`, moves `latest`, no banner; `v0.3.0rc1` gives `0.3`, leaves `latest`, "pre-release" banner; `v0.2.5` with `0.3` published gives `0.2` and leaves `latest`; `v0.3.1` after `v0.3.0` gives `0.3` and moves `latest`; a malformed tag such as `v0.3` fails, naming it. Then write the tool. Verify: the tests pass.
  Done 2026-11-20. A pre-release deploy is titled "X.Y (pre-release)" in mike's `versions.json`, so a later final tag counts only final versions when deciding `latest` (a 0.2 patch still moves `latest` while 0.3 is only an rc). Output is `version=`, `latest=`, `banner=`, `title=` lines for `$GITHUB_OUTPUT`; a bad tag exits 2.
- [x] 3.2 Write `.github/workflows/docs.yml` (design D-c), using 3.1's tool:
  - `dev` nightly when `development` moved, and on `workflow_dispatch`;
  - `X.Y` on `v*` tags, with `latest` moved only for the highest final release, and the pre-release banner on pre-release tags;
  - `mike set-default latest`;
  - `contents: write` for the push job only;
  - strict build, then media, then demo, then offline zip, then push, with any failure stopping before the push.

  Until groups 8 and 9 land, the media and demo steps use placeholders and skip. Add the `mkdocs.dev.yml` overlay with the "unreleased" banner, and the same overlay with "pre-release" text. Verify: `actionlint` passes (or a workflow lint in CI), and a dispatch run on the branch builds without pushing (dry-run input).
  Done 2026-11-20. `actionlint` passes; `tests/test_docs_workflow.py` checks the triggers, permissions, step order and the gh-pages fetch. A manual run needs the workflow on the default branch (`main`), so the dry run was a `pull_request` run instead: PRs touching the deploy machinery get a forced dry run of `dev` (run 44862767880 on #106: plan and build passed, deploy skipped). The plan and deploy scripts were also rehearsed against a scratch origin: first nightly `dev` (holds `latest`), a no-change nightly (skipped), a feature-branch manual run (forced dry run), `v0.3.0rc1` (`0.3 (pre-release)`, `latest` unchanged), `v0.3.0` (takes `latest`).
  Found in the rehearsal: mike works from the local `gh-pages` branch, so the deploy job fetches it first (without it, the first push would replace every published version); `mike set-default latest` fails when no `latest` exists (a first deploy from an rc tag), so the root then opens the deployed version. Concurrency is on the deploy job, not the workflow, so a PR dry run can't displace a queued deploy. `docs-version.py dev` gives `latest` to `dev` until the first final release (D-b). A manual run can also redeploy a released minor (`tag` input) from `main` or the tag, for release-checklist step 8.
- [ ] 3.3 [HUMAN] An org or repo admin enables GitHub Pages for `IgniteRobotics/flashpoint` from the `gh-pages` branch, root. Verify: `gh api repos/IgniteRobotics/flashpoint/pages` returns the site URL.
  Order: Pages can only be enabled once `gh-pages` exists, and the first deploy creates it (the first nightly after `docs.yml` reaches `main`, or the rc1 tag, whichever comes first). So 3.3 follows that first deploy, and 3.4's checks follow 3.3.
- [ ] 3.4 Run the first `dev` deploy, and alias `latest` to `dev` until 0.3.0 (design D-b). Verify: `https://igniterobotics.github.io/flashpoint/` opens `latest`; `/dev/` pages show the unreleased banner and the version selector; a page with a broken link pushed to a test branch fails its dispatch run without changing `gh-pages`.
  Blocked until `docs.yml` is on `main`: GitHub fires schedule and manual runs only from the default branch's workflow file, so the nightly `dev` deploy starts with the `development` → `main` promotion (rc1, ~12-07), Josh chose to keep it that way (2026-11-21): no `push` trigger on `development`. The plan already gives `dev` the `latest` alias while no final version exists.
- [x] 3.5 Add the offline archive step: a `use_directory_urls: false` build zipped as `flashpoint-docs-X.Y.zip`, published at `/X.Y/`. Coordinate with #80 so the release workflow also attaches it to the GitHub Release. Verify: unzip it locally, open `index.html` from disk with Wi-Fi off, and pages, navigation, links and images work.
  Done 2026-11-20. `mkdocs.offline.yml` (`use_directory_urls: false`, no web fonts, no version selector); the build job zips it into `docs/guide/` so mike publishes it at `/X.Y/flashpoint-docs-X.Y.zip`. #80's `release.yml` is unchanged: `docs.yml`'s `attach-archive` job (tags only) waits up to 30 min for the GitHub Release and uploads the zip, because only `docs.yml` will have the real media. Checked by unzipping a built archive elsewhere and opening it in Chromium from `file://` with every non-file request blocked: pages, tab navigation, all 45 links on the team-setup page and images work. Search doesn't work from `file://` (expected; the offline install page must say so, D-c).
- [x] 3.6 Add a manual `docs-squash.yml` workflow that replaces `gh-pages` history with one commit of its current tree. Verify: a run on a scratch fork keeps every version's files, and leaves one commit.
  Written 2026-11-20 (`docs-squash.yml`, confirm input, the deploy concurrency group, `--force-with-lease` on the checked-out head). Rehearsed against the scratch origin: 4 commits became 1 with an identical tree (`0.3`, `dev`, `latest`, `versions.json`, root redirect kept), and a deploy pushed mid-squash made the push fail (stale info) instead of being lost. Not run on a scratch fork: Josh accepted the local rehearsal instead (2026-11-21).

## 4. CLI reference generated from the CLI (M1)

- [x] 4.1 Test first:
  - The generator's output for the real `_parser()` must equal the committed `reference/cli.md` blocks between `<!-- generated:cli:<name> -->` markers.
  - A fixture parser with an option that has no help text must make generation fail, naming the subcommand and option.
  - A fixture where an option is added without regenerating must fail, naming both.

  Verify: the fixture tests pass, and the real-tree test fails until 4.2 (expected).
- [x] 4.2 Write `tools/gen-cli-reference.py`: a table per subcommand with option (positional arguments included), help and default. Add the three missing help strings in `src/flashpoint/cli.py`: `ingest`'s `paths` and `--profile`, and `rebuild`'s `--profile`. Regenerate `reference/cli.md`, keeping the hand-written purpose and examples outside the markers. Verify: `pytest tests/test_docs.py` passes, and `flashpoint <cmd> --help` for each subcommand agrees with its table.
  Done 2026-11-21. Markers are `<!-- generated:cli:<name> -->` … `<!-- /generated:cli:<name> -->`; columns Option, What it does, Default. The option cell is argparse's own invocation and the help is expanded the way `--help` shows it (`%(default)s`), so the tables agree with `--help` by construction (spot-checked on `serve` and `ingest`). `--check` exits 1 naming each stale subcommand and option. Help strings added for `ingest`'s `paths` (now `PATH`) and `--profile` and `rebuild`'s `--profile`, and sharpened where the hand tables said more than `--help` (`--jobs` default, `--team` range, `--team-name` length, `--quarantined` retries, `report --static`/`--no-raw`, `serve --host`/`--port` defaults, the acquire options). Longer notes (`serve --host 0.0.0.0` has no password and makes the app read-only; `init` header fallback; `restore --force`) moved to prose outside the markers. Fixed the stale Python range in the `doctor` table (3.11 to 3.14).
- [x] 4.3 Test first: the commands-parse check (design D-e) on fixtures:
  - a good command;
  - `flashpoint ingest --recursive logs/` (rejected);
  - a `FLASHPOINT_CONFIG=… flashpoint …` prefix;
  - `poetry run` and `uv run` prefixes;
  - a `\` continuation;
  - a PowerShell block;
  - a `<placeholder>`.

  Then run it on the real docs, and fix every page it flags. Verify: `pytest tests/test_docs.py` passes.
  Done 2026-11-21. Fixture `tests/fixtures/docs/commands.md` covers each case, plus `uv tool install … flashpoint` (not a flashpoint call), a comment line, a trailing `# comment`, a placeholder with spaces, console output lines, and a non-shell fence. Calls are split on `&&`, `;` and `|`; argparse's message is reported with the page, line and command. The real docs (README, deploy README, every guide page) had nothing to fix. The invalid-choice wording differs across Python versions, so that fixture line is checked by prefix.

## 5. Docs links from the app (M1, with #72)

- [x] 5.1 Test first (design D-e "App docs links"): until #72 lands, the repo-relative docs paths in `setup/checks.py` and `live-can.js` must name files that exist under `docs/guide/` (stubs count). Once `flashpoint/docs_links.py` exists, every path in its page map must be a page listed in the site navigation (#72's own test checks pages and anchors against the source tree). Use a fixture page map with a stub path. Verify: the fixture fails, naming the page id and path, and the real tree passes.
  Done 2026-11-21. #72 had landed, so only the second case applies: `page_map_problems` checks each `docs_links.PAGES` path is a nav-listed, existing, non-stub page (`<path>.md` or `<path>/index.md`). The fixture names a stub path and a nav entry with no page; the real map passes, and pointing an id at an old path (`auto-import`) is caught as a moved stub.
- [x] 5.2 Post the D-d page paths and the version rule (design D-b) on #72, so `mvp-install-and-first-run`'s `docs_links.py` uses them. Verify: the comment is linked here, and after #72 merges, 5.1's second case runs against the real map.
  Moot 2026-11-21: #72's `docs_links.py` merged (#103) before the tree moved, so 2.3 updated its `PAGES` paths in the move commit, and 5.1's check runs against the real map. No comment on #72 was needed.

## 6. Install, Quickstart and sample logs (M1)

- [x] 6.1 Test first: `tools/build-sample-bundle.py` on fixture files writes `flashpoint-sample-1.zip`. It holds `logs/` (a wpilog and hoots, never a `truncated` file), `config/robots/2026-comp.toml`, `README.txt` and `SHA256SUMS`, and the sums match. Then write the tool. Verify: the unit test passes, and a run on the fetched corpus E10 gives a zip of 30 MB or less.
  Done 2026-11-21. Built from the fetched corpus-v1 E10 (each file checked against the corpus manifest): **15.1 MB**. Entries carry a fixed timestamp, so two builds are byte-identical; `shasum -a 256 -c SHA256SUMS` passes inside the unpacked bundle. Its `config/` holds `robots/2026-comp.toml`, plus `acquire.toml` and Ignite's `seasons/2026.toml` (added in 6.5, when running the bundle showed the robot file needs them). Current sha256: `92558ca93444048069b9b7594177bdcc24d412c084873078874cf0d42a95e4bd` (the one `quickstart.md` quotes; the corpus test fails if they drift).
- [ ] 6.2 [HUMAN] A maintainer publishes the bundle as the only asset of a `samples-v1` GitHub release, marked not-latest. Verify: `gh release view samples-v1` lists it, and `gh release list` still doesn't show it as Latest.
- [x] 6.3 Write `install/index.md`, `install/macos.md`, `install/linux.md` and `install/windows.md`, using the install command #70 ships (`mvp-install-and-first-run` 1.2–1.3; if it hasn't merged, write to its team-setup "Installable public package" requirement and re-check the commands once it lands):
  - uv first;
  - pipx as the alternative;
  - the `py` launcher form on Windows;
  - the platform floor;
  - updating;
  - offline prep (the owlet download, linking `how-to/install-offline.md`).

  Use OS content tabs only where commands differ. Verify: the commands-parse and link checks pass.
  Done 2026-11-21, from the install #70 shipped (ADR-0018; the user guide's Install section). One page per OS rather than tabs, since every step's command differs by OS (Windows blocks are `powershell`). Each page: the floor, uv once, `uv tool install --python 3.13 flashpoint`, `init`, a "Check it worked" with `--version` and `doctor`, pipx (the `py` launcher form on Windows), owlet prep before the first event linking `how-to/install-offline.md`, and updating. Commands-parse, link and nav checks and the strict build pass.
- [x] 6.4 Test first (corpus marker), against a fixture `quickstart.md` until 6.5 writes the real one: a test builds the bundle from the corpus, runs the Quickstart's `sh` "Try it" commands taken from the page (with the download replaced by the local zip) in a temp home, and checks that Replay's API lists the E10 match with motor tracks, that ingest printed the `incomplete-read` warning for the two 19-29-15 hoots in the words the page's note quotes, and that the temp home's default config folder and lake don't exist afterwards. A second case checks a cut-short copy of the zip against the page's SHA-256 command and expects a mismatch. Verify: `pytest -m corpus tests/test_quickstart_sample.py` passes on the fixture page.
- [x] 6.5 Write `quickstart.md`: own logs with the install plus at most three commands; "Try it with sample logs" with `sh` and PowerShell tabs, the size, the SHA-256 check and the note that the incomplete warning is expected; a link to the demo; and a closing link to the Robot side section (#83). Point 6.4's test at the real page. Verify: `pytest -m corpus tests/test_quickstart_sample.py` passes, and the commands-parse and link checks pass.
  Done 2026-11-21 (6.4's test written first, against the page path; it failed on the placeholder page, then passed on the real one). The test builds the bundle from the corpus, runs the page's `sh` block in a temp home (download → local copy, `serve` dropped), and checks E10 is `ok` in Replay with motor series, the two `19-29-15` `incomplete-read` warnings the note quotes, the page's SHA-256 line passing (so the published hash matches the built bundle), and no `~/.config/flashpoint` or `~/flashpoint-lake` afterwards; a cut-short zip fails the page's checksum line. The PowerShell tab is untested here (no pwsh; 6.6). Running it found:
  - the bundle needed `config/acquire.toml` and Ignite's `config/seasons/2026.toml` (6.1 and D-i updated; new sha256 `92558ca93444048069b9b7594177bdcc24d412c084873078874cf0d42a95e4bd`, still 15.1 MB);
  - **a relative `--lake` crashes `detect`** (`restarts.py`: `lake.ledger.as_uri()` on a relative path) and `ingest` still exits 0 with no Replay data. A `src/` bug, filed as #107; the page uses `--lake "$PWD/lake"`, which is right either way.
  The env var is inline on each `sh` command so it doesn't linger; the PowerShell tab says to close the window. Added the Troubleshooting entry the "Damaged download" scenario needs. The page's download URL works once 6.2 publishes `samples-v1`.
- [ ] 6.6 [HUMAN] Someone runs the PowerShell "Try it" tab on a Windows 11 machine, and the `sh` tab on Ubuntu 24.04. Note where the steps differ from the page, and fix the page. Verify: both reach Replay, and the results are recorded here.

## 7. Robot side: "What to log" and samples (M2)

- [x] 7.1 Test first, with fixtures:
  - every fenced `java` block under `docs/guide/robot-side/` must be a contiguous run of lines in some `examples/robot-java/**/*.java` (trailing whitespace ignored);
  - every `.java` file must carry `Tested with WPILib <v>, Phoenix 6 <v>`.

  Verify: the fixture tests pass. The real-tree test fails until 7.3 (expected, because the blocks are still inline).
- [x] 7.2 Turn `examples/robot-java/` into a minimal GradleRIO project:
  - `build.gradle` and `settings.gradle`;
  - the wrapper;
  - `vendordeps/Phoenix6-*.json`;
  - `Main.java` and `Robot.java`.

  Move `CanHealthPublisher.java` to `src/main/java/frc/robot/flashpoint/`, and update `how-to/live-can-health.md`, the guide index and any test that names the old path. Verify: `pytest tests/test_docs.py` passes (links).
- [x] 7.3 Move `CanInventoryLogger`, `CanBusStatusLogger` and the build-info `MetaData` code out of the old robot-side page into files. Wire all of them in `Robot.robotInit()` as the one-file usage example. Note in the inventory page that the `firmware` field isn't read yet (#9). Verify: the 7.1 tests pass.
- [x] 7.4 On a machine with JDK 17 and network access, run `./gradlew compileJava` in `examples/robot-java/` against the versions on the tested-with lines. Verify: it compiles with no errors, and the versions and date are recorded here.
  Done 2026-11-21 on macOS (Temurin JDK 17.0.18): `./gradlew compileJava` in `examples/robot-java/` builds with no errors or deprecation warnings (`-Xlint:deprecation` is on) against GradleRIO **2026.2.1** and Phoenix 6 **26.3.0**, the latest 2026 releases that day (Gradle plugin portal; CTRE's `Phoenix6-frc2026-latest.json`), and the versions Robot-2026 uses. gversion 1.10.3 writes `BuildConstants` under `build/`, so it's never committed.
- [x] 7.5 Write `SignalLoggerSetup.java` and `SignalRates.java`:
  - hoot logging on, with where hoots land;
  - `DataLogManager.start()` and `DriverStation.startDataLog(...)`;
  - `BaseStatusSignal.setUpdateFrequencyForAll(50, …)` for current, voltage, velocity and temperature, called before `optimizeBusUtilization()`.

  Check each behaviour against the current CTRE and WPILib docs, and put the date checked in a comment. Wire them into `Robot.java`. Verify: 7.4's compile passes again, and the 7.1 tests pass.
  Checked 2026-11-21 against CTRE's signal logging and status signals pages and WPILib's DataLog page (dates in the file comments). Two findings that differ from the design's wording: Phoenix's auto-start keys on the Driver Station connecting or the robot enabling, not on FMS; and CTRE's page names no default hoot folder, so `SignalLoggerSetup` sets it to `DataLogManager.getLogDir()` (the USB stick, else `/home/lvuser/logs`, both of acquire's default `roots`).
- [x] 7.6 Write `robot-side/index.md`: the ranked "What to log" table, in the spec's row order. Each row has what to add, what Flashpoint gains, what you lose without it, and the sample file. State "Java only". Verify: the link, Java-block and commands-parse checks pass.
- [x] 7.7 Write `robot-side/signal-logging.md`, `datalog.md` and `signal-rates.md`. The signal-rates page covers the optimisation caveat, the rio-bus budget, and Replay's "slower than 20 Hz" note (#7). Verify: the link and Java-block checks pass.
- [x] 7.8 Write `robot-side/pro-and-temperature.md` (using #75's wording), `battery-and-pdh.md` (the proxy today, with suggested entry names marked "planned, not read yet", for #6), `disk-space.md` (rio and USB stick space, and checking for a hoot after each match), and `build-info.md`, `can-inventory.md` and `bus-status.md` from the old page. Verify: the link and Java-block checks pass.
  Done 2026-11-21, ahead of #75: the Pro page states today's behaviour (Replay's "Temperature not logged. The hoots are not Pro-licensed…" note; History reads NO DATA) and names #75's "not logged (needs Phoenix Pro)" as the coming change. Re-check it at 12.8. The firmware note links #87, the issue #9 was split into.
- [x] 7.9 Test first: a synthetic wpilog (`tests/wpilog_builder.py`) whose `/Flashpoint/CANInventory` holds `{"schema": 1, "error": "…"}`. Ingest and derive it, and check that the error is recorded and that history uses swap dates, as `can-inventory.md` says. Verify: the test passes. If behaviour differs from the page, fix the page.
  `tests/semantics/test_inventory_error_flow.py`: ingest and derive through the CLI with a fake-converted rio hoot in the same session. The error lands in the ledger's `inventory.error`, and the slot gets `legacy:bot:hood:1` (one swap date before the log). The page needed no change. A valid inventory in the same test fails it (checked by mutation).
- [ ] 7.10 After #76 merges, write `robot-side/advantagekit.md`: what AdvantageKit logs get (identity per #76), what they don't (full alignment and framing), and "not tested on real logs" until 12.8 says otherwise. Verify: the link check passes, and the page matches the support matrix row.
- [ ] 7.11 [HUMAN] By 2026-12-12 (the `mvp-public-release` M2 gate), a mentor other than the author reviews the Robot side section and its samples against a real robot project. Verify: the review is recorded on #83, and each finding is fixed or filed.

## 8. Generated screenshots and animations (M2; final capture in M3)

- [ ] 8.1 [HUMAN] Josh decides design open question 2: publish more 2026 matches as `samples-v1` assets, or capture from corpus-v1 only. If more, add them to the bundle tool's sample-lake list and to `samples-v1` (6.2). Verify: the decision is recorded here.
- [ ] 8.2 Test first:
  - every view in `web/static/views/*.js` must have a screenshot entry;
  - every entry must have non-empty `alt`, a known `kind` and a `steps` name that the script defines;
  - every image a page references must be a manifest file.

  Then complete `docs/media-manifest.toml`:
  - a screenshot for each view;
  - screenshots for the tutorial steps;
  - a quarantine screenshot;
  - animations: ingest to Replay, History motor pick, anomaly triage, Live CAN dropouts.

  Verify: the tests pass.
- [ ] 8.3 Test first: `docs-media.py capture` with an empty corpus dir exits 2, names the first missing file and `python tools/fetch-corpus.py`, and writes nothing. Then write the corpus check, and the sample-lake build: a temp `FLASHPOINT_CONFIG` copied from `config/` plus a theme, then `ingest` of the sample groups (never the empty hoot or the truncated E10 copy; a unit test asserts the file list), `derive`, `detect` and `report`. Verify: the unit test passes, and a local run builds the lake within the design budget (4 min or less).
- [ ] 8.4 Test first (browser marker): capture one screenshot entry against a small synthetic lake (`tests/web` builders), with the design D-f fixed context. Check that:
  - the file is written within budget;
  - a console error in the page fails the shot, naming it;
  - a non-loopback request is blocked.

  Then write the screenshot capture, with masks, PNG optimisation and the quantise-if-over-budget rule. Verify: `pytest -m browser tests/tools/test_docs_media.py` passes.
- [ ] 8.5 Add the animations: Playwright video to WebM, then the bundled ffmpeg converts it to a GIF (12 fps, 960 px, two-pass palette), with the 20 s and 3 MB checks. Add `reel`, which joins the flows into one video of 60 s or less for #81. Write a browser-marked test that one short flow on the synthetic lake gives a WebM and a GIF within budget. Verify: the test passes.
- [ ] 8.6 Add the Live CAN entries: start `flashpoint.live.fake_robot --scenario shake` on loopback, and serve with `--robot-host 127.0.0.1`. Verify: a local capture shows dropouts in the Live CAN screenshot and animation.
- [ ] 8.7 Wire the capture into `docs.yml`, before the build, and add the non-blocking `docs-media` PR job that uploads `build/docs-media/` as an artifact. It runs on PRs touching `src/flashpoint/web/`, the manifest, or `tools/docs-media.py`. Verify: a dispatch `dev` deploy shows real images, every image is within budget, and the whole run takes 20 min or less.

## 9. Hosted Replay demo (M2)

- [ ] 9.1 Test first (browser marker): export a synthetic lake with `report --static --no-raw` and the demo theme ("Sample data · 6829 robot"), open it from disk with non-loopback blocked, and check that the matches are listed, the header shows the sample-data name, and the console has no errors. Verify: the test passes.
- [ ] 9.2 Add the demo step to `docs.yml`: export the sample matches to `<site>/demo/`, fail on a non-zero exit or a missing match, check that the size is 15 MB or less, and run the same browser check on the built demo. Write `demo.md`. Verify: the `dev` deploy serves `/dev/demo/index.html`, and a run with one match key broken on purpose fails without pushing.

## 10. Reference: config files and views (M1–M2)

- [x] 10.1 Test first: each `reference/config/<file>.md` must name, as `` `key` ``, every field of its loading model (design D-e list). Use a fixture model plus page with one missing key. Verify: the fixture fails, naming the file and key. The real-tree test fails until 10.4 (expected).
- [x] 10.2 Write `reference/config/acquire.md` (every key and default, moved from the auto-import guide) and `theme.md`, each with "Common errors" quoting real loader messages. Verify: the 10.1 test passes for these files.
- [x] 10.3 Write `reference/config/robots.md`, `seasons.md` and `units.md` (every column of the units CSV header). The robots page covers slots, buses, signals, siblings, `battery_proxy` and swap dates, moved from the user guide. Test first: a robot file with an unknown key produces exactly the message quoted under "Common errors". Verify: both tests pass.
- [x] 10.4 Write `reference/config/report.md`, `anomaly.md`, `anomaly-causes.md` and `failure-labels.md`, linking the bundled defaults instead of restating thresholds. Verify: the 10.1 test passes on the whole tree.
  Done 2026-11-21. `tests/test_docs_config_reference.py` maps each page to its loader models by TOML name (`[[slot]]`, not `slots`); a table counts when written `[name]` or `[[name]]`. Every "Common errors" line is `flashpoint doctor`'s own one-line form (`describe_error`), captured by feeding each loader a broken file; the robots test runs the real checks and requires the page to quote the result exactly. The `acquire.toml` table left the auto-import how-to, and the robot, theme and thresholds sections left the user guide, each replaced by a link. Found while writing: a season file in the config folder replaces the bundled one whole, so it must repeat every root (now said on `seasons.md`).
- [ ] 10.5 Test first: each view in `web/static/views/*.js` must have `reference/views/<view>.md`. Then write `replay.md` and `history.md`, explaining every control, marker and badge, with the manifest screenshot. Verify: the test passes for these two views, and the strict build passes.
- [ ] 10.6 Write `reference/views/anomalies.md` (beta label, #77), `setup.md` and `live-can.md` (experimental label). Verify: the 10.5 test passes for every view.

## 11. Tutorials and how-to guides (M2)

- [ ] 11.1 Test first: every `how-to/*.md` must have "Before you start", a numbered list, "Check it worked" and a link into `troubleshooting.md`. The how-to set must include the spec's list. Verify: a fixture how-to missing "Check it worked" fails by name. The real-tree test fails until 11.7 (expected).
- [ ] 11.2 Write `tutorials/first-event.md` with the sample bundle: USB folder, ingest, hottest motor in Replay, History, triage one anomaly, static export. Each step uses its manifest screenshot. Verify: following it on a clean macOS install succeeds as written (record it here), and the link and commands-parse checks pass.
- [ ] 11.3 Rework `tutorials/team-setup.md` for the new IA. Link the how-tos and references instead of repeating them, and use the #70 install and the #72 init and Draft robot order. Verify: the link and commands-parse checks pass.
- [ ] 11.4 Write `how-to/set-up-a-robot-file.md` and `how-to/import-from-usb.md`, and rework `how-to/auto-import.md` to the how-to shape: the robot-pull "beta" label (#74), backup linked to 11.5, and the install command from the Install page. Verify: the 11.1 test passes for these three.
- [ ] 11.5 Write `how-to/back-up-the-lake.md` (with the restore drill), `move-to-another-laptop.md` and `start-a-new-season.md` (with the 2027 steps, from #73). Verify: the 11.1 test passes for these three.
- [ ] 11.6 Write `how-to/replay-on-a-usb-stick.md`, `share-on-the-pit-network.md` and `open-in-advantagescope.md`, and rework `live-can-health.md` (experimental label, examples path from 7.2). Verify: the 11.1 test and the live CAN contract test pass.
- [ ] 11.7 Write `how-to/triage-an-anomaly.md` (beta), `team-name-and-colours.md`, `share-logs-privately.md` and `update-flashpoint.md`. Verify: the 11.1 test passes for these four.
- [ ] 11.8 Write `how-to/install-offline.md`. For each OS, collect into one folder Flashpoint and its dependency wheels for the target Python and platform (`uv` or `pip download --only-binary=:all:`), the owlet versions (`doctor` names them), and the offline docs zip. Then install from that folder. Verify: the link and commands-parse checks pass, and 11.9 runs it.
- [ ] 11.9 [HUMAN] Someone follows `install-offline.md` on a macOS or Linux laptop with networking off. Verify: the install succeeds, `doctor` shows owlet cached, a corpus hoot ingests, and the result is recorded here.

## 12. Support matrix, limits, troubleshooting, FAQ, privacy (M2)

- [x] 12.1 Test first:
  - the `tools/gen-support-matrix.py` output for `docs/guide/support-matrix.toml` must equal the committed tables between the markers;
  - every cell must be one of the spec's six values ("supported", "supported (needs Phoenix Pro)", "rough", "not tested on real logs", "not tested", "not supported");
  - every "supported" cell's evidence must name an existing corpus group, an existing path under `tests/`, or `outside:<yyyy-mm>` with an issue link;
  - every "rough" cell must name its workaround and an issue, and every "not supported" row must link an issue.

  Use fixture TOMLs, including a cell reading "works". Verify: the fixtures fail as expected, with the table, row and column named.
- [x] 12.2 Write `support-matrix.toml`, the generator and `support-matrix.md`, with the device, log-setup and platform tables. Fill cells from the corpus and the tests; outside-team rows read "not tested" or "not tested on real logs". File or link the tracking issues for REV, Phoenix 5 and Systemcore. Verify: the 12.1 tests pass.
  Done 2026-11-21. `tools/gen-support-matrix.py` checks and renders; `tests/test_docs_support_matrix.py` runs the fixtures, the real data, the page's freshness and the spec's row order. A "not supported" cell may carry its issue itself (per-feature gaps such as Pigeon 2 temperature, #75) or on its row (hardware). Filed for the table: #108 (REV Spark), #109 (Phoenix 5, also linked from wpilog-only motor data), #110 (Systemcore), #111 (macOS service); PDH/PDP link #6, hoots-only links #8. Platform install/ingest/serve cite corpus E10 via CI's first-run smoke (real owlet) on all three OSes, so Windows reads "supported" there until 12.8 takes `mvp-public-release` 8.2's walkthrough result. Outside-log rows (non-Pro, AdvantageKit, rio bus only) read "not tested" until 12.8.
- [x] 12.3 Write the Known limits section:
  - 2027 and Systemcore status (from #73);
  - the first hoot needs internet;
  - the hoot-only session warning (#8);
  - each labelled feature;
  - the 2027 lines and the hoot-only line handed over by `mvp-install-and-first-run` 6.4 and 7.4;
  - the first-run figure at event scale, if `mvp-public-release` 8.5 hands one over (over 15 min or 1 GB);
  - the Linux owlet crash that retries absorb (#4, handed over by `mvp-public-release` 1.4).

  Verify: the link check passes.
  Done 2026-11-21. No event-scale first-run figure yet (`mvp-public-release` 8.5 is open); add the line if it hands one over. The Linux owlet line is the text requested on #84; #107 is listed as a known issue until it's fixed.
- [ ] 12.4 Test first: the troubleshooting message check (design D-e) on a fixture entry whose message no longer exists in a fixture source tree. Then write the install and first-run half of `troubleshooting.md`:
  - command not found;
  - Python refused;
  - proxy blocks owlet;
  - Windows paths and quoting;
  - a damaged sample download (SHA-256 mismatch, from 6.4);
  - each `doctor` `PROBLEM`.

  Take each message from the code or from real output. Verify: the test passes on the real page.
- [ ] 12.5 Test first (corpus marker): ingest the corpus's empty hoot and check that it is quarantined with the message the quarantined-file entry quotes; ingest the truncated E10 copy and check that it succeeds with the warning the incomplete-file entry quotes. Then write the logs-and-views half of `troubleshooting.md`:
  - all non-match;
  - each empty Replay state (from #72);
  - no temperature (from #75);
  - robot "unknown";
  - quarantined file;
  - incomplete file.

  Verify: the corpus test and the message check pass.
- [ ] 12.6 Write `faq.md`: REV, AdvantageKit, non-Pro hoots, Systemcore/2027, Windows, cost, where data lives, whether it sends data, one lake on several laptops, getting help and build-season support expectations (linking Discussions and issue forms from #79). `mvp-public-release` 10.2 writes the announcement FAQ from this page. Verify: the link check passes.
- [ ] 12.7 Test first (corpus marker): with owlet cached, patch socket connections so any non-loopback connect raises. Then ingest, derive, report and serve one corpus match, and check that every step succeeds with no outbound attempt. Then write `privacy.md`:
  - no telemetry;
  - each outbound connection and when: owlet download, robot SSH and SFTP, rclone backup, Live CAN NetworkTables to the robot (only while the view is open), and docs links (only when clicked);
  - what a log and a lake contain;
  - sharing logs privately;
  - the `serve --host` caveat.

  Verify: the test passes, and the link check passes.
- [ ] 12.8 After #78's outside-team logs are checked (logs due 2026-11-28; fallback "not tested on real logs"), update `support-matrix.toml` with the `outside:<yyyy-mm>` evidence and issue links, and the AdvantageKit and non-Pro rows (text from `mvp-outside-team-logs` 7.3), and the Windows platform row from `mvp-public-release` 8.2 ("supported" or "rough"). Verify: the 12.1 tests pass, and `advantagekit.md` and `pro-and-temperature.md` agree with the matrix.

## 13. README, Why Flashpoint, home page and metadata (M2 draft, M3 final; 13.3 and 13.6, with 13.1's pin-tool case, by 2026-12-05, before `0.3.0rc1`, because `mvp-public-release` 4.4's link check fails the rc build without them)

- [ ] 13.1 Test first:
  - every README image must be an absolute URL under `https://igniterobotics.github.io/flashpoint/latest/assets/media/`, naming a manifest file;
  - a relative image must fail, naming the line;
  - the README quickstart block must have four commands or fewer, starting with an install;
  - `tools/pin-readme-media.py`, run on a fixture README for version `0.3.0`, must rewrite every docs-site `/latest/` image to `/0.3/`, every `blob/main/` and `tree/main/` repository link to `blob/v0.3.0/` and `tree/v0.3.0/`, and change nothing else; for `0.3.0rc1` the images go to `/0.3/` and links to `blob/v0.3.0rc1/`.

  Verify: the tests fail on today's README (expected).
  Partly done 2026-11-21: the pin-tool case (`tests/tools/test_pin_readme_media.py`, fixtures in `tests/fixtures/pin-readme/`) is in, because 13.3 needed it by 12-05. The three README-content cases come with 13.2, since today's README would fail them and the README's images don't exist until group 8.
- [ ] 13.2 Rewrite `README.md` (design D-j): pitch, hero GIF, three screenshots, "Works with / needs", the quickstart, "Try it" (demo and sample logs), docs links, the "Getting help" line (Discussions and issue forms; keep `mvp-public-release` 6.3's line if it landed first), project and license. Run `audience-style:de-slop`. Verify: the 13.1 README tests and the existing PyPI link test pass.
- [ ] 13.3 Add `tools/pin-readme-media.py` to the release workflow's build step, before `poetry build` and before `mvp-public-release`'s release-build link check (its task 4.4), and add `.png`, `.gif`, `.webm` and `.mp4` to its no-media assertion. Extend `tests/test_package.py` with the same no-media assertion for the wheel and sdist. Coordinate with #80, which owns `release.yml`. Verify: `pytest tests/test_package.py` passes, and a TestPyPI dry run's long description shows `/X.Y/` image URLs, tag-pinned repository links, and passes the link check.
  Implemented 2026-11-21: `release.yml`'s build job runs `tools/pin-readme-media.py` before `poetry build` (the README is read at build time) and before the link check, and its wheel assertion refuses `.png`, `.gif`, `.webm` and `.mp4`; #80 had left a placeholder comment for this step. `tests/test_package.py` asserts no media in the wheel or the sdist, building the sdist while the placeholders exist. Rehearsed on a scratch copy for `0.3.0rc1`: today's README has 14 moving-branch links; after pinning the built wheel passes the link check. Not ticked until a TestPyPI dry run (Josh: it publishes).
- [ ] 13.4 Write `why.md` (spec: Why Flashpoint page). Cover the FRC problems, then each differentiator followed by its manifest image or page link, then "What it doesn't do (yet)" linking the support matrix and known limits, then the demo and Quickstart links. Check every claim against the matrix. Run de-slop. Verify: the link check passes, and a reviewer confirms that no claim goes beyond the matrix (13.8).
- [ ] 13.5 Write `index.md` (the home): pitch, three images, demo link, and "start here" cards for Quickstart, Install, Tutorial and Robot side. Verify: the strict build passes.
- [x] 13.6 Point `pyproject.toml` `Documentation` and `Homepage` at the site, and add a test that the built wheel's metadata `Project-URL: Documentation` is the site URL. Verify: the test passes.
  Done 2026-11-21. Both point at `https://igniterobotics.github.io/flashpoint/`; the repository stays on the PyPI page as `Source`. `tests/test_package.py` checks the built wheel's `Project-URL` lines.
- [ ] 13.7 [HUMAN] A repo admin runs `gh repo edit IgniteRobotics/flashpoint` with:
  - `--description` set to the README pitch;
  - `--homepage https://igniterobotics.github.io/flashpoint/`;
  - `--add-topic` for `frc`, `wpilib`, `ctre`, `phoenix6`, `telemetry` and `datalog`.

  Verify: `gh repo view --json description,homepageUrl,repositoryTopics` shows them.
- [ ] 13.8 [HUMAN] Josh reviews `why.md`, `README.md` and `index.md` for tone and honesty (his "why Flashpoint kicks ass" bar). Verify: the review comments are resolved, and recorded on the PR.

## 14. Maturity labels (M3)

- [ ] 14.1 Test first: `docs/guide/maturity.toml` lists each labelled feature with its label and its pages. Every listed page must show the label in its first 10 lines. The README and `why.md` must show the label wherever they name the feature. Fill it from the release's labels: robot pull beta (#74; drop it if `mvp-install-and-first-run` 8.5 removed the label after the robot session), Live CAN experimental (`mvp-public-release` 5.x), Anomalies beta (#77). Verify: the test passes, and a manual check confirms that the app shows the same labels on the release candidate.

## 15. Release candidate, beta feedback and polish (M3 by 2026-12-19, M4 by 2027-01-04)

- [ ] 15.1 Capture the media from the release-candidate UI (the 0.3.0 candidate on `development`), and review every screenshot and animation for stale or private content. Verify: the `docs-media` artifact has every manifest entry within budget, and the reviewed list is recorded here.
- [ ] 15.2 [HUMAN] Before tagging 0.3.0, run 7.4's compile check with the tested-with versions on the release commit. Verify: it compiles, and the result is recorded here.
- [ ] 15.3 After the `v0.3.0` tag, check that:
  - `/0.3/` exists and `latest` points to it;
  - the CLI reference lists exactly the 0.3.0 subcommands;
  - every README image URL on the 0.3.0 PyPI page returns 200;
  - `/0.3/demo/` works with other hosts blocked;
  - the offline zip is on the GitHub Release.

  Verify: each result is recorded here.
- [ ] 15.4 [HUMAN] Someone unpacks `flashpoint-docs-0.3.zip` on a machine with networking disabled and opens it. Verify: pages, navigation, links and images work, and the result is recorded here.
- [ ] 15.5 Fold the outside-mentor beta findings (#78, done by 2026-12-14) into the docs: each docs finding is fixed, or filed and linked here. Verify: the link, commands-parse and strict checks pass.
- [ ] 15.6 Run `audience-style:de-slop` over every page not yet de-slopped: student and volunteer voice for user pages, technical voice for reference and under-the-hood pages. Verify: `pytest` (default markers), `ruff check .` and the strict build pass.
- [ ] 15.7 [HUMAN] Before 2027-01-04, a student or mentor who hasn't used Flashpoint follows the site's Quickstart and first-event tutorial from the published `latest` (0.3) docs. This is separate from `mvp-public-release` 8.1, which owns project-documentation 6.4 and team-setup 9.4. Note where they got stuck, and fix those spots. Verify: the notes and fixes are linked here.
- [ ] 15.8 Hand the hero GIF, the screenshots and the `reel` video to #81, and confirm that the announcement links `latest`, the demo and the Quickstart. Verify: #81 links the files.

## 16. Nice-to-have: robot samples compile in CI

- [ ] 16.1 Add `.github/workflows/robot-samples.yml`: non-blocking, `actions/setup-java` 17, `./gradlew compileJava` in `examples/robot-java/`, on PRs touching `examples/`. It is not added to `ci-ok`. Verify: it is green on a PR, and a PR with a deliberate syntax error turns it red (then revert).
