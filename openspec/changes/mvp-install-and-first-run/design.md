## Context

See proposal.md for the motivation and the seven delta specs for behaviour. What the code does today, checked on `development` at c027d6d:

- **Python range:** `pyproject.toml` says `>=3.11,<3.14`, and `setup/checks.py` has `PYTHON_RANGE = ((3, 11), (3, 13))` with the fix `pipx reinstall --python python3.12 flashpoint`. The CI lint-test matrix runs 3.11 and 3.12 only, so 3.13 is claimed but untested. Every runtime dependency inside its current cap ships cp314 wheels (checked on PyPI 2026-11-14: numpy 2.4, numba 0.68, llvmlite 0.50, pyarrow 25, duckdb 1.5, pyntcore 2026.2.2; polars is abi3). pyntcore has no cp315 wheel, and its wheels stop at macOS 13, manylinux_2_35 and win_amd64.
- **Clean install CI** (`ci.yml` `clean-install`) installs the wheel with pip into a venv and runs only `--version` and `doctor` with no configuration. The corpus and browser jobs are Linux only.
- **Repo-relative docs paths shown to users:** `checks.py:44` `BACKUP_GUIDE`, `checks.py:480` (service), and `live-can.js:19`. `tests/web/test_static_lint.py` forbids any remote host in the app's shipped files, which is why `live-can.js` names its guide instead of linking.
- **Backup:** `acquire/backup.py:447-454` returns a warning for every lake with logs and no remote. `_backup` (`checks.py:306-322`) suggests `flashpoint backup` without checking for rclone, although `CheckContext.which` exists.
- **Replay empty state** (`replay.js:479-500`) always says to run `flashpoint report`. When nothing was built, the server's stand-in index (`server.py:478-488`) carries `sessions_without_match_key: None`, so the view can't tell the three cases apart.
- **Next steps:** `cli.py:247` says "add your robot file to .../robots"; the robot-season check (`checks.py:253-264`) says the same and is a problem whenever no robot matches the calendar year; Draft robot's empty state (`setup.js:1170`) says only "Run flashpoint ingest first."
- **Seasons:** `load_config_seasons` (`robot_config.py:260`) merges bundled and root files per year, root winning, and the checks list both. **Verified:** a bundled `seasons/2027.toml` is read on an existing configuration that has no 2027 file of its own. The catch is `init`: `EDITABLE_DEFAULTS` (`setup/init.py:24`) copies named season files, and a copy shadows every later fix to the bundled file.
- **Rio hash:** `acquire/robot.py:300` runs a bare `sha256sum` with a 300 s timeout. Exit 127 means "no hashing command" (`size-verified`).
- **Hoot-only sessions:** derive writes `offset_us = NULL` for them and every downstream step drops them. The only sign is History's "Sessions without aligned samples: N".

## Goals / Non-Goals

**Goals:**
- Each first-run message names a step that works for an outside team on a pip install, in the order the guide gives.
- The cross-OS smoke follows the documented install path (uv) rather than a developer's venv.
- No lake format, derive or report version changes. Nothing re-derives.

**Non-Goals:**
- No change to how sessions are grouped or aligned (#8 proper is post-MVP).
- No new configuration keys.
- No rewrite of the guide pages. This change makes the M1-era text true; `mvp-documentation` rewrites it.

## Decisions

### D-a. uv is the documented install; this amends D9 and needs an ADR
- Primary: `uv tool install --python 3.13 flashpoint` on all three OSes. uv downloads a managed CPython when none fits, which fixes "python.org gave me 3.15" without asking a student to manage interpreters. Install uv from its standalone installer, Homebrew or winget.
- Alternative: pipx. On macOS and Linux, `pipx install --python python3.13 flashpoint`. On Windows, `py -3.13 -m pip install --user pipx`, then `py -3.13 -m pipx install flashpoint`.
- Why 3.13 and not 3.14: it's the newest version every dependency has shipped wheels for over several releases. 3.14 is supported and tested, not the default.
- `requires-python = ">=3.11,<3.15"`, classifiers 3.11–3.14, `PYTHON_RANGE = ((3, 11), (3, 14))`. The Python check's fix becomes the uv command.
- Every other install-method string follows: the owlet "can't read C<n>" fix (`checks.py:359`, today `pipx upgrade flashpoint`, the exact message a team sees on its first 2027 hoot) becomes `uv tool upgrade flashpoint` with pipx as the alternative; the service fix's "install it with pipx first" (`fixes.py:134`) names uv; the Windows task template's comment (`deploy/windows/flashpoint-acquire.xml:4-5`) says uv or pipx (both use `%USERPROFILE%\.local\bin`).
- D9 says "`pipx install` + per-user services". This is a deviation: a new ADR (the next free number, expected ADR-0018) amends ADR-0009. The D9 row in `05-target-architecture.md` changes to "`uv tool install` (pipx alternative) + per-user services". Services are unchanged: the systemd unit already runs `%h/.local/bin/flashpoint`, which is where both uv and pipx put the command, and the service-install fix action writes the resolved path (`setup/fixes.py:140`).
- *Alternative:* `pipx install --fetch-missing-python`. Rejected: Ubuntu 24.04's apt pipx (1.4.x) predates the flag (to be confirmed in task 1.4), and it still needs a Python to run pipx.
- *Alternative:* pyntcore as a `[live]` extra to lower the platform floor. Deferred: it changes the install story late. The floor is documented instead.

### D-b. One docs-URL module, served to the app through a read-only route
- `flashpoint/docs_links.py` holds `DOCS_BASE` and `docs_url(page, section=None)`, with pages named by id (`auto-import`, `live-can-health`, `user-guide`, ...). Every CLI, check and server string that points at documentation calls it.
- `DOCS_BASE` is `https://igniterobotics.github.io/flashpoint/`, the versioned site that `mvp-documentation` publishes ("Published, versioned docs site"). The version segment comes from the installed package: `X.Y` for a final `X.Y.Z` and for its pre-releases (`rcN`, `aN`, `bN`), `dev` for development builds (`.devN`, local). This follows `mvp-documentation` D-c, where a pre-release tag deploys `X.Y` with a pre-release banner: `0.3.0rc1` (the outside-mentor beta build, ~12-07) links to the `/0.3/` docs its own tag deployed, the same version its PyPI page's images use, so beta testers read docs frozen at the build they installed. `mvp-documentation` owns the page-to-path rule and moves `docs/guide/` once, early (by 2026-11-28). The module follows that layout, so nothing in this change moves guide files.
- A new `GET /api/docs` returns `{page id: address}`, read-only and unguarded like other GETs. Views fetch it in served mode and render the address as a link. In static mode (no API) they render the page and section title. The shipped JS never contains an address, so the "no remote host" scan stays as it is (D6, ADR-0013).
- **Drift test:** every `docs_url` page and section resolves to a source page and a heading anchor that exist in the docs tree, so a renamed heading fails CI. A second test greps `src/flashpoint` for `docs/guide/` in user-facing strings.
- *Alternative:* keep naming files (`see docs/guide/...`). Rejected: those paths don't exist in a pip install.
- *Alternative:* link to GitHub `blob/main`. Rejected: `mvp-public-release` forbids `blob/main` links in the released package, and `main` doesn't track the installed version.

### D-c. The Replay empty state reads counts from the index
- The build's `matches.js` index and the server's stand-in index both carry `sessions`, `keyed_sessions`, `sessions_without_match_key` and `quarantined`. The counts come from the meta sessions table and the ledger, never from logs.
- The view picks its state: `sessions == 0` gives no sessions; `keyed_sessions == 0` gives no match key; otherwise not built. An index from an older build without the fields gives the no-sessions state with no counts, until the next build rewrites it (every build rewrites the index, so `REPORT_VERSION` isn't bumped).
- In served mode the no-sessions state fetches `/api/setup/checks` once for the problem count. That's only on an empty lake, so its 2 s budget doesn't slow normal use.

### D-d. Robot-season reads the lake's seasons; init copies only past and current seasons
- `CheckContext` gains a cached "seasons with sessions" read from the meta sessions table (quarantined files form no session). Problem when the current season is in it and no robot matches, warning otherwise. The fix text names Draft robot, then the robot folder.
- `init` copies `seasons/<year>.toml` for every bundled year up to `current_season()`, injected for tests. A December install doesn't copy 2027, so a 0.3.1 fix to the bundled 2027 roots reaches it.
- `seasons/2027.toml` is the 2026 file with `season = 2027`. Systemcore or WPILib 2027 root changes go into 0.3.1.
- The kickoff-week 0.3.1 (2027 owlet compliancy, pyntcore `<2028`, any 2027 roots) gets its owner and date in the release checklist owned by `mvp-public-release` (#80; its task 9.2, default Josh, 2027-01-16). This change supplies only the 2027 known-limits lines and a comment on #73.

### D-e. Backup: optional means ok
- `backup_status` returns level `ok` with `optional: true` when no remote is set. The message is unchanged ("exist only on this machine"), and the fix is `docs_url("auto-import", "backup-and-restore")`.
- When a remote is set and `ctx.which("rclone")` is None, the level is at least `warning`, and the command is "install rclone (https://rclone.org/install/), then run `flashpoint backup`". The overdue badge still keys only on `problem`.

### D-f. First-run guidance comes from the server
- `/api/setup/sessions` gains `lake` and `import_command` when it lists no session. The command is quoted with `shlex.quote` on POSIX and double quotes on Windows. Double quotes work in both PowerShell and cmd for paths with spaces.
- `init`'s last line becomes `next: import a match's logs with flashpoint ingest <folder>, then run flashpoint serve and use Setup > Draft robot`.

### D-g. Hoot-only: derive warns, a check reports; no alignment change
- `Deriver.run` already knows each rebuilt session's `wpilog_id`. It returns the rebuilt hoot-only sessions, and `_derive` prints one warning line, so ingest shows it through its derive subprocess.
- `_derive`'s existing no-robot warning (`cli.py:310-312`, "add one" to the robots folder) is the first thing a new team sees after its first ingest; it names Draft robot too (#38).
- A `hoot-only` check reads the meta sessions table: the latest season with hoot sessions, and whether every one of them lacks a wpilog. Warning only in that case. 6829's lake has 4 hoot-only sessions among many with wpilogs (reboots mid-event) and would otherwise carry a permanent warning. Those still show at ingest time, which is when they can be acted on.
- *Alternative:* the hoot-clock path (offset 0, method `hoot-clock`). Post-MVP. If it lands later, it needs its own clock-method column rather than a NULL or 0 sentinel (skeptic note on #8).

### D-h. Rio hash at nice 19; log-acquisition is the home
- Once per connection, `command -v nice` decides the command: `nice -n 19 sha256sum -- <path>`, or bare `sha256sum` with one note per host per cycle. Probing first keeps exit 127 meaning "no sha256sum" (coreutils `nice` also exits 127 when its command is missing).
- `ionice -c3` is not added yet. 11.1 measures with and without nice, and ionice joins only if flash I/O shows up.
- **Where the requirements live:** `log-acquisition` is created by `p3-log-acquisition`, which the umbrella archives in M0 (by 2026-11-21). These are ADDED requirements, not a MODIFIED "Verified transfer": an ADDED delta archives cleanly whichever change archives first, while a MODIFIED one fails if P3 hasn't archived. The beta label covers service mode (an `acquisition-service` concern) too, but it sits here so the label lives in one place.
- The label is the default deliverable. If 11.1 and 11.3 pass before 2026-12-07, task 8.5 removes it by setting the one maturity constant to none, and drops the "Robot pull labelled beta" requirement from this delta before archive. The label tests are written against that constant, so removing the label edits no test (test freeze).

### D-i. Smoke job: a reusable workflow, driven by one script
- `.github/workflows/first-run-smoke.yml` runs on `schedule` (nightly), `pull_request` with `paths: [src/**, pyproject.toml, poetry.lock, tools/first-run-smoke.py, the workflow]`, and `workflow_call` with an optional wheel-artifact input. It is not in `ci-ok`'s needs. Tags reach it only through `release.yml`, which calls it with the wheel its `build` job made, and its `pypi` job `needs` the call. The gate is specified here (ci-quality-gates, "First-run smoke on every platform"); `mvp-public-release` design and task 4.3 rely on that and only check the wiring and the GitHub Release ordering (public-release, "A tag publishes the package and a GitHub Release"). This change wires the `needs` (task 9.4).
- `tools/first-run-smoke.py` runs the five steps with named output and per-step timeouts, with `FLASHPOINT_CONFIG`, `FLASHPOINT_CACHE` and the lake all in fresh temp folders (so the owlet download is real) and its working directory outside the checkout:
  - build the wheel (or take the release's), then `uv tool install --python 3.13 <wheel>` into a temp `UV_TOOL_DIR`, so the documented install path itself is tested; uv comes from `astral-sh/setup-uv` pinned by SHA (ci-quality-gates, "CI supply chain");
  - `flashpoint init --team 9999 --lake <tmp>`, run from outside the checkout;
  - `fetch-corpus.py --group 2026-gacmp-e10`, then `flashpoint ingest`, with no owlet cache, so the real download and checksum run;
  - `flashpoint report`;
  - `flashpoint serve --port 0`: parse the printed address, GET `/` and `/api/setup/checks`, then terminate (Ctrl+Break on Windows) and kill after 10 s. `serve` opens no browser, so it runs headless as is.
- E10 is the corpus pick: 27 MB, both buses, and its second hoot group is hoot-only, so the smoke also sees the #8 warning on every OS.
- `fetch-corpus.py` gains `--group`. (Its `--help` already works; #11 is `update-owlet-manifest.py` and isn't in this change.)
- The existing `clean-install` job stays in `ci-ok`. Its OS matrix gains a Python axis of 3.11, 3.13 and 3.14 (lowest, documented default, highest; #70 asks for 3.13 and 3.14), so the required check installs the wheel on the documented interpreter on every OS. Nine short jobs; each is about 3 min today.

### Architecture decisions
- **D9 / ADR-0009:** amended by a new ADR (D-a).
- **D4:** the smoke exercises the owlet fetch and checksum on all three OSes for the first time.
- **D6 / D7 / ADR-0013:** the offline rule holds; docs addresses come from the server (D-b). The empty Replay links to History.
- **D1 / D2:** new counts are DuckDB reads over meta. No new lake tables.
- **D11:** untouched. Draft robot still uses the CAN inventory.
- D3, D5, D8 and D10 are untouched.

## Budgets
- **Setup checks:** still < 2 s in total on the corpus lake; the new season and hoot-only reads take < 100 ms together and share one meta read.
- **Index counts:** < 100 ms in the build, and in the server's stand-in index on a 500-session lake.
- **Smoke job** (touches full logs): < 15 min per OS. Ingest, derive and report of E10 take < 4 min on a hosted runner, excluding the owlet download. Peak RSS < 1 GB, since each step runs in its own process (`cli.py:_derive_and_report`).
- **Derive warning:** no extra log reads; it uses the rows derive already holds.
- **Rio hash:** no budget change on the laptop side. 11.1 records the rio's hash time per 100 MB with and without nice, under robot code load.

## Risks / Trade-offs
- [Under nice on a CPU-starved rio, hashing a 200 MB hoot could pass the 300 s timeout, count as a failed attempt, and after 3 end `failed`] → 11.1 measures it. If the hash takes > 150 s per 200 MB, a follow-up scales the timeout with file size.
- [Smoke flakes on hosted runners (network, owlet CDN, Windows process teardown) and gets ignored] → It is outside `ci-ok` by design, but it gates release publishing, so a red nightly is visible before tagging. Each step names itself.
- [The smoke adds about 15 min to a release] → Accepted. The release is the one time it must pass.
- [In-app links 404 until the docs site's first deploy, and its page paths may still move] → `mvp-documentation` deploys the site and does its one restructure by 2026-11-28, before `0.3.0rc1` (around 12-07). The drift test fails if a link targets a page or heading that no longer exists.
- [A heading rename breaks a docs link] → The anchor drift test (D-b).
- [A team that edited a past season file never gets bundled fixes to it] → That's the existing rule: a team's file wins. The new-season guide says so.
- [The hoot-only check stays ok for a team that mixes wpilog and hoot-only sessions] → They see the ingest-time warning each time it happens.
- [The `py` launcher isn't installed with every Windows Python] → uv is primary. The Windows pipx route says to tick "py launcher" in the python.org installer.

## Migration Plan
1. No data migration: no `DERIVE_VERSION` or `REPORT_VERSION` bump. The next `report` rewrites the index with its counts.
2. Existing pipx installs keep working (`pipx upgrade flashpoint`). The guide gives the switch to uv as optional: `pipx uninstall flashpoint`, then the uv command. The configuration and lake are untouched.
3. Existing configuration roots keep their season copies. 2027 comes from the package.
4. Teams with no backup remote lose a warning. 6829's configured backup is unchanged.
5. Rollback: revert the PR. There is nothing to undo in lakes or configuration.

## Open Questions
- The page-to-path rule (for example `auto-import/#backup-and-restore` or `how-to/robot-pull/`), from `mvp-documentation`'s restructure. It lives only in `docs_links.py`, so it doesn't change the specs or tasks.
- Resolved 2026-11-15: Josh owns the kickoff-week 0.3.1, target 2027-01-16 (`mvp-public-release` task 9.2).
- Page-to-path slugs arrive from `mvp-documentation` task 5.2 (a comment on #72); task 2.3 here waits on it.
