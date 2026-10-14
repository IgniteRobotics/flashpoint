## 1. Drift checks first (TDD)

- [ ] 1.1 Cut `feature/project-documentation` from `rewrite`. Write `tests/test_docs.py` with the relative-link check (design §5). Cover these cases with fixtures under `tests/fixtures/docs/`: a good link, a broken link, an anchor-only link, an absolute web link, and a malformed `[text](` link. Verify that `pytest tests/test_docs.py` passes on the fixtures, and that the run against the real repo fails, naming the files and lines of any current broken links.
- [ ] 1.2 Add the CLI coverage test: build `flashpoint.cli._parser()`, collect the subcommand names, and assert that each has a ``## `flashpoint <name>` `` heading in `docs/guide/cli-reference.md`. Confirm that `tests/test_deploy.py` checks unit files, not `deploy/README.md` text. Verify that the coverage test fails with a message naming all 11 missing subcommands (the reference doesn't exist yet).

## 2. Reference pages

- [ ] 2.1 Write `docs/guide/cli-reference.md`. Each subcommand gets a section with its purpose, the options users reach for, at least one example, and a pointer to `--help` for the full option list. Verify that the coverage test from 1.2 passes and that each example's `--help` matches the options documented.
- [ ] 2.2 Write `docs/guide/how-it-works.md`. Cover:
  - the pipeline from raw to bronze, silver, and gold, with the command that triggers each step;
  - lake layers and views;
  - match identity and session grouping;
  - slot versus unit;
  - clock alignment;
  - detection kinds;
  - "Querying the lake", with the DuckDB examples moved from the README.

  Add one diagram, and link the ADRs and 05-target-architecture.md. Verify that the link check passes and that the moved DuckDB snippets run against a corpus lake.

## 3. User guide

- [ ] 3.1 Write `docs/guide/user-guide.md`, part 1. Cover:
  - what Flashpoint is and isn't (AdvantageScope for single-log deep dives);
  - install on macOS, Linux, and Windows from a local checkout (`gh auth login`, clone, `pipx install`), plus updating and installing from a USB copy (design §3);
  - lake location and `--lake`;
  - ingesting by hand;
  - the offline-prep step (owlet fetched while online).

  Verify that the link check passes.
- [ ] 3.2 Write `docs/guide/user-guide.md`, part 2. Cover:
  - `serve`, with Replay, History, and Anomalies (shared filters, triage);
  - the static USB export;
  - describing a robot in `config/robots/<robot>.toml`, and NT signals;
  - reading `doctor`;
  - an event-day checklist.

  Link `config/*.toml` instead of restating thresholds. Verify that the link check passes.

## 4. Automatic import guide

- [ ] 4.1 Write `docs/guide/auto-import.md`. Cover:
  - what the watch pulls, and the never-modify guarantee;
  - every `acquire.toml` key with its default, checked against the config loader;
  - the dry run;
  - the Linux systemd unit (with linger and udiskie);
  - the Windows scheduled task;
  - macOS foreground or tmux, stated as having no service;
  - a statement that cloud-folder import isn't supported.

  Verify that every key in the loader's schema appears in the guide, and that the link check passes.
- [ ] 4.2 Add backup and restore drill, status (`doctor`), and troubleshooting to `auto-import.md`. Troubleshooting covers: robot unreachable, USB not seen, file skipped as still being written, low robot space, stale lock, and a leftover `.part` file. Take each symptom's wording from the actual log and doctor output in `src/flashpoint/acquire/`. Verify that the link check passes.
- [ ] 4.3 Slim `deploy/README.md` to the service files and their install steps, with a link to `auto-import.md`. Fix the install line. Verify that `pytest tests/test_deploy.py tests/test_docs.py` passes.

## 5. Developer guide and README

- [ ] 5.1 Write `docs/guide/developer-guide.md`. Cover:
  - building from source (pyenv or system Python 3.11–3.13, Poetry);
  - lint and type checks;
  - test markers and when each runs;
  - `tools/fetch-corpus.py` (needs `gh auth login`; the release is private);
  - what each CI job runs;
  - the OpenSpec propose → apply → archive flow;
  - `feature/*` → `rewrite` branching;
  - where decisions and research live;
  - the notebooks link.

  Verify that the build and unit test steps pass on a fresh clone in a temp dir.
- [ ] 5.2 Write `docs/guide/README.md` (the index), then rewrite `README.md` as an overview with a quickstart of five commands or fewer and links to all four guides, with no phase-named headings. Check that every section removed from the old README has a new home. Verify that the link check passes and that `grep -nE '^#+ .*\bP[0-6]\b' README.md` prints nothing.
- [ ] 5.3 Add the "drop the check-out-`rewrite` install step" step to the merge checklist in `docs/rewrite/06-roadmap.md` §4. Verify by reading the diff.

## 6. Verification pass

- [ ] 6.1 Build a scratch lake from the golden corpus (`FLASHPOINT_LAKE` set to a temp dir). Run every command block in the README quickstart, `user-guide.md`, `cli-reference.md`, and `auto-import.md` that doesn't need a robot or a service manager. Fix any doc that is wrong. Record here each command group and its result, and mark which ones were run and which were only read (design §6).
- [ ] 6.2 File a GitHub issue for any CLI or help-text defect the pass finds, rather than fixing it here (proposal non-goal). Verify that the issues are linked in this task.
- [ ] 6.3 Run the `audience-style:de-slop` skill over each guide (students and volunteers for user pages, technical for developer pages). Verify that `pytest` (the default markers) and `ruff check .` pass afterward.
- [ ] 6.4 [HUMAN] At the next pit session, a student who hasn't used Flashpoint follows `user-guide.md` from install to Replay, and someone follows `auto-import.md` to pull from the robot. Note where they got stuck, and fix those spots.
