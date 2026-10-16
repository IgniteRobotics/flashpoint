## Context

Current state (see proposal.md for why this matters):

- `README.md` (152 lines) is organized by roadmap phase. Its command list leaves out `detect` and `backtest`.
- `deploy/README.md` covers the acquisition service well. Its install line (`pipx install flashpoint`) points at a package that isn't published.
- `notebooks/README.md` is current, and the guides link to it.
- `docs/rewrite/` and `docs/adr/` are the research and the decision log. They are accurate but written for the rewrite's planners, not for operators.
- The repository is private on GitHub (`IgniteRobotics/flashpoint`, made private 2026-10-14). Cloning it and fetching the golden corpus both need GitHub credentials (`gh auth login`, PR #52). `main` still holds the legacy code until `rewrite` merges into it once (06-roadmap.md §4).
- The CLI is one argparse parser built by `flashpoint.cli._parser()`, with 11 subcommands: `ingest`, `rebuild`, `derive`, `detect`, `backtest`, `report`, `serve`, `acquire`, `backup`, `restore`, `doctor`.

None of D1–D11 is touched. The guides describe D1 (Parquet lake and DuckDB), D2 (Polars), D3/D4 (readers and the owlet registry), D6/D7 (views, as amended by ADR-0013), and D11 (serial identity) as built. They link to the ADRs instead of restating them. No ADR is needed.

## Goals / Non-Goals

**Goals:**
- One place to start (the README). From there, two guides by audience, plus two shared reference pages.
- Every command in the guides runs as written against a real lake, checked once during apply.
- The two kinds of drift that already happened (a missing subcommand, a dead link) fail CI from now on.

**Non-Goals:**
- Executing code blocks automatically in CI (doctest-style). The commands need a corpus lake, a robot, or a service manager. The manual verification pass covers them instead.
- Checking heading anchors (`file.md#section`). The check covers the file part only.
- Restating thresholds or config values that live in `config/*.toml`. The guides link to those files.

## Decisions

### 1. Page layout under `docs/guide/`

| Page | Reader | Content |
|---|---|---|
| `docs/guide/README.md` | everyone | Index: which page to read for what |
| `user-guide.md` | students, mentors | Purpose, install, first ingest, the views, static export, robot config, lake location, `doctor`, event-day checklist |
| `auto-import.md` | whoever sets up the pit machine | Watch, `acquire.toml`, Linux, Windows, and macOS, backup and restore drill, status, troubleshooting |
| `how-it-works.md` | developers (curious mentors too) | Pipeline, lake layers, identity, clock alignment, detection, with one diagram |
| `developer-guide.md` | developers | Build from source, tests and markers, corpus, CI, OpenSpec workflow, branching, where decisions live |
| `cli-reference.md` | everyone | One `##` section per subcommand |

The spec's "developer guide" means `developer-guide.md` plus `how-it-works.md`. "How it works" gets its own page because mentors who never touch code still ask how it works, and a page of its own is easier to link to.

*Alternatives:*
- One long guide: rejected. Students would scroll past build and test material they don't need.
- One page per topic, about 15 small pages: rejected. That's too many places to keep current for a team this size.

### 2. `deploy/README.md` keeps service files only

It shrinks to what the files in `deploy/` are and how to install each one. Configuration, backup, troubleshooting, and the restore drill move to `auto-import.md`, which links back for the unit and task files. The test in `tests/test_deploy.py` checks the unit files, not the README, so nothing breaks. (Task 1.2 confirms this.)

*Alternative:* keep `deploy/README.md` as the import guide. Rejected: users don't look in `deploy/` for "how do logs get in".

### 3. Install command

Install from a local checkout: `gh auth login` once, `gh repo clone IgniteRobotics/flashpoint`, check out `rewrite` (until it merges into `main`), then `pipx install ./flashpoint`. Updating is `git pull` followed by `pipx install --force ./flashpoint`. A pit laptop keeps working offline after that; only cloning and updating need GitHub access.

*Alternative:* `pipx install git+https://…` straight from GitHub. Rejected now that the repo is private: it needs a credential helper or a token in the URL on every pit laptop, and a token in a URL ends up in shell history.

*Alternative:* publish to PyPI. That's out of scope, and the name `flashpoint` may already be taken there. That risk is why the bare `pipx install flashpoint` line is a defect, not just a stale line.

### 4. Hand-written CLI reference, with a coverage check

The reference is written by hand, with purpose, the options that matter, and examples. A unit test builds the parser with `_parser()`, reads the subcommand names from its subparsers action, and asserts that each one has a `## \`flashpoint <name>\`` heading in `cli-reference.md`.

*Alternative:* generate the reference from `--help`. Rejected: argparse help is terse and has no examples, and a generator adds a build step that has to stay in sync. Generating would remove drift entirely, but the coverage test catches the drift that matters (a new command) at near-zero cost.

### 5. Link check in pure Python

A test module, `tests/test_docs.py`, scans `README.md`, `deploy/README.md`, and `docs/guide/*.md`. For each inline link whose target isn't absolute (no scheme, no leading `#`), it resolves the target against the file's directory, strips any `#anchor`, and asserts that the path exists. A malformed `[text](` with no closing parenthesis is collected as a failure with file and line. All failures are reported together, not one at a time.

It runs in the default `pytest` marker set, so the existing Linux and Windows unit jobs pick it up. Paths go through `pathlib` so Windows separators work.

*Alternative:* lychee or markdown-link-check in a CI step. Rejected: a new tool, network access, and flakiness on external links, all for checking we don't need.

*Budget:* fewer than 10 files and under 100 KB of Markdown. The test runs in well under 1 s with negligible memory. Full logs are never touched.

### 6. Verification pass during apply

Before the docs tasks are marked done, every command block in `user-guide.md`, `auto-import.md`, and the README quickstart is run against a scratch lake built from the golden corpus (`FLASHPOINT_LAKE` pointed at a temp dir). What gets verified where:

| Commands | Where verified |
|---|---|
| Lake and CLI commands | Run on macOS (Josh's machine) |
| Linux service | The systemd path is checked by the existing CI unit-file test |
| Windows task | Checked by reading the XML only; not run |
| Robot pull | Checked by reading the code; needs a robot |

Each command's result is recorded in tasks.md, so it's clear which ones a person ran and which were only read.

### 7. Writing style

Plain language for the user guide, aimed at a high-school student who knows what a roboRIO is. Short sentences, commands in fenced blocks, and no roadmap jargon ("P3", "D4") in user-facing pages. `how-it-works.md` may name decisions, linked to their ADRs. Each page gets a final pass through the `audience-style:de-slop` skill (audience: students and volunteers for user pages, technical for developer pages).

## Risks / Trade-offs

- **The hand-written CLI reference drifts on options** (a new flag goes undocumented). The coverage test only checks subcommands. Mitigation: the reference links to `flashpoint <cmd> --help` as authoritative for the full option list, and documents only the options users actually reach for.
- **Students without GitHub access can't install or update.** Mitigation: the user guide says who to ask for org access, and a mentor can install from a USB copy of the checkout (`pipx install /Volumes/…/flashpoint`).
- **The "check out `rewrite`" step goes stale at the merge.** Mitigation: task 5.3 adds the edit to the merge checklist in 06-roadmap.md §4.
- **The README loses detail people relied on** (the DuckDB query examples). Mitigation: those examples move to `how-it-works.md` under "Querying the lake", and nothing is deleted without a new home.
- **The Windows and robot paths can't be fully exercised.** Mitigation: tasks.md records each as read-only verified, and the `[HUMAN]` check in task 6 covers the robot pull at the next pit session.

## Migration Plan

Docs only. The change merges into `rewrite` through a PR. Rollback is reverting that PR. No lake, config, or CLI change.
