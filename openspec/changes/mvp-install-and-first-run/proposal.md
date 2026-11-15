## Why

M1 of the MVP public release (#68, due 2026-12-05): a team that isn't 6829 installs Flashpoint on macOS, Linux or Windows with one command, and its first hour makes sense. Today the install refuses python.org's current Python, and nothing tests a real first run off Linux. Every import earns a backup warning pointing at a repo path pip installs lack. Replay sends practice-log users to a no-op command, Draft robot dead-ends on an empty lake, `doctor` turns red on 2027-01-01, and hoots without a wpilog give an empty History with no reason.

Roadmap: no P-phase; the M1 track of the "MVP public release" section of `docs/rewrite/06-roadmap.md`. It amends D9 (`05-target-architecture.md`). Pitfalls: partly #66 (new-season hand edits).

## What Changes

- **Install (#70):** documented install is `uv tool install --python 3.13 flashpoint`; pipx is the alternative (`py` launcher on Windows), and every upgrade or reinstall hint names both. `requires-python` widens to `<3.15`; CI tests 3.11–3.14. Install states the pyntcore floor (macOS 13+, glibc 2.35+, x86-64 Windows, no 3.15).
- **Cross-OS smoke (#71):** on Ubuntu, macOS and Windows: install the wheel, `init --team 9999`, ingest corpus E10 with a real owlet download, build Replay, check `serve`. Runs nightly, on `src/` PRs, and on tags, where PyPI publishing waits for it; outside `ci-ok`.
- **First-run copy (#72, #37, #38, #44):**
  - one docs-link module for every docs link in the CLI and app, and a test that no repo-relative docs path reaches users;
  - unconfigured backup is `ok (optional)`; configured backup without rclone says to install it;
  - empty Replay has three branches: no sessions; no session with a match key; keyed sessions not built;
  - `init`, derive's no-robot warning, the robot-season check and Draft robot's empty state say "ingest a log, then Draft robot", with a copy-ready command; the guide's order matches.
- **2027 season (#73):** robot-season is a warning while the lake has no current-season logs. `seasons/2027.toml` ships, and `init` copies only past and current seasons so later bundled fixes still reach existing installs. The guide states 2027 status; 0.3.1 gets an owner and date.
- **Robot pull (#74):** the rio checksum runs under `nice -n 19`. P3 11.1 and 11.3 run on a robot before 2026-12-07; until they pass, `acquire --help` and the guide label robot pull "beta, tested on one robot".
- **Hoot-only sessions, warning only (#8):** derive (so ingest) warns on sessions with hoots but no wpilog; `doctor` warns when a season's hoot sessions all lack one.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `team-setup`: install and Python range, season lookup, `init` copies, robot-season warning, next steps; adds docs links.
- `backup-status`: unconfigured backup is ok; missing rclone named.
- `config-editor`: Draft robot, empty lake.
- `match-reports`: adds the empty Replay state.
- `session-grouping`: adds hoot-only session reporting.
- `ci-quality-gates`: adds the first-run smoke job.
- `log-acquisition`: adds low-priority hashing and the beta label. Created when `p3-log-acquisition` archives (M0; design D-h).

## Non-goals

- The hoot-clock alignment path for #8 (post-MVP).
- An "Import a folder" UI action (#38's question; post-MVP).
- Opening on Setup when empty; History and Anomalies empty states.
- pyntcore as an optional extra, Python 3.15, Windows on ARM.
- Pausing pulls while the robot is enabled (a follow-up only if 11.1 shows overruns).
- The docs site and README (`mvp-documentation`); the release checklist (`mvp-public-release`).

## Impact

- Code: `pyproject.toml`, `setup/`, `acquire/`, `cli.py`, `web/server.py`, three views, a docs-URL module, `seasons/2027.toml`.
- CI: wider matrices, a smoke workflow that `release.yml` waits on, `tools/fetch-corpus.py --group`.
- Docs: Install, guide order, new-season steps, beta label, an ADR amending ADR-0009.
