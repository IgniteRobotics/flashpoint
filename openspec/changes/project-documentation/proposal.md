## Why

Flashpoint's documentation grew one roadmap phase at a time. The README is a changelog organized by phase ("P2", "P3", "P4", ...), the service setup is in `deploy/README.md`, and the architecture is in `docs/rewrite/`, which is research rather than a guide. A student at the pit, or a new mentor, can't answer "what is this, how do I run it, how do logs get in automatically" without reading all three. The docs have also started to drift from the code again (pitfall #67). The README's command list leaves out `detect` and `backtest`, and `deploy/README.md` tells people to `pipx install flashpoint`, a package that isn't published. The rewrite is close to merging into `main`, and this is the documentation the team will land on.

Roadmap: this isn't a numbered phase. It meets the **Docs** quality gate (docs/rewrite/06-roadmap.md §5) for the whole rewrite, and it describes the system built in P1–P5 and its follow-ups. It summarizes 05-target-architecture.md §2–§5 for readers; it doesn't replace that document.

## What Changes

- A **user guide** in `docs/guide/` for students and mentors who run Flashpoint:
  - what it does and what it's for;
  - install;
  - first ingest;
  - reading Replay, History, and Anomalies;
  - configuring a robot;
  - setting up automatic import.
- A **developer guide** in `docs/guide/` for people who change the code:
  - how it works (pipeline, lake layers, identity, detection);
  - building from source;
  - tests and markers;
  - the golden corpus;
  - CI;
  - the OpenSpec workflow and branching.
- An **automatic import guide**:
  - `flashpoint acquire --watch` pulling from the roboRIO and USB sticks;
  - `acquire.toml`;
  - running it as a Linux systemd user unit, a Windows logon task, or a macOS terminal/tmux session (no macOS service exists, and the guide says so);
  - rclone backup;
  - checking status with `doctor`;
  - troubleshooting.
- A **CLI reference** page with one section per `flashpoint` subcommand.
- **README** rewritten as a short overview with a quickstart that links into the guides. The phase-by-phase sections move into the guides.
- `deploy/README.md` slimmed to the service files and a link to the import guide. The install command is corrected.
- **Drift guards** in the unit test suite:
  - every relative link in the guides, README, and deploy README resolves;
  - every `flashpoint` subcommand has a section in the CLI reference.

## Capabilities

### New Capabilities
- `project-documentation`: the user, developer, import, and CLI guides. Covers what each must contain, where it lives, and the automated checks that keep it in step with the code.

### Modified Capabilities
None. No runtime behavior changes.

## Non-goals

- A docs site generator (MkDocs or similar), hosting, or GitHub Pages
- A macOS launchd service, or any new acquisition source (Google Drive, network shares). Documented as not available
- Changes to CLI behavior, help text, or config formats. Where the docs find a defect, it's filed as an issue, not fixed here
- Rewriting `docs/rewrite/` or the ADRs; the guides link to them
- Tutorials for AdvantageScope, rclone, or WPILib themselves beyond the steps Flashpoint needs
- Translating or screenshot-heavy walkthroughs; screenshots only where a view can't be described in text

## Impact

- New: `docs/guide/` (several Markdown pages), one unit test module for the doc checks under `tests/`.
- Rewritten: `README.md`, `deploy/README.md`. `notebooks/README.md` is linked, not changed.
- No new runtime or dev dependencies. The checks use the standard library and the existing CLI parser.
- CI: the checks run in the existing unit job on Linux and Windows. No new job.
