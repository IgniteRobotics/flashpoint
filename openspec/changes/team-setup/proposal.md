## Why

Flashpoint works only for team 6829, and only for whoever can clone a private repo. The robot addresses are hard-coded for 6829 (`acquire/config.py`), Ignite's robots and seasons ship inside the package's `config/`, and a pipx install finds no configuration unless `FLASHPOINT_CONFIG` is set, and then fails silently (#54). Configuration means hand-editing TOML, and install problems surface as `doctor` lines at best. Another team can't start without a mentor who knows the code.

This extends D9 / ADR-0009 (`pipx install flashpoint`, which assumed a public package that doesn't exist yet), ADR-0013 (one local app) and ADR-0015 (guarded writes). It follows P3 and P4 on the roadmap (`06-roadmap.md`).

## What Changes

- **Public install:** the repo goes public and Flashpoint is published to PyPI, so any team runs `pipx install flashpoint` and gets a working command. Ignite's corpus and robot files leave the package.
- **Per-user configuration home:** configuration lives in a per-user folder (`~/.config/flashpoint`, or the platform equivalent). `flashpoint init` creates it from bundled defaults (season roots, thresholds, anomaly settings), asks for the team number and lake folder, and never overwrites an existing file. **BREAKING:** the fallback to the checkout's `config/` goes away. Ignite moves its robot, unit and threshold files to its own configuration folder; the guide shows how.
- **Team number drives robot addresses:** acquire's default hosts come from the team number (`10.TE.AM.2`, `roborio-TEAM-frc.local`, USB `172.22.11.2`), not 6829's.
- **Setup view:** a new view in the app shell showing:
  - the configuration: team, lake, acquire settings, robots and their slots and signals, thresholds, and the units list;
  - the setup checks `doctor` makes, plus new ones (no configuration, no robot for this season, owlet missing, AdvantageScope missing, acquire service not installed or running, lake not writable).
- **Editing configuration in the UI:** team and connection, robots and slots, thresholds and units can be changed from the Setup view. Every edit is validated with the same rules as loading, written atomically, and kept as a backup; an invalid edit is refused with the message loading would give.
- **Draft a robot from a log:** a new robot file can be started from an imported log's devices and CAN inventory, then completed in the UI. This is the fast path for a new team.
- **Fix actions:** known problems get a fix button: create the configuration, download and verify owlet, set the AdvantageScope path, install or start the acquire service. These run only on the machine running `serve`, behind the ADR-0015 guard. Anything else shows the exact command.
- **Robot-side guide:** suggested Java samples for the CAN inventory and bus-status loggers.

## Capabilities

### New Capabilities
- `team-setup`: package distribution, the configuration home and `init`, team-derived robot addresses, setup checks, and guarded fix actions.
- `config-editor`: the Setup view's configuration display and editing, validation and backups, and drafting a robot from a log.

### Modified Capabilities
None in `openspec/specs/`. Acquire's default hosts change through `team-setup`. The `acquisition-service` spec is still in the unarchived `p3-log-acquisition` change, and is reconciled when that change archives (see design.md).

## Non-goals

- A standalone installer that doesn't need Python, and signed app bundles.
- Hosting, accounts or sharing data between teams.
- Editing logs, lake data or derived results from the UI.
- A robot-side library or vendordep; samples only.
- The acquire service on macOS (still foreground, per ADR-0009).

## Impact

- Packaging: PyPI metadata, a release workflow, bundled defaults as package data. The repo goes public; the corpus fetch must not need private access.
- New CLI command `flashpoint init`; `doctor` gains checks; `config_root()` changes.
- New Setup view, new guarded write routes in `web/server.py`, and a new ADR amending ADR-0013 and ADR-0015.
- Docs: install, init and Setup rewritten for any team; the CLI reference gains `init`.
- Pitfalls fixed: none from `03`; fixes #54.
