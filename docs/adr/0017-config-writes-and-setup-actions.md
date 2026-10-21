# 0017. Configuration writes and setup actions

- Status: Proposed
- Date: 2026-10-21
- Amends: [0013](0013-views-one-local-app.md) (the read-only app) and [0015](0015-anomaly-triage-writes.md) (its guarded writes)
- Deciders: Josh
- Refs: openspec change `team-setup` (design.md D-e, D-f, D-g), D9, issue #54

## Context
Another team can't start with Flashpoint without a mentor who knows the code. Configuration means hand-editing TOML files whose mistakes show up later as unmapped slots, and install problems show as `doctor` lines. The `team-setup` change adds a Setup view that shows the configuration and the setup checks, and lets a team fix what it finds without leaving the app.

That needs two kinds of write the app doesn't make today. ADR-0015 allowed exactly one, an appended triage row, behind the ADR-0014 guard. Logs and derived layers stayed read-only.

## Decision
The local app accepts **two more kinds of write**, behind the same guard:

1. **Configuration saves**, `PUT /api/config/<section>` (team, `robots/<id>`, `thresholds/<file>`, units). A save:
   - is validated by the same pydantic models and cross-file checks the loaders use, and refused, writing nothing, with the loader's message;
   - is refused when the file's SHA-256 differs from the one the view loaded (someone edited it by hand);
   - is written to a temp file and moved into place (`os.replace`), keeping comments and untouched entries (`tomlkit` round-trip);
   - keeps the previous version under `<config root>/.backups/`, the last 20 per file.
2. **Setup actions**, `POST /api/setup/fix/<action>`, from a fixed allow-list of Python functions, never shell strings: `init` (the CLI's code), `owlet` (the registry's download and verify), `advantagescope` (writes the path through the editor), `service-install` (copies the shipped unit or task with the resolved executable path) and `service-start`. Each is safe to run twice and reports what it changed; the checks re-run afterwards.

**The guard** is ADR-0014's, generalised: loopback bind, loopback client, `Host` and `Origin` naming this machine, a JSON body, and `X-Flashpoint: setup`. Configuration bodies may be up to 256 KB (a robot file is about 20 KB); launch and triage keep 4 KB. `GET /api/setup/checks` reports `writable: false` when the guard can't pass, and the view then shows commands instead of buttons. Shared on the network or opened as a static export, the configuration is read-only.

**Still read-only to the app:** logs, the raw store, bronze, silver, gold, and the ledger's records of files and logs. A save never runs `derive`, `detect` or `report`; the view names the command to run.

## Consequences
- A team can go from `pipx install` to a configured robot without a text editor, and every file the app writes is one the CLI loads.
- The server now writes outside the lake, into the configuration root. Backups make every save reversible by hand.
- A fix can download (owlet) or touch the OS (the service). Both happen only on the machine running `serve`, from an allow-list, after a click.
- ADR-0013's "read-only" statement and the server docstring carry this exception, as they do ADR-0015's.

## Alternatives considered
- **Regenerate files from the models:** simpler, but drops the comments the robot files depend on (wiring notes, sources).
- **A separate setup app or wizard:** a second thing to install and keep in step with the checks; `doctor` and the view would drift.
- **Fix actions as shell commands from the checks:** any string the server builds and runs is an injection risk; a fixed list of functions isn't.
- **Writable from the pit network:** anyone on the network could change thresholds or robot slots. Deferred until the app has accounts, as for triage.
