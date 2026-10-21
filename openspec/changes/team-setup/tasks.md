## 1. Going public

- [ ] 1.1 [HUMAN] Review what a public repo exposes: corpus release assets (Ignite match logs), `config/` (CAN ids, serials), issues and history. Decide whether to keep the corpus public or move it to a private repo; record the decision in design.md Risks
- [ ] 1.2 [HUMAN] Claim the PyPI name `flashpoint` (fallback `flashpoint-frc`) with a 0.1.0 placeholder, and set up a PyPI trusted publisher for this repo's release workflow; verify the project page exists
- [ ] 1.3 If the corpus moves, point `tools/fetch-corpus.py` and CI at its new home; verify `python tools/fetch-corpus.py` and the CI corpus job pass
- [ ] 1.4 [HUMAN] Make the repository public; verify an anonymous `curl` of the corpus manifest URL behaves as decided in 1.1

## 2. Package and bundled defaults

- [x] 2.1 Write failing tests that the built wheel contains `flashpoint/defaults/` (season roots, `report.toml`, `anomaly.toml`, `anomaly-causes.toml`, units header) and no robot file, units rows, `.wpilog` or `.hoot` ("No team data in the package"); move the shared files to `src/flashpoint/defaults/` and set package data; verify `poetry build` and the tests pass, and the wheel is under 5 MB
- [ ] 2.2 Add PyPI metadata (description, license, URLs, classifiers, Python 3.11–3.13) and `.github/workflows/release.yml` (build on `v*` tag, trusted publishing, version from the tag); verify a dry run against TestPyPI installs with `pipx install --index-url https://test.pypi.org/simple/ flashpoint`
- [x] 2.3 Add a CI job that installs the built wheel into a clean venv on macOS, Windows and Ubuntu and runs `flashpoint --version` and `flashpoint doctor` with no configuration ("Clean install"); verify it passes

## 3. Configuration home and init

- [x] 3.1 Write failing tests for root resolution (explicit, `FLASHPOINT_CONFIG`, per-user folder per platform, no checkout fallback) and per-file default fallback ("Per-user folder", "Default thresholds"); implement in `config.py` and the loaders; set `FLASHPOINT_CONFIG` in `tests/conftest.py`; verify the full suite passes
- [x] 3.2 Write failing tests for `acquire.toml` `team` and `lake`, `lake_root()` order, and derived hosts ("Four-digit team", "Short and long team numbers", "No team"); implement; verify tests pass
- [x] 3.3 Write failing tests for `flashpoint init` ("First run", "Second run", "Bad team number", prompts only on a terminal); implement; verify tests pass, and `cli-reference.md` gains `init` (the CLI drift test passes)
- [x] 3.4 Write a failing test for "Derive without configuration" (warning naming `flashpoint init`, #54); implement; verify it passes, then close #54 when merged

## 4. Setup checks

- [x] 4.1 Write failing tests for the check registry: each check's ok, warning and problem cases (configuration root, each file loads, team set, robot for the current season, lake writable, owlet per Phoenix version in the lake, AdvantageScope, acquire service on Linux and Windows and "not applicable" on macOS, Python version); implement `flashpoint.setup.checks`; verify tests pass and the full run takes < 2 s on the corpus lake
- [x] 4.2 Rebuild `doctor` on the registry, keeping its informational lines, and make it exit 1 on any problem ("Missing configuration", "Invalid robot file"); verify `pytest tests/test_cli.py` passes and update `cli-reference.md`
- [ ] 4.3 Run the first-release checkpoint: groups 1–4 merged, tag `v0.2.0`, install with `pipx install flashpoint` on a clean machine, run `flashpoint init` and ingest the corpus; verify `doctor` reports only "no robot configured"

## 5. Guarded writes and fix actions

- [ ] 5.1 Write ADR-0017 (config writes and setup actions, amending ADR-0013 and ADR-0015) and add it to `docs/adr/README.md`; verify `pytest tests/test_docs.py` passes
- [ ] 5.2 Generalise the ADR-0015 guard for `X-Flashpoint: setup` with a 256 KB body limit; write failing tests for "Request without the guard" and "Shared on the network" (refused from a non-loopback client, `writable: false` in `/api/setup/checks`); implement; verify tests pass
- [ ] 5.3 Write failing tests for each fix action (init, owlet download and verify, AdvantageScope path, service install, service start), each run twice; implement as an allow-list; verify "Download owlet from the view" with a fake registry, and that the checks re-run after a fix

## 6. Config editor backend

- [ ] 6.1 Write failing tests for the save pipeline: tomlkit round-trip, validation through the loader models (including cross-file robot checks), atomic write, SHA-256 conflict check, backups pruned to 20 ("Valid slot edit", "Duplicate slot refused", "Changed on disk"); implement `flashpoint.setup.editor`; verify the untouched parts of `2026-comp.toml` are byte-identical after a slot edit
- [ ] 6.2 Write failing tests for `GET /api/config` (sections, source file, default flag, load errors: "Ignite configuration", "Broken file") and `PUT /api/config/<section>` for team, robots, thresholds and units ("Duplicate serial", reused label); implement; verify tests pass
- [ ] 6.3 Write failing tests for "What an edit affects" (each section names its command; the lake hash is unchanged after saves); implement; verify tests pass

## 7. Robot draft

- [ ] 7.1 Write failing tests for drafting from a session: devices from bronze, serials from the CAN inventory, bus names from bus-status entries, project and season ("Draft from Q7": 22 devices; "Log without hoots"; "Existing robot id"); implement; verify tests pass and Q7 drafts in < 1 s

## 8. Setup view

- [ ] 8.1 Add `web/static/views/setup.js` (`needsApi: true`) with the Checks tab: status per check, fix buttons when writable, commands otherwise; verify browser tests for a missing-owlet fix and for read-only when shared
- [ ] 8.2 Add the Configuration tab: sections with source file and default flag, inline edit forms, save per section, refusal messages shown in place, "what to run" after save; verify browser tests "Valid slot edit", "Duplicate slot refused" and keyboard reachability
- [ ] 8.3 Add the Draft robot tab: pick a session, complete the slots, save; verify a browser test that drafts from Q7 and saves a new robot file that `doctor` loads

## 9. Docs and roadmap

- [ ] 9.1 Rewrite the user guide's Install section for `pipx install flashpoint` and `flashpoint init`, add a Setup view section, and add a robot-side page with suggested Java samples for the CAN inventory and bus-status loggers; update the developer guide (`FLASHPOINT_CONFIG=config` in a checkout); verify `pytest tests/test_docs.py` passes
- [ ] 9.2 Update `docs/rewrite/06-roadmap.md` §Team setup and ADR-0009 (PyPI is now real); note in `p3-log-acquisition`'s `acquisition-service` delta that "No config file" now refuses naming `flashpoint init`; verify `openspec validate --all` passes
- [ ] 9.3 Run the full suite (`PYENV_VERSION=flashpoint poetry run pytest`, `-m browser`, `-m corpus`) and `openspec validate team-setup --strict`; verify all pass
- [ ] 9.4 [HUMAN] New-team walkthrough: on a clean laptop with no Flashpoint history, a student who hasn't used it installs from PyPI, runs init, drafts a robot from a log, fixes every Setup problem from the view, and imports a match; record where they got stuck
