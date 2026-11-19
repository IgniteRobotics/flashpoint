Test freeze: each group's first task(s) write its failing tests. Once a group's implementation starts, nothing edits those tests. If one looks wrong, stop and tell a mentor.

Dates: every task without `[HUMAN]` is due by M1 (2026-12-05), the smoke job included (the M1 gate in `mvp-public-release` 7.2 needs it green on all three OSes). 8.4 runs at the robot session, before 2026-12-07, and 8.5 follows it. 9.5 runs to 2026-12-12. The Windows row of 1.4 runs with `mvp-public-release` 8.2 by 2026-12-17; Windows flakes from 9.5 may be fixed up to M3 (2026-12-19). The M1 macOS and Linux walkthrough is `mvp-public-release` 8.1, and the kickoff-week 0.3.1 owner and date are its task 9.2; neither is repeated here.

## 1. Install and Python range (#70)

- [x] 1.1 Write failing tests: the package's declared range is `>=3.11,<3.15` with classifiers 3.11–3.14, and each is a `lint-test` matrix entry in `ci.yml` ("Declared Pythons are tested"); `clean-install` runs 3.11, 3.13 and 3.14 on each OS; the Python check for 3.10 names 3.11 to 3.14 and the `uv tool install` command, and 3.14 is ok ("Unsupported Python"); a synthetic unreadable hoot format gives the `uv tool upgrade flashpoint` fix with pipx as the alternative ("Hoot format newer than this release"); no string under `src/flashpoint` tells the user to install with pipx alone; verify they fail on `development`
- [ ] 1.2 Widen `requires-python`, add the 3.14 classifier, set `PYTHON_RANGE` and the check's fix, add 3.13 and 3.14 to `lint-test`, add the 3.11/3.13/3.14 axis to `clean-install`; change the owlet upgrade fix (`checks.py:359`), the service fix's "install it with pipx first" (`fixes.py:134`) and the Windows task template comment; refresh `poetry.lock`; verify the 1.1 tests pass and `ci-ok` is green with the new matrix entries
- [x] 1.3 Rewrite the user guide's Install and Updating sections for `uv tool install --python 3.13 flashpoint`, with pipx as the alternative (the `py -3.13 -m pipx` form on Windows, and ticking "py launcher" in the python.org installer), the pyntcore floor (macOS 13+, glibc 2.35+, x86-64 Windows, no 3.15), and the optional pipx-to-uv switch; change only the README quickstart's install line (the rest of the README is `mvp-documentation`'s); verify `pytest tests/test_docs.py` passes and no guide names `python3.12`
- [ ] 1.4 [HUMAN] Run the documented command on a fresh macOS (Homebrew Python 3.14 or 3.15 only), a fresh Ubuntu 24.04, and a fresh Windows 11 (python.org 3.15 only, or none), then `flashpoint --version` and `init`; record each result and the apt pipx version on 24.04 in design.md ("Fresh machine with no suitable Python"). macOS and Ubuntu by 2026-12-05; Windows by 2026-12-17, in the same session as `mvp-public-release` 8.2
- [x] 1.5 Write the ADR amending ADR-0009 (uv primary, pipx alternative, Python 3.11–3.14, the platform floor), add it to `docs/adr/README.md`, add an implementation note to ADR-0009, and change the D9 row in `docs/rewrite/05-target-architecture.md`; fill the ADR number into `mvp-public-release` design D-i's D9 line (already reworded to "amended"); verify `pytest tests/test_docs.py` passes

## 2. Documentation links (#72)

- [x] 2.1 Write failing tests for the links module: no string under `src/flashpoint` shown to users contains `docs/guide/`; every check in a synthetic not-ok state gives docs addresses that start with the published base ("Every check's fix"); every `docs_url` page and section exists as a source page and heading anchor in the docs tree; addresses use `0.3` for 0.3.1 and 0.3.0rc1, and `dev` for 0.4.0.dev2 ("Links match the installed version"); verify they fail
- [x] 2.2 Write failing tests for the app side: `GET /api/docs` returns the page map; Live CAN shows the published address in served mode and the title in a static export ("Live CAN guide reference", "Static export names the guide"); verify they fail
- [x] 2.3 Add `flashpoint/docs_links.py` (the versioned docs site base, the page slugs `mvp-documentation` posts on #72 in its task 5.2) and replace `BACKUP_GUIDE` and the service guide string in `checks.py`; verify the 2.1 tests pass
- [x] 2.4 Add `GET /api/docs` and replace `live-can.js`'s file name with the served address or the title; verify the 2.2 tests and `tests/web/test_static_lint.py` pass

## 3. Backup is optional (#72, #44)

- [x] 3.1 Write failing tests with a fake `which` and the corpus lake: "Not configured" (ok, optional, published guide address, no `doctor` warning), "Configured without rclone", "Overdue without rclone", "Unreadable records"; verify they fail and that the other backup-status tests still pass
- [x] 3.2 Implement in `backup_status` and `_backup`; verify the 3.1 tests and the overdue-badge browser test pass

## 4. Empty Replay (#72, #37)

- [x] 4.1 Confirm corpus `2025-nofms` has no match key (else build a synthetic wpilog with no FMS data and a plain filename). Write failing tests: the built index and the server's stand-in index carry `sessions`, `keyed_sessions`, `sessions_without_match_key` and `quarantined`, in < 100 ms on a 500-session synthetic lake; verify they fail
- [x] 4.2 Write failing browser tests for "Blank lake", "Practice logs only", "Keyed but not built", "Only a quarantined file" and "Static export of practice logs"; verify they fail
- [x] 4.3 Add the counts to `write_index` and the server's stand-in index; verify the 4.1 tests pass and Q7's match data file is byte-identical before and after
- [x] 4.4 Implement the three states in `replay.js`, with the Setup link and problem count in served mode only, and titles instead of addresses in static mode; verify the 4.2 browser tests and the static lint pass

## 5. Next steps and Draft robot (#38)

- [x] 5.1 Write failing tests: `init`'s last line names `flashpoint ingest` and `flashpoint serve` ("First run"); derive's no-robot warning names Draft robot ("Derive with no robot"); `/api/setup/sessions` on an empty lake returns the lake and an import command quoted for POSIX and Windows paths with spaces ("Lake path with a space"); browser tests for "Empty lake" and "Only quarantined files" in the Draft robot tab; verify they fail
- [x] 5.2 Implement the `init` line, the derive warning, the API fields, and the Draft robot empty state in `setup.js`; verify the 5.1 tests pass
- [x] 5.3 Write failing tests for the automatic draft: "First import of Q7", "Later import keeps the draft", "Team's own robot untouched", "Two robot projects" (synthetic sessions), "Hoot-only first import", "Opted out", "Existing lake gets its draft" and "No configuration" (team-setup, "First derive drafts the robot"); verify they fail
- [x] 5.4 Implement `auto_draft` in `setup/draft.py`, the call and `--no-robot-draft` in `_derive` (and so `ingest`, `rebuild`, the acquire cycle), the comment line, the printed line, and re-deriving sessions whose robot changed; add the option to `cli-reference.md`; verify the 5.3 tests and the CLI drift test pass, and that 6829's lake with `config/` drafts nothing
- [ ] 5.5 Write a failing test for "Same CAN id on two buses", then make draft slot ids name their bus whenever a (model, CAN id) is on two buses; verify it and "Draft from Q7" (22 slots, ids unchanged) pass
- [ ] 5.6 Write failing tests for "Drafted robot not named yet" and the Setup view's unnamed-slot mark (browser); implement both; verify they pass
- [ ] 5.7 Write failing browser tests for "Save starts a rebuild" and "Rebuild already running"; implement the detached derive on a draft save and the view's message; verify they pass
- [ ] 5.8 Write failing tests for "Match with no robot" and the reworded "Hoot-only session" reason (served and static); implement the reason by cause in `report/build.py` and the Draft robot link in `replay.js`; verify they pass and Q7's data file with a robot is byte-identical
- [ ] 5.9 Reorder the user guide's Install section and `team-setup.md` §3 so that importing a log comes before the robot, and say the first import drafts it (name the slots' subsystem and role in Setup, keep the drafted ids; Draft robot by hand for hoot-only logs or `--no-robot-draft`); change "the only problem should be no robot configured" (`user-guide.md:43`) and "`PROBLEM Robot for <year>`" (`team-setup.md:89`) to the new warning-or-problem rule; verify `pytest tests/test_docs.py` passes

## 6. 2027 season (#73)

- [ ] 6.1 Write failing tests: "New year, last year's logs", "Current-season logs and no robot" and "Only a quarantined log this season" for the robot-season check, with the fix naming Draft robot; "Newer bundled season on an existing configuration" and "Team's season file wins"; "Init before the next season" with the date injected; the wheel holds `seasons/2027.toml`; verify they fail
- [ ] 6.2 Add the lake-seasons read to `CheckContext` and change the robot-season level and fix; verify its 6.1 tests pass and the full checks run stays < 2 s on the corpus lake
- [ ] 6.3 Make `init` copy seasons up to the current one, and ship `defaults/seasons/2027.toml`; verify the remaining 6.1 tests pass
- [ ] 6.4 Update the user guide's "Starting a new season" for the bundled 2027 file and the January warning, and hand `mvp-documentation` the 2027 lines for the known-limits page (2027 hoots need the owlet that 0.3.1 adds; pyntcore is capped `<2027` until 0.3.1; Systemcore untested); comment on #73 that the 0.3.1 owner and date live in `mvp-public-release` 9.2; verify `pytest tests/test_docs.py` passes

## 7. Hoot-only sessions, warning only (#8)

- [ ] 7.1 Write failing tests: "Hoot logger restart" (all E10 files; one warning naming `2026gacmp_e10`, exit 0, check ok with 1), "Hoots only" (Q19 2025; check warning naming 2025 and WPILib data logging), "Re-run with nothing new", "Quarantined hoot"; verify they fail (corpus marker for the E10 and Q19 cases)
- [ ] 7.2 Return the rebuilt hoot-only sessions from derive and print the warning in `_derive`; verify the derive-side 7.1 tests pass and E10 derive time is within 5% of before
- [ ] 7.3 Add the `hoot-only` check; verify the check-side 7.1 tests pass and the full checks run stays < 2 s
- [ ] 7.4 Add a paragraph to the user guide's "Import logs by hand" saying that motor history needs a wpilog, and what the warning means; hand the line to `mvp-documentation` for the robot-side "What to log" guide (#83); verify `pytest tests/test_docs.py` passes

## 8. Robot pull: low-priority hash and beta label (#74)

- [ ] 8.1 Write failing tests against the fake SFTP robot: "Hash at low priority" (the recorded command starts with `nice -n 19`), "No way to lower priority" (bare hash, `verified`, one note per host per cycle), "No checksum command" and "Priority command without a checksum command" (`size-verified`, no failed attempt); verify they fail
- [ ] 8.2 Write failing tests for the label, written against one maturity constant so that removing the label later is a code change, not a test edit: while the constant is "beta", `acquire --help` labels robot pull and service mode "beta, tested on one robot" ("Help text"), the removable-volume options carry no label ("Removable volumes unlabelled"), and the guide's label text equals the constant (project-documentation "Beta label matches the app", owned by `mvp-documentation`); verify they fail
- [ ] 8.3 Implement the probe and the nice'd hash in `acquire/robot.py`, the maturity constant and the help text, and the label at the top of `auto-import.md` and of its "Run it in the background" section (or the robot-pull pages, if `mvp-documentation`'s restructure has landed); verify the 8.1 and 8.2 tests pass
- [ ] 8.4 [HUMAN] Before 2026-12-07, at the booked robot session, run P3 11.1 and 11.3 together, with robot code running: MB/s over radio and tether, hash time per 100 MB with and without nice, loop overruns during a pull (DS log), the clock-reset active-file check, and plug-in to gold in < 5 min; record the results in `p3-log-acquisition`'s design (or the archived copy if it has archived) and on #74
- [ ] 8.5 If 8.4 passed: set the maturity constant to none (the 8.2 tests then check no label, unchanged), remove the guide label, and drop "Robot pull labelled beta until checked on a robot" from this change's `log-acquisition` delta; verify the tests pass and `openspec validate mvp-install-and-first-run --strict` passes. If it didn't: keep the label. If 8.4 showed loop overruns during a pull, file a follow-up issue to pause pulls while the DS reports enabled; if the hash took > 150 s per 200 MB, file one to scale the timeout with file size

## 9. Cross-OS first-run smoke (#71)

- [ ] 9.1 Write a failing `corpus`-marked test that runs `tools/first-run-smoke.py` against a local wheel and an injected server-start failure, checking each step is named in the output and the server process is gone afterwards ("Server doesn't answer"); add unit tests for `fetch-corpus.py --group` (only that group's files are fetched, an unknown group exits non-zero); write a failing test that parses the workflows: the smoke is not in `ci-ok`'s needs, has `workflow_call`, excludes docs-only paths ("Docs-only change"), and `release.yml`'s `pypi` job needs the smoke called with the build job's wheel ("Smoke failure holds a release", "Release smoke tests the built package"); verify they fail
- [ ] 9.2 Add `fetch-corpus.py --group` and write `tools/first-run-smoke.py` (the five steps from design D-i, including the check that E10 is not `no-aligned-samples` ("Empty Replay fails the smoke"), with named output, timeouts, and temp config, cache and lake); verify the 9.1 script tests pass on macOS locally, and a full run on E10 takes < 15 min with peak RSS < 1 GB
- [ ] 9.3 Add `.github/workflows/first-run-smoke.yml` (nightly, `src/` and packaging PRs, `workflow_call` with a wheel input; ubuntu, macos, windows; `setup-uv` pinned by SHA), left out of `ci-ok`; verify the workflow test's smoke checks pass and one PR run is green on all three OSes
- [ ] 9.4 Call the smoke from `release.yml` with the `build` job's wheel and make `pypi` need it; verify the workflow test passes and a manual dry run (TestPyPI) still runs without the smoke wait. `mvp-public-release` 4.3 then adds only the GitHub Release ordering after `pypi`
- [ ] 9.5 [HUMAN] Watch a week of nightlies (by 2026-12-12). Fix or file each flake; on Windows, fix what's in reach and file the rest for M3; record the pass rate on #71
- [ ] 9.6 Update the developer guide's CI section (the smoke job, when it runs, that it gates releases and not merges); verify `pytest tests/test_docs.py` passes

## 10. Wrap-up

- [ ] 10.1 Update the CLI reference for the `acquire` label and the `init` output; update `docs/rewrite/06-roadmap.md`'s MVP section only if a decision here changed (status goes on #68 and the project board, per `openspec/config.yaml`); verify `pytest tests/test_docs.py` passes
- [ ] 10.2 Run the full suite (`PYENV_VERSION=flashpoint poetry run pytest`, `-m browser`, `-m corpus`), `poetry run ruff check .`, `poetry run mypy`, and `openspec validate mvp-install-and-first-run --strict`; verify all pass
