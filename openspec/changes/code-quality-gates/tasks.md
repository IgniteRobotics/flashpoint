## 1. Self-tests first (red), and every tests/ edit before the freeze

- [x] 1.0 Clean `tests/` against the new rules (with the tests/ per-file ignores from design.md §3) and give every existing `noqa` (`# noqa: CODE - reason`) and `type: ignore` (`# type: ignore[code]  # reason`) in `tests/` a reason; verify `ruff check tests` with the new rules passed on the command line is clean and the unit run passes. After this group, `tests/` is frozen
- [x] 1.1 Add `tests/fixtures/quality/python/` with one minimal violation per check-enforced Python rule (`S602`, `S603`, `S608`, `BLE001`, `T201`, `DTZ005`, `C901`, `ARG001`, `PT011`, `PGH004`, `RUF100`, `TID251` for `sys.exit` and `os.system`), and exclude the tree from ruff; verify `ruff check .` ignores it
- [x] 1.2 Add `tests/fixtures/quality/js/` with one violation per check-enforced JS rule (undeclared global, unused variable, loose equality, unformatted file, `import` statement, `innerHTML`); verify the existing static-lint test doesn't scan it
- [x] 1.3 Write `tests/test_quality.py`: each Python fixture fails ruff with its expected code; each JS fixture fails Biome (through pre-commit) with its expected rule; verify it fails now, because the rules aren't enabled
- [x] 1.4 Add tests for suppression format (`# noqa: CODE - reason`, no suppressed `S602`/`S604`/`S605`), workflow `permissions:` blocks, and SHA-pinned actions; verify each fails on today's tree, naming the offending files
- [x] 1.5 Add the classic-script check (no `import`/`export`) to `tests/web/test_static_lint.py`; verify it passes on the app and fails on the JS fixture

## 2. Python rules

- [x] 2.1 Add `pre-commit`, `pytest-cov` to the dev group; extend ruff `select`, per-file ignores, `mccabe.max-complexity = 12`, `TID251` banned-api; add mypy `ignore-without-code`; verify the Python fixtures in 1.3 now pass
- [x] 2.2 Fix or suppress (with reasons) the `S`, `BLE`, `DTZ`, `ARG`, `PERF`, `PT`, `RUF` hits in `src/`; give the existing bare `noqa` comments reasons; verify `ruff check .` is clean and the suppression test passes
- [x] 2.3 Same for `tools/` (tests/ was done in 1.0); mark the 9 functions over complexity 12 `# noqa: C901 - grandfathered (code-quality-gates)`; verify `ruff check .`, `mypy src tests tools`, and the unit run pass

## 3. Frontend rules

- [x] 3.1 Add `biome.json` (includes, vendor/mode.js/index.html excluded, globals, `noRedundantUseStrict` and `useTemplate` off) and `.pre-commit-config.yaml` (local ruff hooks, `biome-check` pinned to 2.6.7, hygiene hooks); verify the JS fixtures in 1.3 now pass
- [x] 3.2 Run the Biome formatter only, commit it alone, add its SHA to `.git-blame-ignore-revs`; verify `pytest -m browser` passes on that commit
- [x] 3.3 Fix the remaining Biome errors and warnings in `shell.js` and `views/replay.js`; verify `biome ci --error-on-warnings` is clean for those files and the browser tests pass
- [x] 3.4 Same for `anomalies.js`, `history.js`, `live-can.js`, `setup.js`, `shell.css`; verify `pre-commit run --all-files` passes with no changes and the browser tests pass

## 4. Coverage

- [x] 4.1 Add `[tool.coverage]` (source, branch, `fail_under = 93`); the coverage leg sets `NUMBA_DISABLE_JIT=1`; and the per-package report commands for `readers/`, `lake/`, `semantics/physics.py` at 85%; verify they pass locally and fail when the floor is set 1 point above the measured value

## 5. CI

- [ ] 5.1 In `ci.yml`: top-level `permissions: contents: read`, actions pinned by SHA, lint steps replaced by `pre-commit run --all-files --show-diff-on-failure` on Linux and Windows with a cached `~/.cache/pre-commit`, `--cov` and the floor steps on the py3.12 leg; verify the workflow tests in 1.4 pass
- [ ] 5.2 Add `browser` and `clean-install` to `ci-ok`'s `needs`; verify on the PR that a forced browser failure (temporary commit, reverted) turns `ci-ok` red
- [ ] 5.3 Pin and add permissions to `release.yml` and `openspec-drift.yml` (the drift workflow is mentor-owned: get a mentor's review); verify the workflow tests pass for all files
- [ ] 5.4 Add `.github/dependabot.yml` (pip + github-actions, `development`, monthly, grouped) and `.github/workflows/codeql.yml` (python + javascript-typescript, vendor ignored); verify both run on the PR
- [ ] 5.5 [HUMAN] After merge, add a code-scanning rule (CodeQL, high or higher) to the `dev-main-pr-gate` ruleset; verify a PR view shows it as required

## 6. Docs

- [x] 6.1 Add the coding standards section to `docs/guide/developer-guide.md` (each rule with its reason and enforcer, as in `specs/coding-standards`), link to the ignition-labs Flashpoint section, document `pre-commit run --all-files`, `blame.ignoreRevsFile`, the coverage floors, and the updated CI table; verify `pytest tests/test_docs.py` passes
- [x] 6.2 Update `docs/rewrite/06-roadmap.md` §5: coverage scope is `readers/`, `lake/`, `semantics/physics.py`; name the floors and the new gates; verify the docs test passes
- [ ] 6.3 [HUMAN] Ratify and merge the ignition-labs PR adding the Flashpoint section to the coding standards; then make the developer guide's text match it
- [ ] 6.4 Full check: `pre-commit run --all-files`, `mypy src tests tools`, `pytest`, `pytest -m browser`, and `pytest -m corpus` all pass; CI is green with the new `ci-ok`
