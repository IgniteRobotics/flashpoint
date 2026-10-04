## 1. Branch and CI skeleton

- [x] 1.1 Create branch `feature/p0-stabilize-foundation` from `main`
- [x] 1.2 Add a Poetry project (`pyproject.toml`, Python `>=3.11,<3.14`, src layout), `src/flashpoint/__init__.py` with `__version__`, and dev dependencies (pytest, ruff, mypy). Configure pytest `--strict-markers` and register the `corpus` marker
- [x] 1.3 Write a failing smoke test `tests/test_package.py` (imports the package, checks the version), then make it pass
- [ ] 1.4 Add `.github/workflows/ci.yml`. A `lint-test` job (3.11, 3.12) runs ruff check, ruff format --check, mypy --strict, and pytest -m "not corpus". A `corpus` job fetches the corpus (cached) and runs pytest -m corpus
- [x] 1.5 Run lint, type check, and tests locally; all green

## 2. Tags

- [x] 2.1 Create the annotated tag `legacy-2025` at `70be731` and push it
- [x] 2.2 Create annotated tags `archive/development` and `archive/power-tracking` at the current branch tips and push them (the branches stay)

## 3. ADRs

- [x] 3.1 Add `docs/adr/README.md` (index, status legend, template)
- [x] 3.2 Write ADR-0001 to ADR-0010 for D1–D10 with status **Proposed**. Include the P0 findings (Pro licensing, owlet compliancy) in ADR-0004 and ADR-0005
- [x] 3.3 Write ADR-0011 (device identity) with status **Accepted**

## 4. Golden corpus

- [x] 4.1 Assemble the corpus files from the Drive and the 2025 downloads, and derive the truncated hoot
- [x] 4.2 Write `tests/corpus/manifest.toml` with id, file, sha256, size, kind, season, and what each one covers
- [x] 4.3 Write `tools/fetch-corpus.py` (stdlib only). It downloads the release assets into the cache dir, verifies sha256 and size, and is idempotent
- [x] 4.4 Write a failing `tests/test_corpus.py` (`@pytest.mark.corpus`) that checks every manifest entry is present with the right hash and that wpilog headers start with `WPILOG`. Make it pass
- [x] 4.5 Create the GitHub release `corpus-v1` and upload the assets
- [x] 4.6 Run the fetch from a clean cache and confirm the corpus tests pass

## 5. Pro and compliancy findings

- [x] 5.1 Run `owlet --check-pro` and `--compliancy` on every corpus hoot. Record the results in the manifest (`pro`, `compliancy`)
- [x] 5.2 Update docs/rewrite to retire the temperature risk and record compliancy-based owlet selection (README TL;DR, 03 §6, 04 §1, 05 D5, 06 §5)

## 6. Robot-side CAN inventory logger

- [x] 6.1 Robot-2026: on a new branch `feature/can-inventory-logger` (using a worktree, so the existing WIP is left alone), add `CanInventoryLogger` and a unit test for the normalizer, start it from `Robot`, run `./gradlew build`, and open a PR
- [x] 6.2 Phoenix-2026: the same, on its own `feature/can-inventory-logger` branch, then open a PR
- [ ] 6.3 [HUMAN] On a live Phoenix 6 robot, open `http://<rio>:1250/?action=getdevices`, confirm the field names, and adjust the normalizer if needed
- [ ] 6.4 [HUMAN] Deploy, and confirm `/Flashpoint/CANInventory` appears with real serial numbers in a fresh wpilog from each robot

## 7. Unit registry seed

- [x] 7.1 Add `config/units/units-seed.csv` (headers only) and `config/units/README.md` describing the Tuner X walk and the labelling convention
- [ ] 7.2 [HUMAN] Walk every robot and the spare shelf with Tuner X, fill in the CSV, and physically label each motor

## 8. Land it

- [x] 8.1 Do `retire-legacy-code` stage 1 on this branch (tracked in that change's tasks)
- [ ] 8.2 Open the PR, and confirm CI is green
- [ ] 8.3 Turn on branch protection for `main`: require a PR, 1 review, and the CI checks; `enforce_admins` off
- [x] 8.4 Mark P0 as in progress in `docs/rewrite/06-roadmap.md`. List the open [HUMAN] tasks in the PR body
