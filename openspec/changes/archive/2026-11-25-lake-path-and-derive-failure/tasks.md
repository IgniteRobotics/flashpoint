## 1. Tests first (before any source change; the test freeze starts after this group)

- [x] 1.1 In a new file `tests/test_lake_root.py`, add unit tests for `config.lake_root`: a relative `--lake` and a relative `FLASHPOINT_LAKE` are taken from the current folder (`monkeypatch.chdir`); a relative `lake` in a temporary configuration folder's `acquire.toml` is taken from the current folder, not the configuration folder; `~` is expanded from each of the three sources; `../lake` comes back normalised; the default stays `~/flashpoint-lake`. Verify: the relative and `..` cases fail on `development`, and `tests/test_config.py` still passes.
- [x] 1.2 In a new file `tests/test_ingest_derive_failure.py`, add CLI tests using the `FakeRuns` approach from `tests/test_cli.py`, with its own copy of the fixture so `test_cli.py` doesn't grow. Cases: derive exits 1, then 2, after a good ingest, and each time the command exits 1, stderr ends with the `derive failed: Replay data was not built` line naming `flashpoint derive --report`, and no `report` step runs; a run with one quarantined file and a failed derive exits 1 and shows both; `rebuild` with a failed derive exits 1 with the message; `--no-derive` prints no derive message and keeps the ingest's own code; a failed Replay build after a good derive keeps exit 0. Verify: the failure cases fail on `development`, and the last two pass.
- [x] 1.3 In the same file, add a test with a real derive child process: ingest a synthetic wpilog into a lake whose configuration folder holds an invalid robot TOML. Verify: it exits 1, and stderr shows `robot configuration error` before the `derive failed` line. Then fix the TOML, run `flashpoint derive --report`, and check that it exits 0 and that `report/data/matches.js` exists.
- [x] 1.4 Change the `([], 1)` case of `test_report_skipped` in `tests/test_cli.py` to expect exit 1, since a failed derive no longer exits 0. Leave the other two cases as they are. Verify: this case fails on `development`, and the rest of `tests/test_cli.py` passes.
- [x] 1.5 Add a `corpus` test in a new file `tests/test_relative_lake_corpus.py`: symlink corpus group `2026-gacmp-q7` into a temporary folder, `chdir` there, and run `flashpoint ingest --lake lake .` as a child process. Verify: it fails on `development` (exit 0, no `lake/report/data/matches.js`, a traceback on stderr). After group 2 it exits 0, `matches.js` exists, and stderr has no traceback.
- [x] 1.6 Add a test that `flashpoint serve --lake lake` (relative), on a lake with gold, serves the anomalies API without error. Use the helpers in `tests/web/` (`anomaly_lake.py`, `conftest.py`) in a new file. After `chdir`, the server's lake must be built the way `serve` builds it, through `config.lake_root(Path("lake"))`. A test that builds `LakePaths("lake")` directly skips the fix. Verify: it fails on `development` with the `as_uri` error and passes after group 2.

## 2. Absolute lake roots (design D1–D3)

- [x] 2.1 In `src/flashpoint/config.py`, make `lake_root` return `os.path.abspath` of the expanded path for each source. Verify: 1.1, 1.5 and 1.6 pass, and `test_config.py` and `tests/test_cli.py` still pass.
- [x] 2.2 Grep `src/flashpoint` for other code that depends on the current folder (`as_uri()`, `Path.cwd`, `os.getcwd`, bare relative `Path(...)` used as a lake path). Fix only what this change makes wrong, and list what was checked in the PR. Verify: no lake path reaches `as_uri()` without being absolute, and `ruff check .` and `mypy src tests tools` pass.

## 3. A failed derive fails the ingest (design D4)

- [x] 3.1 In `src/flashpoint/cli.py`, make `_derive_and_report` return whether derive succeeded, and print the `derive failed` line to stderr after a non-zero derive. In `main`, return `EXIT_FAILED` when derive failed, else the ingest's own exit code, for both `ingest` and `rebuild`. Verify: 1.2, 1.3 and 1.4 pass, and full `pytest` passes.

## 4. Docs

- [x] 4.1 Update the docs:
  - `docs/guide/support-matrix.md`: remove the #107 known-issue entry.
  - `docs/guide/reference/config/acquire.md`: say a relative `lake` is taken from the folder the command runs in, and recommend an absolute path for the acquire service.
  - `docs/guide/troubleshooting.md`: add the `derive failed: Replay data was not built` message, what it means, and `flashpoint derive --report`.
  - `CHANGELOG.md` under Unreleased / Changed: the new exit code, citing #107.

  Verify: `pytest tests/test_docs.py` (link and CLI-reference drift) and the strict docs build pass.
- [x] 4.2 If any decision here changes the rewrite design, record it in `docs/rewrite/05-target-architecture.md`. Lake resolution belongs with the §4 lake layout. Verify: the link check passes, or the PR says no rewrite doc needed a change.

## 5. Gate and wrap-up

- [x] 5.1 Run the full gate: `ruff check .`, `ruff format --check .`, `mypy src tests tools`, `pytest`, `pytest -m corpus`, and `openspec validate lake-path-and-derive-failure --strict`. Verify: all pass, and `ci-ok` is green on the PR (into `development`, body says `Closes #107`, and lists the edited `test_cli.py` case). Done 2026-11-25: local gate passed, `ci-ok` green on PR #127.
- [x] 5.2 [HUMAN] Josh reproduces the issue's steps on a clean folder: `flashpoint ingest --lake lake .` on a corpus match, then `flashpoint serve --lake lake`. Verify: Replay shows the match, and his OK is recorded on the PR. Done 2026-11-25: run by Claude Code at Josh's direction on a fresh folder (Q7, throwaway config): ingest exit 0, no traceback, 1 match built; serve returned 200 for Replay data and the anomalies API. Josh reviewed the results and directed the merge; recorded on PR #127.
- [x] 5.3 Run `openspec archive lake-path-and-derive-failure` as the PR's last commit. Verify: `openspec validate --all` passes.
