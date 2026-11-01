## 1. Classify error reasons

- [x] 1.1 Write failing tests for a reason-kind table in `readers/` (design §2): each file reason (`empty-file`, `invalid-header`, `too-old`, `unsupported-compliancy:19`, `unsupported-version`, `no-signals-for-profile`) maps to `file`; the four owlet environment reasons map to `environment`; `owlet-failed`, `owlet-timeout`, `internal-error:KeyError` and an unknown reason map to `suspect`; and a source scan finds every literal reason raised as `HootError`/`WpilogError` in `src/flashpoint/readers/` in the table. Then add the table and lookup. Verify the tests pass

## 2. Ledger: `failed` stage, attempts, migration

- [x] 2.1 Write failing tests in `tests/lake/test_lake.py`: `Stage.FAILED` exists; a `failed` write stores reason and increments `attempts`; `success` resets `attempts` to 0; `needs_processing` is True for `failed` and False for `quarantined` and `success` at the current version; `files.parquet` export carries `attempts`. Then implement (design §1, §3). Verify the tests pass
- [x] 2.2 Write failing tests for opening an old lake: a SQLite file created with the current `files` schema minus `attempts`, holding `quarantined` rows with `owlet-download-failed`, `owlet-failed`, `internal-error:OSError` and `invalid-header`, opens with `attempts` added, the first three at `failed`/0, and `invalid-header` still `quarantined`; opening it twice is a no-op. Then implement the on-open migration. Verify the tests pass

## 3. Ingest: route errors, statuses, rebuild

- [x] 3.1 Write failing tests in `tests/test_ingest.py` with a fake owlet registry that raises a chosen `HootError`: `owlet-download-failed` leaves the hoot `failed` and a following run with a working registry ends it at `success`; 5 runs of `owlet-download-failed` never quarantine; `owlet-failed` is `failed` after runs 1–2 and `quarantined` after run 3; `empty-file` quarantines on run 1; an unexpected exception is `failed` with `internal-error:<type>`; `failed` files leave no bronze or metadata behind (same cleanup as quarantine). Then change `_finish`/`_quarantine` (design §2). Verify the tests pass
- [x] 3.2 Write failing tests for `FileResult` statuses and exit code (design §5): a second run over corpus `2026-gadal-q11-empty` + `2026-gacmp-q7` reports Q7 `skipped`, Q11 `quarantined-earlier` with reason `empty-file`, exit 0; a run with a `failed` file exits 1. Then implement. Verify the tests pass, and the existing "Re-ingest is a no-op" corpus test still passes
- [x] 3.3 Write failing tests for `Ingestor.rebuild`: plain rebuild reprocesses `failed` files from raw as well as older-version files; quarantined mode reprocesses only `quarantined` files, resets `attempts` first, re-quarantines `empty-file`, and reports a file with no raw copy without changing it. Then implement (design §4). Verify the tests pass

## 4. CLI

- [x] 4.1 Write failing tests in `tests/test_cli.py` for output: `QUARANTINED`, `FAILED … will retry` and `quarantined earlier (<reason>)` lines; the summary counts ingested, skipped, failed, quarantined, quarantined earlier; the `flashpoint rebuild --quarantined` hint appears only when something was quarantined earlier. Add `rebuild --quarantined`. Then update `_summarize` and the parser. Verify the tests pass
- [x] 4.2 Write failing tests for the report step (design §6) with the subprocess command stubbed: it runs after a derive that exits 0; it doesn't run with `--no-report`, with `--no-derive`, when nothing succeeded, or when derive exits non-zero; exit 3 prints the "already running, run `flashpoint report` after it" line; exit 1 prints the build's output tail; the command's exit code is the ingest's in every case; `rebuild` runs it after `derive --all`. Then implement. Verify the tests pass
- [x] 4.3 #15 regression (corpus, real subprocesses): ingest only the hoots of a corpus match, run `flashpoint report` and check the match's Replay data is `no-aligned-samples`, then `flashpoint ingest` the wpilog with no other command, and check the match's data now has aligned device data. If it still has no aligned samples, stop: record the finding in this design.md and open a new issue for the alignment cause instead of shipping 4.2 as the fix for #15. Verify the test passes

## 5. Acquire

- [x] 5.1 Add tests in `tests/acquire/test_cycle.py`: an inbox hoot that ingest leaves `failed` is `KEPT` and is ingested again by the next cycle; the cycle is not marked as an ingest crash (the ledger changed); a hoot quarantined on its 3rd failure is cleared as `QUARANTINED`. The acquire ingest command still passes `--no-derive`, so the cycle runs one Replay build. Verify the tests pass with no change to `cycle.py`, or make the smallest change that does

## 6. Docs and gate

- [x] 6.1 Update `docs/guide/cli-reference.md` (`ingest`: output lines, exit code, Replay build and `--no-report`; `rebuild`: `failed` files and `--quarantined`) and `docs/guide/user-guide.md` (quarantined means the file is bad; failed means try again; how to retry). Add the downgrade note (design, Migration Plan) to the release notes or changelog the repo uses. Verify the docs link/drift tests pass
- [ ] 6.2 Run the full gate locally (`PYENV_VERSION=flashpoint poetry run` ruff, ruff format, mypy, pytest, `-m corpus`, `-m browser`), open a PR from `feature/ingest-retry-and-refresh` into `main` with `Closes #14` and `Closes #15`, and verify CI is green on the blocking jobs
- [ ] 6.3 [HUMAN] On the machine that hit GAGAI1 Q4 (or with networking off): ingest a new hoot with owlet not cached, check it reports `failed (owlet-download-failed), will retry`, reconnect, ingest again, and check it reaches `success` and the match opens in Replay with no `flashpoint report`. Record the result in design.md and in #13
