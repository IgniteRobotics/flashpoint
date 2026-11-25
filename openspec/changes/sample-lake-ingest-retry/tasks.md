## 1. Tests first (before any change to `tools/docs-media.py`)

- [x] 1.1 Create `tests/tools/test_docs_media_retry.py`. Load the tool as `test_docs_media_samples.py` does. Add a scripted fake for the module's `subprocess.run`: it returns one `CompletedProcess` per call from a queue of (exit code, stdout) and records each call's args. Add a helper that renders summary stdout from failed/quarantined names. Verify: the module imports and pytest collects it.
- [x] 1.2 Test "fails once, then imports": run 1 reports `X.hoot` FAILED (exit 1), run 2 is clean (exit 0), then report exits 0. Expect: `build_sample_lake` returns the lake, ingest ran twice with the same file list, and the captured output names `X.hoot` and says it retried. Verify: it fails against today's code.
- [x] 1.3 Test "keeps failing": all 3 ingest runs report `X.hoot` FAILED. Expect: `SystemExit` after exactly 3 ingest calls, with a message naming `X.hoot` and 3 runs, and `report` never called. Verify: it fails against today's code (today raises after 1 call without the file name).
- [x] 1.4 Test "quarantined isn't retried": `ok=(0,)`, run 1 quarantines `Y.hoot` (exit 1). Expect: exactly 1 ingest call, and `SystemExit` naming `Y.hoot`. Add a test for the quarantine lake: `ok=(0, 1)`, run 1 quarantines `Y.hoot`. Expect: 1 ingest call, then report runs, and the lake is returned.
- [x] 1.5 Test "retry quarantines it": run 1 FAILED `X.hoot`, run 2 QUARANTINED `X.hoot` (exit 1), `ok=(0,)`. Expect: 2 ingest calls, and `SystemExit` naming `X.hoot` as quarantined.
- [x] 1.6 Test "non-file failure": exit 1 with a summary of 0 failed and 0 quarantined. Also test exit 1 with no summary line at all. Expect for each: 1 ingest call, `SystemExit`, and the run's output written to stderr.
- [x] 1.7 Drift test: build an `IngestReport` with one `failed`, one `quarantined` and one `quarantined-earlier` `FileResult`. Capture `flashpoint.cli._summarize` stdout, followed by an unrelated derive line. Assert that the tool's summary parser returns failed=[that name], quarantined=[that name], and the right counts. Verify: it fails now because the parser doesn't exist.
- [x] 1.8 Run `PYENV_VERSION=flashpoint poetry run pytest tests/tools/test_docs_media_retry.py`. Every new test fails for the expected reason (missing behavior or parser), not a test bug. Then run the rest of `tests/tools/` and confirm it still passes. From here on, `tests/` is frozen.

## 2. Implementation (`tools/docs-media.py`)

- [x] 2.1 Add the summary parser: the last count line (`… ingested, … skipped, … failed, … quarantined, … quarantined earlier …`) plus the `FAILED` and `QUARANTINED` line names. It returns `None` when no count line is found. Verify: test 1.7 passes.
- [x] 2.2 Add `INGEST_RUNS = 3`. Rewrite the ingest step of `build_sample_lake` to apply design decision 1's rules in order: summary not found, then quarantined, then failed with runs left, then failed on the last run, then everything else. Print `ingest: retrying <names> (run <n> of 3)` before a retry, and a closing line naming the retried files when one succeeds. Keep the `report` step and its `ok` check as they are. Verify: tests 1.2–1.6 pass.
- [x] 2.3 Run `poetry run ruff check .` and `poetry run mypy`, then `PYENV_VERSION=flashpoint poetry run pytest`. Verify: all clean and green.

## 3. Integration and close-out

- [x] 3.1 Run `poetry run python tools/docs-media.py capture --out build/docs-media` locally with a fetched corpus-v1. Verify: it completes and the quarantine lake still builds. If no owlet flake happens, the output shows no retry lines, which is expected.
- [x] 3.2 Check `docs/rewrite/05-target-architecture.md` (docs tooling, line ~273). Verify: no D1–D11 decision or design text changes, and the docs tooling line is still accurate. No edit is expected, and no ADR.
- [ ] 3.3 Open the PR from `feature/123-sample-lake-ingest-retry` into `development` with `Closes #123` and the AI disclosure. Verify: `ci-ok` is green, and the `docs` workflow's Media step passes on the PR or after merge.
- [ ] 3.4 Archive (`openspec archive sample-lake-ingest-retry`) as the PR's last commit. Verify: the `project-documentation` main spec gains "Sample lake rides out a failed file".
