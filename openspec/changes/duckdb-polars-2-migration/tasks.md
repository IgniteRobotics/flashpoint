## 1. Tests (before the freeze; design D3, D4)

- [x] 1.1 Add the guard test from design D3, using the existing quality self-test style. It checks `src/` modules that import duckdb and fails, naming file and line, on `.fetchone()`, `.fetchmany()` or `.pl()` chained onto `.execute(...)`, or `.fetchone()` chained onto `.sql(...)`. Verify: it fails today and names exactly frame.py:78, power.py:102, build.py:215, markers.py:102, markers.py:156, can_health.py:160, usage.py:164, signals.py:124 and silver.py:143, and it leaves out `lake/ledger.py`
- [x] 1.2 Move the test-side `rel.fetchone()` calls to `fetchall()` with unchanged assertions (`== (n,)` becomes `== [(n,)]`) in test_ingest, test_ingest_corpus, lake/test_lake, semantics/{test_signals, test_can_health_builder, test_derive_corpus}, report/test_build_corpus, and every other test using duckdb that `grep -rn 'fetchone' tests/` turns up (SQLite ledger and backup tests stay as they are). Verify: `poetry run pytest` and `poetry run pytest -m corpus` pass on the current lock (duckdb 1.5.6)
- [x] 1.3 Narrow `duckdb.sql()`'s optional return with `assert rel is not None` where mypy flags it on duckdb 2 (test_derive_corpus, test_ingest_non_pro, test_temperature, test_silver_device_enable_corpus, test_build_corpus, lake/test_lake). Verify: `poetry run mypy src tests tools` is clean on the current lock
- [x] 1.4 Commit group 1 on its own. The test freeze starts here, and nothing under `tests/` changes after this commit

## 2. Code (design D1)

- [x] 2.1 Move the four `execute(...).pl()` reads (anomaly/frame, report/markers ×2, semantics/usage) to `con.sql(q, params=...).pl()`, fetching once per relation. Verify: the guard no longer names them, and tests/anomaly/test_frame, tests/report/test_markers and tests/semantics/test_usage pass
- [x] 2.2 Move the three `execute(...).fetchone()` reads (anomaly/power, semantics/can_health, report/build) to relation `fetchall()`, keeping each missing-row branch, and the two `sql(...).fetchone()` counts (semantics/signals, silver) to `fetchall()`. Drop the `# type: ignore` comments that become unused. Verify: the guard test passes, tests/anomaly/test_power, tests/semantics/test_can_health_* and tests/report/test_build pass
- [x] 2.3 Full check on the current lock: `poetry run pytest`, `poetry run pytest -m corpus`, `poetry run ruff check .`, `poetry run mypy src tests tools` all green. This shows the code works on duckdb 1.5.6 before the bump

## 3. Dependencies (design D2)

- [x] 3.1 Set `duckdb (>=2.0,<3)` and `polars (>=2.0,<3)` in `pyproject.toml` and run `poetry lock`. Verify: the lock shows duckdb 2.0.x and polars + polars-runtime-32 2.2.x, and the cap test from #55 passes
- [x] 3.2 Full local check on the new lock: `poetry run pytest`, `-m corpus`, `-m browser`, `-m perf`, ruff and mypy. Verify: everything is green except known non-blocking perf overruns, which go in the PR with their numbers next to the design's baseline band

## 4. Docs

- [x] 4.1 Add a CHANGELOG `Unreleased / Changed` entry saying Flashpoint now requires duckdb 2 and polars 2 (#56, #57). Verify: the docs drift tests pass
- [x] 4.2 Run the DuckDB snippets in `docs/guide/under-the-hood/how-it-works.md` against a lake on the new lock and fix any that break. Confirm `docs/rewrite/` needs no edit (D1/D2 unchanged, no version pins). Verify: the snippets print rows, and the PR notes the docs/rewrite check

## 5. Ship

- [ ] 5.1 Open the PR from `feature/56-duckdb-polars-2-migration` into `development` with `Closes #56` and `Closes #57` and the AI disclosure. Verify: `ci-ok`, corpus and browser are green
- [ ] 5.2 [HUMAN] Pit check: on the new lock, `flashpoint ingest` a fresh log, then `flashpoint serve` against the real lake, and click through Replay, History, Anomalies and Setup with no server errors in the console. Verify: tick here and on the [HUMAN] tracker issue
- [ ] 5.3 `openspec archive duckdb-polars-2-migration` as the PR's last commit. Verify: `openspec validate` passes and the change directory is under `openspec/changes/archive/`
