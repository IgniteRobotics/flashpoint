## Context

See proposal.md (Why) for the breakage inventory. ADR-0001 (D1, Parquet lake + DuckDB) and ADR-0002 (D2, Polars + DuckDB SQL) stay as they are: this change keeps the same engines and moves to their next major. Nothing here departs from D1–D11, so it needs no new ADR.

These are the duckdb 2.0.1 behaviors the trial run confirmed (scratch venv, 2026-11-26):

| Call | 1.5.6 | 2.0.1 |
|---|---|---|
| `con.sql(q, params=[...]).pl()` | ok | ok |
| `con.execute(q, p).pl()` | ok | **AttributeError** |
| `rel.fetchone()` | ok | **AttributeError** ("use `rel.limit(1).fetchall()`") |
| `rel.fetchall()`, `rel.limit(1).fetchall()` | ok | ok |
| `con.execute(SELECT).fetchone()` then `con.sql(...)` / `con.cursor()` | ok | **ResourceInUseException** |
| `con.execute(SELECT).fetchall()` / `.fetchnumpy()`, then anything | ok | ok |
| `con.execute(SET / COPY / CREATE / INSERT)`, then anything | ok | ok |
| `duckdb.sql(...)` return type in stubs | `DuckDBPyRelation` | `DuckDBPyRelation \| None` |

Under polars 2.2.0 the unit suite and mypy are clean. The ingest, derive and report paths that handle full logs use polars heavily, so the corpus and perf suites are still the real test.

## Goals / Non-Goals

**Goals:**
- Every DuckDB read in `src/` and `tests/` works on duckdb 2.
- A guard test catches the third break (the unfinished result). It only fires when the next call lands on the same connection, so test coverage alone can miss it.

**Non-Goals:**
- A query-helper layer over DuckDB. The fixes are local and use one or two idioms, and nine call sites don't justify an abstraction.
- Code that runs on both 1.x and 2.x. The idioms below happen to work on both, but nothing tests 1.x, and the floors say so.

## Decisions

**D1. Reads go through relations. `execute()` is for statements and fully drained cursors.**
Polars frames come from `con.sql(q, params=...).pl()`, which derive.py and gold.py already use. Single-row aggregates use `con.sql(q, params=...).fetchall()` and unpack the one row (`(low,) = ...` or `rows[0]`). A row that may be missing uses `.limit(1).fetchall()` with an explicit empty-list branch. The existing `cursor = con.execute(...)` + `description` + `fetchall()` pattern (report/build, views/queries) stays, because it drains the result.
*Alternatives:* `execute(...).fetchone()` followed by `con.complete()` is correct but easy to forget. `execute(...).to_arrow_table()` + `pl.from_arrow` works but warns about an extra copy. A per-read `con.cursor()` adds a connection object for every query.

**D2. Floors move to the new major: `duckdb (>=2.0,<3)`, `polars (>=2.0,<3)`.**
CI tests only the lock (duckdb 2.0.x, polars 2.2.x). A floor of `>=1.3` would claim support for 1.x with no test behind it. Installs are isolated (ADR-0018 uv tool / pipx), so a higher floor costs users nothing. The caps keep #55's rule: below the next major.
*Alternative:* keep the 1.x floors and add a CI matrix leg on 1.x. That doubles CI for a compatibility promise nobody asked for.

**D3. Guard test, scoped to duckdb modules.**
A new unit test parses every `src/` module that imports `duckdb`. It fails, naming the file and line, on `.fetchone()`, `.fetchmany()` or `.pl()` chained directly onto `.execute(...)`, and on `.fetchone()` chained onto `.sql(...)`. The duckdb-import scope keeps it away from the SQLite ledger (`lake/ledger.py`, where `execute().fetchone()` is correct). It's best-effort: it can't follow a result stored in a variable. Corpus and browser suites cover those paths.
*Alternative:* a custom ruff rule. Ruff has no plugin API, and the test sits next to the other quality self-tests.

**D4. Test edits happen before the freeze, as their own task group.**
About 20 test call sites use `rel.fetchone()`, and the corpus tests trip mypy on the optional `duckdb.sql()` return. These are API moves only, with unchanged assertions: `.fetchone() == (n,)` becomes `.fetchall() == [(n,)]`, and `duckdb.sql()` gets an `assert rel is not None`. They land with the new guard test in group 1, before any `src/` edit. The freeze begins once group 1 is done. On the current lock (duckdb 1.5.6) the migrated tests still pass, which shows they're behavior-neutral.

## Budgets

Nothing changes in time or memory. The memory and thread settings stay (`DUCKDB_MEMORY_LIMIT` 512 MB in derive and 256 MB in report, 2 threads). Relation `.pl()` materializes the same frame `execute().pl()` did. The perf suite's existing budgets are the acceptance bar (one qual match: ingest < 30 s / < 1 GB, signals ≤ 2 s / 150 MB, detect and derive budgets as set). Known non-blocking overruns from earlier runs (ingest about 34–40 s, detect about 5.3 s) are the baseline. Report a regression beyond that band in the PR. Don't fix it here.

## Risks / Trade-offs

- [A relation re-runs its query on every fetch] → each D1 site fetches once. Don't call `.pl()` or `.fetchall()` twice on the same relation.
- [An unfinished result hidden behind a variable gets past the guard] → after the lock bump, run the corpus, browser and perf suites locally and in CI. Before merge, run `flashpoint serve` on a real lake for a Replay + History pass, because the web API reuses one long-lived connection.
- [polars 2 changes numerics or dtypes in ways the unit suite doesn't catch] → the corpus suites (ingest, derive, usage, can-health, power, detect, report build, web API) check values on the golden log corpus. A failure there stops the change and goes to a mentor. Expected values don't get edited to make a test pass.
- [A Dependabot minor bump inside 2.x brings another break] → same exposure as today. The caps stop the next major only.

## Migration Plan

One PR into `development` from `feature/56-duckdb-polars-2-migration`, closing #56 and #57. To roll back, revert the PR: the lock and the caps go back together. Lake data and on-disk formats are untouched (checked 2026-11-26: zstd Parquet written by 2.0.1 reads in 1.5.6 and polars, and the other way round), so a rollback leaves nothing to migrate.
