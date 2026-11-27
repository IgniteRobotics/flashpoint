## Why

`dependency-update-safety` (#55) capped `duckdb<2` and `polars<2` after the grouped Dependabot PR broke the build. Both caps need to come off before the MVP release (#68). Otherwise Flashpoint ships pinned to two majors that will stop getting fixes, and every Dependabot major PR for them goes red. Closes #56 and #57.

A trial run on 2026-11-26 (unit suite plus mypy, duckdb 2.0.1 and polars 2.2.0 in a scratch venv) showed:

- **polars 2.2.0**: no failures and no new mypy errors. The issue still requires the corpus, browser and perf suites before the cap is raised.
- **duckdb 2.0.1**: about 300 failing or erroring unit tests and 42 mypy errors, from three breaks:
  1. `DuckDBPyConnection.pl()` is gone, so `con.execute(...).pl()` fails (4 sites: anomaly/frame, report/markers ×2, semantics/usage). Calls to `con.sql(...).pl()` (derive, gold) still work.
  2. `DuckDBPyRelation.fetchone()` is gone (2 sites in src/, about 20 in tests/).
  3. A SELECT from `con.execute()` that is not fully read now blocks `con.sql()` and `con.cursor()` on that connection with `ResourceInUseException`. `fetchone()` leaves the result open (3 sites: anomaly/power, semantics/can_health, report/build). `fetchall()`, `fetchnumpy()`, and statements with no result set (SET, COPY, CREATE) are fine. 1.x allowed this. The issue doesn't mention it.

## What Changes

- Move every DuckDB read in `src/` onto calls that work on duckdb 2. Polars frames come from relations (`con.sql(q, params=...).pl()`). Single rows come from `fetchall()`. No SELECT result from `execute()` is left partly read.
- Move the test-side call sites in `tests/` the same way, and narrow `duckdb.sql()`'s now-optional return type for mypy. This edits tests only to keep them running on the new API, with no assertion changes, and happens **before implementation starts** (see design D4).
- Add a guard test: in modules that import duckdb, it fails when `.fetchone()`, `.fetchmany()`, `.pl()` or an Arrow fetch is chained directly onto `.execute(...)`.
- Raise the caps to `duckdb (>=2.0,<3)` and `polars (>=2.0,<3)` and refresh `poetry.lock`.
- Drop the `# type: ignore` comments that become unused.
- **BREAKING** (packaging only): installs now require duckdb 2 and polars 2. Pip and pipx resolve these automatically. Flashpoint doesn't support any environment that has to keep the 1.x versions.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. This is a dependency migration with no behavior change. Every existing requirement, including the CI supply-chain rule that caps every runtime dependency below its next major, holds as written. The change sets `skip_specs: true`.

## Impact

- **Code**: `src/flashpoint/{anomaly/frame,anomaly/power,semantics/{can_health,usage,signals,silver},report/{markers,build}}.py` (9 call sites). All other DuckDB call sites already read results with `fetchall()` or `fetchnumpy()`, or come through relations.
- **Tests**: about 12 files under `tests/` with mechanical API moves, plus one new guard test.
- **Dependencies**: `pyproject.toml` floors and caps, `poetry.lock` (polars now ships as `polars` + `polars-runtime-32`).
- **CI**: no workflow changes. The corpus, browser and perf jobs must pass on the new lock.
- **Roadmap / pitfalls**: maintenance under the MVP release (#68), outside phases P0–P6. It fixes no numbered pitfall from `03-pitfalls-and-performance.md`.

## Non-goals

- Using new duckdb 2 or polars 2 features, or tuning for them. Perf budgets stay as they are.
- mypy 2 (#58), the Windows timing flake (#60), and any 0.1.x re-release that caps the published 0.1.0 (Josh's call, tracked separately).
- Supporting duckdb 1.x or polars 1.x at the same time as 2.x.
- Changing the Dependabot config or the cap-checking tests from #55.
