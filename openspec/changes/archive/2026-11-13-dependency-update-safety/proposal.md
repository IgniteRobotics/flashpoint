## Why

Dependabot's first grouped Python PR (#53) bundled four major updates (duckdb 2, polars 2, ruff 0.17, mypy 2) with routine bumps, so one breaking package failed the whole group. It also raised our lower bounds. Separately, the runtime dependencies have no upper bounds, so a fresh install of 0.1.0 can already resolve duckdb 2, which removed APIs Flashpoint calls (#55).

## What Changes

- Every runtime and optional dependency gets an upper bound: the next major, or the next minor for a `0.x` package. duckdb and polars are capped below 2 until their migrations land.
- Dependabot groups only minor and patch updates; each major update arrives as its own PR. It changes `pyproject.toml` only when a new version falls outside the declared range, so it stops raising floors.
- Adopt ruff 0.17 and drop the two suppressions it no longer needs.
- Follow-up issues for the duckdb 2 and polars 2 migrations.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `ci-quality-gates`: the CI supply chain requirement changes how dependency updates are grouped, and adds bounded runtime dependencies.

## Impact

`pyproject.toml`, `poetry.lock`, `.github/dependabot.yml`, two suppression comments in `src/` and `tools/`, `tests/test_quality.py`. A 0.1.1 release with the caps is a separate decision.
