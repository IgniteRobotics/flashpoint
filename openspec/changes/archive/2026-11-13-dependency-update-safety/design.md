## Context

#53 failed on lint, unit, corpus, browser and Windows. The duckdb 2 release removed `relation.fetchone()` and `DuckDBPyConnection.pl`, both used in `src/`. ruff 0.17 stopped flagging two lines we suppress, so RUF100 removes the comments.

## Decisions

- **Cap rule: next major, or next minor for 0.x.** Simple to check mechanically and matches what semver promises. Caps today: duckdb `<2`, polars `<2`, pyarrow `<26` (the newest major we have tested), pydantic `<3`, numba `<0.69`, tomlkit `<0.16`, scikit-learn `<2`. numpy (`<2.5`), paramiko (`<6`) and pyntcore (`<2027`) already have caps. The dev group keeps Poetry caret ranges, which already cap.
- **`versioning-strategy: increase-if-necessary`.** Dependabot only rewrites a range when the new version falls outside it, which is exactly the major-update PR. It stops raising floors on routine bumps. `lockfile-only` was rejected because it would never propose a major at all.
- **Majors excluded from the group with `update-types: [minor, patch]`.** Dependabot then opens one PR per major. Same for actions.
- **ruff 0.17 adopted here** because it's a dev tool with a one-line fix; mypy 2, duckdb 2 and polars 2 get their own issues.
- **0.1.1 release not in scope.** The caps in `pyproject.toml` protect installs from source now; a PyPI release is Josh's call.

## Risks

- Caps can block a needed security fix in a new major. Dependabot still opens the major PR, so it's visible.
