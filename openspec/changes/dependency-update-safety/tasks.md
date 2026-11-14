## 1. Tests

- [x] 1.1 Add a unit test that fails and names any runtime or optional dependency with no upper bound
- [x] 1.2 Add a unit test that fails and names any Dependabot group that includes major updates

## 2. Dependencies

- [ ] 2.1 Cap the runtime and optional dependencies per the design and refresh `poetry.lock`
- [ ] 2.2 Bump ruff to 0.17, drop the suppressions it no longer needs, apply its formatting

## 3. Dependabot

- [ ] 3.1 Group minor and patch only, and set `versioning-strategy: increase-if-necessary` for pip

## 4. Follow-up

- [ ] 4.1 File issues for the duckdb 2 and polars 2 migrations, and for mypy 2
- [ ] 4.2 Full check green locally and in CI
