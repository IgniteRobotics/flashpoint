## Context

See proposal.md for why. Measured on `development` at 8c0a35a (2026-11-11):

| Check | Today |
|---|---|
| Ruff | 9 rule families (`E F W I B UP N SIM PTH`). Adding the families below gives about 160 hits in `src/` + `tools/`, most of them CLI `print` calls and asserts already marked `noqa` |
| Stale `noqa` in `src/` | 59 (written for `S`/`BLE` rules that aren't enabled) |
| Complexity > 12 | 9 functions |
| Coverage (unit run, branch) | Not measured in CI. Locally: 94% total with Numba JIT on, where `readers/` shows 73% because coverage can't trace `@njit` kernels. With `NUMBA_DISABLE_JIT=1`: **95% total, `readers/` 94%, `lake/` 98%, `semantics/physics.py` 100%** |
| Frontend | 3.4k lines of classic-script JS + 850 lines of CSS, no linter. Biome 2.6 recommended set: 13 errors, 63 warnings. Formatting rewrites ~3.7k JS lines and expands the one-line CSS rules (~2.3k lines) |
| `ci-ok` waits on | `changes`, `docs`, `lint-test`, `corpus` (not `browser`, `clean-install`, `windows`) |

No D1–D11 decision changes. D6 (a static Replay export that opens from `file://`) is why the app uses classic scripts and no build step, and this design keeps that.

## Goals / Non-Goals

**Goals:** every check-enforced rule in `specs/coding-standards` is blocking on day one; local and CI run identical tool versions; the gates test themselves.

**Non-Goals:** JS type-checking (follow-up: `tsc --checkJs` with a `globals.d.ts`); fixing grandfathered complexity; changing what any page does.

## Decisions

### 1. Biome for the frontend, not ESLint + Airbnb
One pinned binary lints and formats both JS and CSS, with no `node_modules` tree in a Python repo. The Airbnb config assumes ES modules and a bundler and has no maintained flat-config release, so most of it would be switched off for IIFE scripts. *Alternative:* ESLint flat config + Prettier + stylelint, which means three tools and three configs for 4k lines.

`biome.json`: `files.includes` covers `src/flashpoint/web/static/**/*.{js,css}` minus `vendor/**`, `mode.js` (generated), and `index.html` (Biome's HTML support is still experimental). `.ignite/` is mentor tooling with its own tests and is excluded. Formatter: 2-space indent, width 100 (matches ruff), single quotes (the existing style). Linter: `recommended`, plus `correctness/noUndeclaredVariables` with `javascript.globals = ["FP", "FP_MODE", "uPlot"]`. Biome parses `.js` as a module, so `suspicious/noRedundantUseStrict` misfires on our per-IIFE `'use strict'`: turn it off for static files. Turn `style/useTemplate` off: it's 271 info-level hits that never block and would bury the real output. CI runs with `--error-on-warnings`.

The "classic script, no module syntax" rule goes into `tests/web/test_static_lint.py` as a regex, alongside the existing sink, remote-host, and colour checks. Biome can't express it.

### 2. pre-commit is the only place tool versions are pinned (for lint/format)
`.pre-commit-config.yaml` with:
- `repo: local` hooks for `ruff format` and `ruff check --fix --force-exclude`, `language: system`, run via `poetry run`, so ruff's version stays in `poetry.lock`.
- The official `biomejs/pre-commit` hook (`biome-check`), pinned by `rev`, with `additional_dependencies: ["@biomejs/biome@2.6.7"]`.
- `check-yaml`, `end-of-file-fixer`, `trailing-whitespace`, `check-merge-conflict`, `check-added-large-files`.

CI replaces its `ruff check` / `ruff format --check` steps with `poetry run pre-commit run --all-files --show-diff-on-failure`, on both Linux and Windows. That adds the missing format check on Windows, and the frontend check runs inside `lint-test`, which `ci-ok` already requires. `~/.cache/pre-commit` is cached by config hash. mypy stays a separate CI step because it's too slow for a commit hook. `pre-commit` joins the Poetry dev group. Installing the git hook (`pre-commit install`) stays optional and documented. *Alternative:* a `package.json` pinned with bun (the house TS convention). Rejected: it adds a JS toolchain for one binary, and students on Windows pit laptops would need it too.

### 3. Ruff rule set
Add `S`, `BLE`, `T20`, `DTZ`, `C90` (max-complexity 12), `ARG`, `PERF`, `PT`, `PGH` (no blanket `noqa`), `RUF` (includes `RUF100` unused-noqa), and `TID251` banned-api: `sys.exit` and `os._exit` banned outside `cli.py`, `__main__.py`, and `tools/`, and `os.system` and `os.popen` banned everywhere.
Per-file ignores: `tests/**` → `S101`, `S105`, `S106`, `S311`; `cli.py`, `__main__.py`, `tools/**` → `T20`. Skip `TRY`, `FBT`, `PLR0913`, `PLR2004`: about 230 hits that would only teach people to add `noqa`.
mypy: add `enable_error_code = ["ignore-without-code"]` (`strict` already warns on unused ignores).
Ruff can't check two rules: that a suppression has a reason, and that `S602`/`S604`/`S605` (shell) are never suppressed. A test in `tests/test_quality.py` greps `src/`, `tests/`, and `tools/` for both, using the format `# noqa: CODE[, CODE] - reason`. Existing violations are fixed where it's cheap. Otherwise they get a reasoned suppression; the 9 complex functions get `# noqa: C901 - grandfathered (code-quality-gates)`.

### 4. Coverage
`pytest-cov` in the dev group. `[tool.coverage]`: `source = ["flashpoint"]`, `branch = true`, `fail_under = <overall floor>`, with `skip_covered` in the report. Only the py3.12 `lint-test` leg runs with `--cov`. Per-package floors are separate `coverage report --include=... --fail-under=85` steps (coverage.py has no per-package threshold) for `readers/` and `lake/`. The roadmap's third package, `physics`, lives at `semantics/physics.py`, and gets the 85% floor too. The floors measure the unit run only. Corpus coverage is real coverage, but combining runs across jobs costs more than it gives at this stage.

The coverage leg runs with `NUMBA_DISABLE_JIT=1`, so the reader kernels run as Python and get measured. Without it, `readers/` reads 73% and the 85% floor fails on code that is tested but can't be traced. The compiled path is still tested by the py3.11 leg, Windows, and the corpus job, which all keep JIT on. *Alternative:* exclude `@njit` functions from coverage. Rejected: that hides the most performance-critical parsing code from the measurement entirely.

Floors: **overall 93%** (measured 95%, leaving 2 points for platform noise) and **85%** per core package (measured 94/98/100).

### 5. `ci-ok`
`needs: [changes, docs, lint-test, corpus, browser, clean-install]`. The existing jq rule (every need is `success` or `skipped`) already handles matrix and docs-only skips. Windows and perf stay outside. The browser job's known flake (#17, the owlet tail on the Q7 rio hoot) is a test-level problem. If it recurs, that one test gets a marker and an issue; the job doesn't get dropped.

### 6. Self-tests against bad fixtures
`tests/fixtures/quality/python/<RULE>.py` and `tests/fixtures/quality/js/<rule>.js`, one per check-enforced rule, each with the smallest violation. Both trees are excluded in ruff (`extend-exclude`) and in Biome. `tests/test_quality.py` runs ruff with `--no-force-exclude` on each Python fixture, and Biome on a temp copy of the JS fixtures (with the exclusion dropped from the config). It asserts the expected rule code appears in the output, not just a non-zero exit. Biome runs through the pre-commit-managed install (`pre-commit run biome-check --files ...`) so the test uses the pinned version. Tests that need Biome skip with a clear reason if `pre-commit` can't bootstrap Node offline, and CI always has it.

### 7. Workflow hygiene
Every workflow gets a top-level `permissions: contents: read`. Jobs that need more (release, CodeQL's `security-events: write`) widen it per job. Third-party `uses:` are pinned to a 40-hex SHA with a `# vX.Y.Z` comment. A test in `tests/test_quality.py` checks both with line regexes, so no YAML dependency. `.github/dependabot.yml`: `pip` (Poetry) and `github-actions`, `target-branch: development`, monthly, one group per ecosystem. New `codeql.yml`: `python` and `javascript-typescript`, `build-mode: none`, vendor ignored, on PRs to `development`/`main` and weekly. The block on high/critical alerts is a code-scanning rule added to the `dev-main-pr-gate` ruleset, a mentor task.

### 8. One reformat commit
The JS/CSS reformat is a commit with no other change, and its SHA goes in `.git-blame-ignore-revs`. The developer guide tells readers to set `git config blame.ignoreRevsFile .git-blame-ignore-revs`; GitHub's blame view reads the file automatically.

## Risks / Trade-offs

- [Biome's Node bootstrap through pre-commit fails on a Windows pit laptop] → CI is the gate. Locally, `npx @biomejs/biome@2.6.7 check` is the documented fallback.
- [Big one-time diff in the frontend] → isolated commit + blame-ignore. Browser tests run on that commit to prove no behavior changed.
- [Requiring browser re-exposes flake #17] → quarantine the single test, as in Decision 5.
- [SHA-pinned actions go stale] → Dependabot's `github-actions` ecosystem bumps SHAs.
- [Time budget] → pre-commit adds ~40 s cold and ~10 s cached per lint leg. Coverage with JIT off adds ~10% to the py3.12 unit run (307 s vs 280 s locally). CodeQL runs in parallel (~5 min) and isn't in `ci-ok`. No full-log memory impact.

## Migration Plan

Land in one PR, commits in task order: config and self-tests first (red), then Python fixes, the format-only commit, frontend fixes, CI wiring, and docs. The ruleset's code-scanning rule is added after merge, once CodeQL has a baseline on `development`. Rollback: revert the PR. The ruleset rule is removed by hand.

## Open Questions

- Should the pre-commit hook pins move to Dependabot's `pre-commit` ecosystem, if it's available for this org? Until then they're bumped by hand in the season maintenance pass. This doesn't affect the specs or the task list.
