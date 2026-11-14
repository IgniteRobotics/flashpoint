## Why

Flashpoint has strong Python gates (ruff, mypy `--strict`, pytest) but nothing on the frontend except one grep test, and no written Python or JavaScript standard at all: the team's coding standard covers only Java. Students are starting to contribute, and the review bot reviews against a document that doesn't describe this repo. The gaps are concrete:

- 59 `noqa` comments in `src/` suppress rules that aren't enabled. They are written for security rules the config never turned on, so they suppress nothing and promise a check that doesn't run.
- The browser tests are the only thing that executes the 3.4k lines of app JavaScript, and they aren't part of the required check. Neither is the clean wheel install, the only job that tests what `pipx install` gives a team.
- The roadmap's quality gates (06-roadmap §5) promise coverage of at least 85% on the core packages. Nothing measures coverage.
- CI has no dependency updates, no code scanning, and runs with the default token scope.

## What Changes

- **Written standards.** A Python and JavaScript coding standard for Flashpoint, each rule with its reason, in the developer guide. The same text goes to the ignition-labs coding standards page as a new Flashpoint section (separate PR in that repo).
- **Python rules, blocking.** Add security, blind-except, print, timezone, complexity, unused-argument, test-style, and suppression-hygiene rules to the linter. Ban process exit outside the CLI entry point (#57) and shell-string subprocesses (#54). Every suppression must name its rule and give a reason. Existing violations are fixed or grandfathered with a reasoned suppression; no rule ships as a warning.
- **Frontend rules, blocking.** A linter and formatter for the app's JavaScript and CSS, with vendored code excluded. The existing escaping, offline, and colour-token checks stay (#56). Add checks that app scripts stay classic scripts and declare no globals beyond the app namespace.
- **Coverage gate.** Measure line and branch coverage on the unit run; fail below a floor overall and below 85% on the readers and lake packages.
- **Required check widened.** `ci-ok` also waits on the browser job and the clean-install matrix.
- **One local command.** A pre-commit configuration that runs the same formatters and linters CI runs, at the same versions.
- **CI hardening.** Least-privilege token permissions, actions pinned by commit, Dependabot for Python packages and actions, and code scanning for Python and JavaScript.
- **One-time reformat.** The app's JS and CSS are reformatted in a single commit listed in `.git-blame-ignore-revs`.

## Capabilities

### New Capabilities
- `coding-standards`: The Python and JavaScript rules Flashpoint code must follow, and which of them the required check enforces.
- `ci-quality-gates`: What the required check runs, the coverage floors, the local pre-commit command, and the CI supply-chain settings.

### Modified Capabilities
- `project-documentation`: The developer guide must carry the coding standards and the single command that runs every check locally.

## Impact

- `pyproject.toml` (ruff rules, coverage config, new dev dependencies), new `biome.json`, `.pre-commit-config.yaml`, `.git-blame-ignore-revs`, `.github/dependabot.yml`, `.github/workflows/ci.yml`, new `.github/workflows/codeql.yml`.
- Lint fixes and suppressions across `src/`, `tests/`, `tools/`. Reformatted `src/flashpoint/web/static/*.js` and `*.css`. No runtime behavior changes.
- New tests in `tests/` that run the gates against deliberately bad fixture files.
- `docs/guide/developer-guide.md`, `docs/rewrite/06-roadmap.md` §5.
- A ruleset update so code-scanning alerts block merges (mentor task).
- Roadmap: implements the cross-phase quality gates in 06-roadmap §5. Pitfalls: #54, #56, #57, and #60 (process hygiene). This change touches no log reading, so there is no malformed-log or quarantine behavior to specify.

## Non-goals

- Type-checking the JavaScript. That's a follow-up once the lint baseline is clean.
- Refactoring the functions that are over the complexity ceiling. They are grandfathered, not rewritten.
- Changing app behavior, the escaping model, or the no-build-step design.
- Making the Windows job or the perf job required.
- Standards for the robot code (Java) or other Ignite repos.
