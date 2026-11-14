## Purpose

What the required pull-request check runs, how coverage is measured and floored, the single local command that runs the same checks, and the CI supply-chain settings, so that a passing check means the same thing on a laptop and on GitHub.

## ADDED Requirements

### Requirement: The required check covers every blocking job
The single required check SHALL pass only when, for a change that touches code, the Python lint, type, and unit job, the golden-corpus job, the browser job, and the clean-install job on Linux, macOS, and Windows all passed. A docs-only change SHALL still pass after only the docs job. The Windows unit job and the perf job SHALL stay outside the required check.

#### Scenario: Browser failure blocks
- **WHEN** a code change passes every job except the browser job
- **THEN** the required check fails

#### Scenario: Clean-install failure blocks
- **WHEN** a code change passes every job except the clean install on one operating system
- **THEN** the required check fails

#### Scenario: Docs-only change
- **WHEN** a change touches only documentation
- **THEN** the code jobs are skipped and the required check passes once the docs job passes

### Requirement: Lint and format run on Python and the app frontend
The Python lint, format, and type checks SHALL run on every code change on Linux and Windows. The frontend lint and format check SHALL run on every code change and be part of the required check. Every rule SHALL be blocking: no rule reports as a warning only.

#### Scenario: Frontend lint error blocks
- **WHEN** a code change adds a frontend lint error
- **THEN** the job that runs the frontend check fails, and so does the required check

#### Scenario: Format drift on Windows
- **WHEN** a Python file is unformatted
- **THEN** the format check fails on both Linux and Windows

### Requirement: Coverage floors
The unit test run on the newest supported Python SHALL measure line and branch coverage of the package. It SHALL fail when total coverage is below the configured overall floor, or when coverage of the readers package or the lake package is below 85%. The failure SHALL print the measured and required percentages. The floors SHALL be raised, never lowered, as coverage grows; lowering one needs a mentor's approval in the pull request.

#### Scenario: Total below floor
- **WHEN** a change drops total unit-run coverage below the overall floor
- **THEN** the job fails and prints the measured and required totals

#### Scenario: Core package below floor
- **WHEN** coverage of the readers package or the lake package drops below 85%
- **THEN** the job fails and names the package

### Requirement: One local command runs the checks
The repository SHALL provide a pre-commit configuration that runs the Python formatter and linter and the frontend formatter and linter. CI SHALL run the formatters and linters through that same configuration, so each tool's version is pinned in one place and local and CI results cannot differ. Running it over all files on a clean checkout of the default branch SHALL pass.

#### Scenario: Clean tree passes locally
- **WHEN** a developer runs the pre-commit hooks over all files on a clean checkout of the default branch
- **THEN** every hook passes and no file changes

#### Scenario: Local and CI agree
- **WHEN** a file passes the pre-commit hooks locally
- **THEN** the CI format and lint step passes on that file too, because it runs the same hooks at the same versions

### Requirement: Self-test against known-bad fixtures
The unit test run SHALL run the Python and frontend linters over a set of deliberately bad fixture files, one per check-enforced rule, and SHALL fail if any fixture passes. The fixture files SHALL be excluded from the repository's normal lint and format run.

#### Scenario: A rule silently turned off
- **WHEN** a configuration change disables a check-enforced rule
- **THEN** the fixture for that rule passes the linter, and the unit test run fails naming the rule

### Requirement: CI supply chain
Workflows SHALL request least-privilege token permissions, read-only unless a job needs more. Third-party actions SHALL be pinned to a full commit, with the version noted beside it. Dependency update pull requests SHALL be opened against the development branch for Python packages and for actions, grouped so that one run covers one ecosystem. Code scanning SHALL run on Python and JavaScript for every pull request and weekly, and a high or critical alert SHALL block merging.

#### Scenario: Unpinned action rejected
- **WHEN** a workflow references an action by tag or branch instead of a commit
- **THEN** the unit test run fails and names the workflow and the action

#### Scenario: Default token scope rejected
- **WHEN** a workflow has no top-level permissions block
- **THEN** the unit test run fails and names the workflow

#### Scenario: High-severity alert
- **WHEN** code scanning raises a high or critical alert on a pull request
- **THEN** the pull request cannot merge until the alert is fixed or dismissed with a reason
