## MODIFIED Requirements

### Requirement: CI supply chain
Workflows SHALL request least-privilege token permissions, read-only unless a job needs more. Third-party actions SHALL be pinned to a full commit, with the version noted beside it. Dependency update pull requests SHALL be opened against the development branch for Python packages and for actions. Minor and patch updates SHALL be grouped so that one run covers one ecosystem; each major update SHALL arrive as its own pull request. Every runtime and optional dependency SHALL declare an upper bound below the next major version, or below the next minor version for a package still at 0.x. Code scanning SHALL run on Python and JavaScript for every pull request and weekly, and a high or critical alert SHALL block merging.

#### Scenario: Unpinned action rejected
- **WHEN** a workflow references an action by tag or branch instead of a commit
- **THEN** the unit test run fails and names the workflow and the action

#### Scenario: Default token scope rejected
- **WHEN** a workflow has no top-level permissions block
- **THEN** the unit test run fails and names the workflow

#### Scenario: High-severity alert
- **WHEN** code scanning raises a high or critical alert on a pull request
- **THEN** the pull request cannot merge until the alert is fixed or dismissed with a reason

#### Scenario: Unbounded runtime dependency rejected
- **WHEN** a runtime or optional dependency is declared with no upper bound
- **THEN** the unit test run fails and names the dependency

#### Scenario: Major updates stay out of the group
- **WHEN** a dependency update group would include major updates
- **THEN** the unit test run fails and names the ecosystem
