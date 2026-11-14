## MODIFIED Requirements

### Requirement: Developer guide
The documentation SHALL include a developer guide for people who change the code. It SHALL explain how Flashpoint works:
- the ingest pipeline from raw file to the bronze, silver, and gold layers;
- what each lake layer holds;
- match identity and session grouping;
- device identity (slot versus unit);
- clock alignment;
- detection.

It SHALL also cover building and running from source, the test markers and when each runs, fetching the golden corpus, what CI runs and which jobs the required check waits on, the coverage floors, the change workflow (specs, changes, branching), and where the decisions and research live. It SHALL carry the coding standards for Python and the app frontend, and the single command that runs every formatter and linter locally. It SHALL link to the target architecture and the ADRs rather than restating them in full.

#### Scenario: Build and test from the developer guide
- **WHEN** a reader with a supported Python version follows the developer guide's build and unit test steps on a fresh clone
- **THEN** the steps install the project and the unit test run passes

#### Scenario: Pipeline explained end to end
- **WHEN** a reader looks up what happens to a `.hoot` file after ingest
- **THEN** the developer guide traces it from the raw copy, through conversion and the bronze samples, to silver mapping and gold match features, and names the command that triggers each step

#### Scenario: Checks run locally before a push
- **WHEN** a reader follows the developer guide's local check step on a fresh clone of the default branch
- **THEN** one command runs every formatter and linter CI runs, and it passes
