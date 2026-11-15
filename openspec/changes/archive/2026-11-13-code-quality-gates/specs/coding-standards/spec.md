## Purpose

The Python and JavaScript rules Flashpoint code follows, the reason for each, and which of them the required pull-request check enforces, so that a student, a mentor, and the review bot all hold code to the same written standard.

## ADDED Requirements

### Requirement: Standards are written down with reasons
The developer guide SHALL contain a coding standards section for Python and for the app's JavaScript and CSS. Every rule SHALL state why it exists. Every rule SHALL say whether the required check enforces it or a reviewer does. The ignition-labs coding standards page SHALL carry the same rules in a Flashpoint section, and the developer guide SHALL link to it. When the two differ, the ignition-labs page is the source and the developer guide is updated to match.

#### Scenario: Every rule has a reason and an enforcer
- **WHEN** a reader reads any rule in the developer guide's coding standards section
- **THEN** the rule states its reason and names either the required check or review as what enforces it

#### Scenario: A rule the check enforces is caught
- **WHEN** a pull request adds code that breaks a rule the section marks as check-enforced
- **THEN** the required check fails and names the file, the line, and the rule

### Requirement: Suppressions name the rule and the reason
A comment that turns off a lint or type rule SHALL name the specific rule and give a reason on the same line. A suppression that names no rule, or names a rule that is not reporting on that line, SHALL fail the required check. The JavaScript and CSS linter's suppressions SHALL also carry a reason.

#### Scenario: Bare suppression rejected
- **WHEN** a Python line carries a suppression that names no rule, or a type suppression with no error code
- **THEN** the required check fails on that line

#### Scenario: Stale suppression rejected
- **WHEN** a suppression names a rule that does not report on its line
- **THEN** the required check fails on that line and says the suppression is unused

### Requirement: Python processes and subprocesses
Code outside the command-line entry point SHALL NOT exit the process, so that one bad log cannot abort a batch (#57). Subprocesses SHALL be started with an argument list and never through a shell string (#54). Every subprocess call SHALL carry a suppression whose reason says where its arguments come from.

#### Scenario: Exit in library code rejected
- **WHEN** a module other than the command-line entry point calls a process exit
- **THEN** the required check fails on that call

#### Scenario: Shell string rejected
- **WHEN** code starts a subprocess through a shell or with a shell command string
- **THEN** the required check fails on that call, and no suppression is accepted for it

### Requirement: Python SQL, exceptions, output, and time
SQL text SHALL be built only from internal identifiers, with values passed as bound parameters. A line that builds SQL from strings SHALL carry a suppression saying which identifiers it interpolates. Code SHALL NOT catch every exception unless it is at a boundary that records the error (a per-file ingest loop, a request handler, a service loop), with a suppression naming that boundary. Library code SHALL NOT print; output goes through logging, or through the command-line entry point and the tools. Date-times SHALL be timezone-aware.

#### Scenario: Interpolated SQL needs a reason
- **WHEN** a pull request adds a query built by string interpolation with no suppression
- **THEN** the required check fails on that line

#### Scenario: Catch-all outside a boundary rejected
- **WHEN** code catches every exception without a suppression
- **THEN** the required check fails on that handler

#### Scenario: Print in library code rejected
- **WHEN** a module outside the command-line entry point and the tools prints
- **THEN** the required check fails on that line

#### Scenario: Naive date-time rejected
- **WHEN** code creates a date-time with no timezone
- **THEN** the required check fails on that line

### Requirement: Python complexity and style
Functions SHALL stay under a cyclomatic complexity ceiling of 12. A function over the ceiling when this standard was adopted SHALL carry a suppression marking it grandfathered, and SHALL NOT be made more complex. Unused function arguments, needless list loops, and non-idiomatic test code SHALL fail the check. Type hints, strict type checking, import order, naming, and formatting rules already in force SHALL stay in force.

#### Scenario: New complex function rejected
- **WHEN** a pull request adds a function with complexity above the ceiling
- **THEN** the required check fails and names the function

#### Scenario: Grandfathered function grows
- **WHEN** a pull request makes a grandfathered function more complex
- **THEN** review rejects it, citing the standard

### Requirement: App scripts stay classic and namespaced
The app's JavaScript SHALL be classic scripts with no module syntax and no build step, so that a static Replay export opens from disk. Each file SHALL run in strict mode, and SHALL NOT read or create globals other than the app namespace, the mode object, and the vendored chart library. Vendored files SHALL NOT be edited, linted, or reformatted.

#### Scenario: Module syntax rejected
- **WHEN** an app script contains an import or export statement
- **THEN** the required check fails and names the file

#### Scenario: Undeclared global rejected
- **WHEN** an app script reads or assigns a global that is not on the allowed list
- **THEN** the required check fails on that line

#### Scenario: Vendored code untouched
- **WHEN** the formatter and linter run over the repository
- **THEN** they report nothing for, and change nothing in, the vendored directory

### Requirement: App scripts are linted and formatted
App JavaScript and CSS SHALL pass the frontend linter's recommended rules with warnings treated as errors, and SHALL match the frontend formatter's output. The existing rules stay: views build DOM only through the app's DOM builder and never through an HTML sink, inline handler, or eval (#56); app files reference no remote host; colours come only from the role-named tokens.

#### Scenario: Lint warning blocks
- **WHEN** a pull request adds an unused variable or a loose equality comparison to an app script
- **THEN** the required check fails on that line

#### Scenario: Unformatted file blocks
- **WHEN** an app script or stylesheet differs from the formatter's output
- **THEN** the required check fails and names the file

#### Scenario: HTML sink still blocked
- **WHEN** an app script assigns to an HTML sink
- **THEN** the required check fails and names the file
