## Purpose

Documentation that tells a student, mentor, or developer what Flashpoint is for, how it works, how to build and run it, and how to set up automatic log import, kept in step with the code by automated checks.

## ADDED Requirements

### Requirement: README is an overview and entry point
The repository README SHALL say what Flashpoint does and who it's for in its opening section. It SHALL include a quickstart of five commands or fewer that goes from a fresh checkout to a served Replay view. It SHALL link to the user guide, the developer guide, the automatic import guide, and the CLI reference. The README SHALL NOT be organized by roadmap phase.

#### Scenario: New reader finds the purpose and the guides
- **WHEN** a reader opens the README
- **THEN** the first section states what Flashpoint ingests, what it tracks, and that it runs offline on a pit laptop, and the README links to all four guides

#### Scenario: No phase-organized sections
- **WHEN** the README's headings are listed
- **THEN** none of them names a roadmap phase (P0 to P6)

### Requirement: User guide
The documentation SHALL include a user guide for people who run Flashpoint but don't change its code. It SHALL cover:
- what Flashpoint is for, and what it isn't (it links to AdvantageScope for single-log deep dives rather than replacing it);
- installing on macOS, Linux, and Windows;
- ingesting logs by hand;
- serving the app and reading the Replay, History, and Anomalies views, including the shared filters and anomaly triage;
- building a static Replay export for a USB stick;
- describing a robot in its config file;
- where the lake lives and how to point Flashpoint at another one;
- reading the output of `flashpoint doctor`.

#### Scenario: First run from the user guide
- **WHEN** a reader follows the user guide's install and first-ingest steps on a supported OS, using a log from the golden corpus
- **THEN** each command succeeds as written, and the served app lists that log's match in Replay

#### Scenario: Offline preparation is called out
- **WHEN** a reader looks for what to do before taking a laptop to an event
- **THEN** the user guide says the hoot converter is downloaded on first use and must be fetched while online, and gives the command that does it

### Requirement: Developer guide
The documentation SHALL include a developer guide for people who change the code. It SHALL explain how Flashpoint works:
- the ingest pipeline from raw file to the bronze, silver, and gold layers;
- what each lake layer holds;
- match identity and session grouping;
- device identity (slot versus unit);
- clock alignment;
- detection.

It SHALL also cover building and running from source, the test markers and when each runs, fetching the golden corpus, what CI runs, the change workflow (specs, changes, branching), and where the decisions and research live. It SHALL link to the target architecture and the ADRs rather than restating them in full.

#### Scenario: Build and test from the developer guide
- **WHEN** a reader with a supported Python version follows the developer guide's build and unit test steps on a fresh clone
- **THEN** the steps install the project and the unit test run passes

#### Scenario: Pipeline explained end to end
- **WHEN** a reader looks up what happens to a `.hoot` file after ingest
- **THEN** the developer guide traces it from the raw copy, through conversion and the bronze samples, to silver mapping and gold match features, and names the command that triggers each step

### Requirement: Automatic import guide
The documentation SHALL include a guide to automatic log import as it exists. It SHALL cover:
- what the watch pulls (roboRIO internal and USB log roots, and removable volumes on the pit machine) and that it never modifies or deletes anything on the robot or the stick;
- every key in the acquisition config file, with its default;
- a dry run before installing a service;
- installing the service on Linux and Windows;
- running it on macOS, where no service definition exists;
- optional backup and the restore drill;
- checking status;
- troubleshooting for: robot not reachable, USB volume not seen, file skipped as still being written, low robot disk space, and a stale lock.

It SHALL state plainly that importing from cloud folders is not supported.

#### Scenario: Linux service from the guide
- **WHEN** a reader follows the Linux steps on a machine with a user service manager
- **THEN** the unit file the guide names exists in the repository, and the commands enable it and show its log

#### Scenario: macOS limitation is explicit
- **WHEN** a macOS reader looks for how to run import in the background
- **THEN** the guide says there's no service definition for macOS and gives the foreground command to run instead

#### Scenario: Install command is correct
- **WHEN** a reader runs the install command the guide gives for a service install
- **THEN** it installs Flashpoint from this repository, not a package of the same name from a public index

### Requirement: CLI reference
The documentation SHALL include a CLI reference with one section for every `flashpoint` subcommand. Each section SHALL give its purpose, its options, and at least one example.

#### Scenario: Every subcommand documented
- **WHEN** the set of subcommands the CLI accepts is compared with the section headings in the CLI reference
- **THEN** each subcommand has a section

### Requirement: Documentation drift checks
The unit test suite SHALL check that every relative link in the README, the deploy README, and the guides points to a file that exists in the repository, and that every CLI subcommand has a section in the CLI reference. A failed check SHALL name the file, the line, and the broken link or the missing subcommand. The checks SHALL run on every pull request on Linux and Windows. They SHALL NOT need the network or the golden corpus.

#### Scenario: Broken link fails the suite
- **WHEN** a guide contains a relative link to a file that doesn't exist
- **THEN** the unit test run fails and names the guide, the line number, and the link target

#### Scenario: Undocumented subcommand fails the suite
- **WHEN** a new subcommand is added to the CLI without a section in the CLI reference
- **THEN** the unit test run fails and names the subcommand

#### Scenario: External links are not fetched
- **WHEN** a guide contains an absolute web link
- **THEN** the check skips it and makes no network request

#### Scenario: Malformed link syntax is reported, not crashed on
- **WHEN** a guide contains an unclosed link, such as `[text](path` with no closing parenthesis
- **THEN** the check reports that file and line as malformed instead of raising an unhandled error
