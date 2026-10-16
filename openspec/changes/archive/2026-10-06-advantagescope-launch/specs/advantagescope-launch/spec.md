## Purpose

Let a user on the machine running Flashpoint open a match's raw logs in AdvantageScope with one action, opened in place under readable names, without downloading copies and without letting any other page or machine start a program.

## ADDED Requirements

### Requirement: Launch is local only
The launch action SHALL be available only when all of these hold:
- the app is served (not a static export);
- the server is bound to the local machine only;
- the request comes from the local machine;
- AdvantageScope has been located.

The app SHALL tell the page whether the launch is available and, when it is not, why. Whenever the launch is unavailable or fails, the page SHALL offer the raw-log download instead.

#### Scenario: Default local serve
- **WHEN** the app is served with its default binding, and AdvantageScope is located
- **THEN** the page reports the launch as available

#### Scenario: Shared on the pit network
- **WHEN** the app is served bound to all interfaces
- **THEN** the launch is reported unavailable with the reason "shared on the network", a launch request is refused, nothing is started, and the download is offered

#### Scenario: AdvantageScope not found
- **WHEN** the app is served locally and no AdvantageScope install is located
- **THEN** the launch is reported unavailable with the reason "AdvantageScope not found", and the download is offered

### Requirement: Guarded launch request
A launch SHALL happen only in response to a request that:
- uses a method that page loads, links, and image or script tags cannot send;
- carries the app's own request marker;
- comes from the app's own origin and names a local host.

The request SHALL carry only a match key. It SHALL NOT accept a file path, a program path, or arguments. The match key SHALL be treated as data and resolved through the ledger. A refused request SHALL start nothing and SHALL say why. No launch request SHALL change the lake.

#### Scenario: Cross-site request
- **WHEN** a launch request for `2026johnson_qm15` arrives with an `Origin` other than the app's own
- **THEN** it is refused, and nothing is started

#### Scenario: Missing request marker
- **WHEN** a launch request arrives from the app's origin without the app's request marker
- **THEN** it is refused, and nothing is started

#### Scenario: Foreign host name
- **WHEN** a launch request arrives with a `Host` that is not a local name or loopback address
- **THEN** it is refused, and nothing is started

#### Scenario: Unknown or hostile match key
- **WHEN** a launch request names the match key `../../etc/passwd` or a key not in the ledger
- **THEN** it answers not-found, nothing is staged or started, and the lake is byte-identical afterwards

#### Scenario: Extra fields ignored
- **WHEN** a valid launch request also carries a `path` or `args` field
- **THEN** those fields have no effect on what is staged or started

### Requirement: Raw logs staged under readable names
On launch, the app SHALL stage every raw log of the match (its wpilog and each hoot) into one folder for that match under the operating system's temporary folder. Each file SHALL be named `<match key>__<wpilog or bus>__<original file name>`. Staged content SHALL be identical to the raw store's. The app SHALL link to the raw store where the operating system allows, and copy only otherwise. The raw store SHALL be unchanged. Staging the same match again SHALL reuse the folder. Staged folders older than one day SHALL be removed when the server starts, and nothing outside the staging folder SHALL ever be removed.

#### Scenario: Q15 staged
- **WHEN** the launch runs for `2026johnson_qm15` on a lake holding its wpilog and rio and CANivore hoots
- **THEN** one folder holds three files whose names start with `2026johnson_qm15__`, each file's SHA-256 equals its ledger hash, and the raw store is byte-identical afterwards

#### Scenario: Repeat launch
- **WHEN** the launch runs twice for the same match
- **THEN** the second run reuses the same folder, and it holds no duplicate files

#### Scenario: Old staging cleaned
- **WHEN** the server starts and a staged match folder is more than one day old
- **THEN** that folder is removed, and newer staged folders and all files outside the staging folder are untouched

#### Scenario: Raw file missing from the store
- **WHEN** the ledger lists a raw log for the match but the raw store no longer holds it
- **THEN** nothing is started, and the response names the missing file's hash

#### Scenario: Truncated hoot
- **WHEN** one of the match's hoots was ingested as an incomplete read
- **THEN** it is still staged, and the launch result marks it incomplete

### Requirement: AdvantageScope opened on the match's wpilog
The launch SHALL start AdvantageScope with exactly one argument: the staged wpilog. It SHALL start it directly, never through a command shell, and SHALL NOT wait for it to exit. The result SHALL name the staged folder and list the hoots there to add with File › Insert log. A match with no wpilog SHALL NOT be launched; the result SHALL say why, and the download SHALL be offered. If AdvantageScope fails to start, the result SHALL say so and the download SHALL be offered.

#### Scenario: Launch Q15
- **WHEN** the launch runs for `2026johnson_qm15` with a stand-in program configured as AdvantageScope
- **THEN** the stand-in is started once with a single argument, the staged `2026johnson_qm15__wpilog__…wpilog` path; the result lists the two staged hoots; and the server answers other requests while the stand-in is still running

#### Scenario: Paths with spaces and quotes
- **WHEN** the staging folder's path contains spaces and a quote character (as a Windows user folder can)
- **THEN** the stand-in receives the staged wpilog path as one intact argument

#### Scenario: Hoot-only match
- **WHEN** the launch is requested for a match that has hoots but no wpilog
- **THEN** nothing is started, the result says the match has no wpilog, and the download is offered

#### Scenario: Program fails to start
- **WHEN** the configured AdvantageScope path exists but cannot be executed
- **THEN** the result reports the failure, and the download is offered

### Requirement: Locating AdvantageScope
The app SHALL locate AdvantageScope in this order:
1. an explicit path in the views configuration;
2. the newest-year WPILib install for the current operating system;
3. a standalone install in the operating system's usual applications location.

An explicit path that does not exist SHALL be reported as an error, and the app SHALL NOT fall back past it. `flashpoint doctor` SHALL print the AdvantageScope found and which rule found it, or "not found".

#### Scenario: Newest WPILib year wins
- **WHEN** WPILib installs for 2025 and 2026 both hold AdvantageScope and no explicit path is set
- **THEN** the 2026 install is used, and `doctor` names it with "WPILib 2026"

#### Scenario: Explicit path missing
- **WHEN** the configuration names an AdvantageScope path that does not exist
- **THEN** the launch is unavailable with a reason naming that path, and `doctor` reports the same

#### Scenario: Nothing installed
- **WHEN** no explicit path is set and no WPILib or standalone install exists
- **THEN** `doctor` prints "AdvantageScope: not found", and the launch is unavailable
