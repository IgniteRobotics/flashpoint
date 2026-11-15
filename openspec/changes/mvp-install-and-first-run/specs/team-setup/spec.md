## ADDED Requirements

### Requirement: Documentation links
Every pointer to the documentation that the CLI, `doctor`, the setup checks and the served app show SHALL be an address on the published documentation site, in the version of the docs that matches the installed package (`X.Y` for a final release `X.Y.Z` or a pre-release of it such as a release candidate, since both tags publish that numbered docs version; `dev` for a development build), built from one base address. No user-facing text SHALL name a path inside the source repository (such as `docs/guide/auto-import.md`). The app's shipped files SHALL still reference no remote host: in served mode the app SHALL get documentation addresses from the server, and a static export SHALL name the guide page and section by title instead of linking.

#### Scenario: Every check's fix
- **WHEN** every setup check is run against a synthetic configuration and lake in which it is not ok (no configuration, no team, no robot, unwritable lake, backup not configured, owlet missing, AdvantageScope missing, acquire service not installed)
- **THEN** no message or command contains `docs/guide`, and every documentation address in them starts with the published base address

#### Scenario: Links match the installed version
- **WHEN** the backup check's guide address is produced by an installed package of version 0.3.1, again by 0.3.0rc1, and again by 0.4.0.dev2
- **THEN** the first two addresses are under the docs site's `0.3` version and the third under `dev`, all on the same base address

#### Scenario: Live CAN guide reference
- **WHEN** the Live CAN view is opened in served mode
- **THEN** it shows the Live CAN guide's published address, and a scan of the app's shipped files still finds no remote host

#### Scenario: Static export names the guide
- **WHEN** a static export is opened from disk with networking disabled and a view that refers to the guide is shown
- **THEN** it names the guide page and section by title, and no address is requested

## MODIFIED Requirements

### Requirement: Installable public package
Flashpoint SHALL be published as a package that installs on macOS, Windows and Linux with one documented command that brings its own Python interpreter: `uv tool install --python 3.13 flashpoint`. The documentation SHALL give `pipx install flashpoint` as the alternative, using the `py` launcher form on Windows, and SHALL NOT name an interpreter command that doesn't exist on the platform (such as `python3.12` on Windows). The package SHALL declare support for Python 3.11 to 3.14, and CI SHALL test every Python version the package declares. The install documentation SHALL state the platform floor that the package's dependencies set: macOS 13 or later, Linux with glibc 2.35 or later (Ubuntu 22.04 or later), and x86-64 Windows; and that Python 3.15 isn't supported yet. The installed package SHALL include the default configuration (season roots that every WPILib robot shares, report and anomaly thresholds, anomaly cause text) and SHALL NOT include any team's robot files, units list or logs. Each release SHALL publish its version, and `flashpoint --version` SHALL print it.

#### Scenario: Clean install
- **WHEN** the built package is installed into an empty environment and `flashpoint --version` and `flashpoint doctor` are run with no configuration
- **THEN** both exit, the version matches the package version, and `doctor` reports that no configuration exists and names `flashpoint init`

#### Scenario: No team data in the package
- **WHEN** the built package's files are listed
- **THEN** it holds the default configuration files and no robot file, units list, `.wpilog` or `.hoot`

#### Scenario: Declared Pythons are tested
- **WHEN** the package's declared Python range and classifiers are compared with the CI unit-test matrix
- **THEN** the range is 3.11 up to but not including 3.15, and each of 3.11, 3.12, 3.13 and 3.14 is both a classifier and a CI matrix entry

#### Scenario: Fresh machine with no suitable Python
- **WHEN** the documented install command is run on a fresh macOS, Ubuntu 24.04 or Windows 11 machine whose only Python is outside 3.11–3.14, or that has none
- **THEN** the install succeeds and `flashpoint --version` prints the release's version

### Requirement: Configuration home
The configuration root SHALL be, in order: a path given on the command line, then `FLASHPOINT_CONFIG`, then the per-user configuration folder for the platform (`~/.config/flashpoint` on Linux and macOS, `%APPDATA%\flashpoint` on Windows). Flashpoint SHALL NOT fall back to a folder inside a source checkout. Where a file is missing from the configuration root, the bundled default SHALL be used, and `doctor` and the Setup view SHALL say which files come from defaults. Season files SHALL resolve per year: the root's own `seasons/<year>.toml` when it exists, otherwise the bundled file for that year, so a season file added in a later release is read on an existing configuration without running `init` again.

#### Scenario: Per-user folder
- **WHEN** `FLASHPOINT_CONFIG` is unset and the per-user folder holds `acquire.toml` with team 254
- **THEN** `doctor` shows the per-user folder as the configuration root and team 254

#### Scenario: Default thresholds
- **WHEN** the configuration root has no `report.toml`
- **THEN** the bundled report thresholds are used, and `doctor` lists `report.toml` as a default

#### Scenario: Newer bundled season on an existing configuration
- **WHEN** a configuration root holds only `seasons/2025.toml` and `seasons/2026.toml`, and the installed package ships `seasons/2027.toml`
- **THEN** a robot file for season 2027 that declares signals loads, and `doctor` lists `seasons/2027.toml` as a default

#### Scenario: Team's season file wins
- **WHEN** the configuration root's `seasons/2026.toml` sets a `robot` root and the bundled 2026 file has none
- **THEN** the 2026 season uses the root's `robot` root

### Requirement: Initialising a configuration
`flashpoint init` SHALL create the configuration root and write a starting configuration: the team number, the lake folder, and copies of the editable default files. Of the bundled season files it SHALL copy only those for the current season and earlier ones; a bundled file for a later season SHALL stay a default until the team copies it. It SHALL take the team number and lake folder from options, or ask for them when run in a terminal without them. It SHALL NOT overwrite an existing file: a second run SHALL list the files that already exist and change nothing. A team number SHALL be a whole number from 1 to 99999, and anything else SHALL be refused with a message, writing nothing. After creating a configuration it SHALL name the next step: import a log with `flashpoint ingest`, then draft the robot from it with Draft robot in the Setup view of `flashpoint serve`.

#### Scenario: First run
- **WHEN** `flashpoint init --team 254 --lake ~/frc-lake` runs with no configuration root
- **THEN** the configuration root exists with `acquire.toml` naming team 254 and the default files, the output's last line names `flashpoint ingest` and Draft robot, and `doctor` reports no problem, with the robot-season check a warning

#### Scenario: Init before the next season
- **WHEN** `flashpoint init --team 254` runs on 2026-12-15 with a package that ships season files for 2025, 2026 and 2027
- **THEN** the root holds `seasons/2025.toml` and `seasons/2026.toml` and no `seasons/2027.toml`, and season 2027 still loads from the bundled file

#### Scenario: Second run
- **WHEN** `flashpoint init --team 1678` runs where a configuration already exists
- **THEN** no file changes, and the command lists the existing files and says how to change the team number

#### Scenario: Bad team number
- **WHEN** `flashpoint init --team 0` runs
- **THEN** it exits non-zero with a message about the team number, and no file is written

### Requirement: Setup checks
Flashpoint SHALL run one set of setup checks, shown by `flashpoint doctor` and by the Setup view. Each check SHALL report ok, warning or problem, a message, and how to fix it: a fix action (see "Fix actions") or the exact command. The checks SHALL cover at least: the configuration root exists; each configuration file loads; a team is set; a robot is configured for the current season; the lake folder exists and can be written; the owlet tool for each Phoenix version in the lake is downloaded and its checksum matches; AdvantageScope is found; the acquire service is installed and running (on platforms with a service); sessions with hoots but no wpilog (see session-grouping, "Hoot-only sessions are reported"); and the Python version is supported (3.11 to 3.14). Derive SHALL warn, naming `flashpoint init`, when it maps sessions with no season configuration available (flashpoint-private#54). When a configuration exists but has no robot, derive's warning SHALL name Draft robot in the Setup view as the way to add one.

The robot-season check SHALL be a problem only when the lake holds sessions from the current season and no robot is configured for it. When the lake holds no session from the current season, it SHALL be a warning that says so. In both cases its fix SHALL name Draft robot in the Setup view first and the robot folder as the alternative. An unsupported Python SHALL be a problem whose fix is the documented install command. Where any check's or fix's text tells the user to install, upgrade or reinstall Flashpoint, it SHALL give the documented `uv tool` command first and the pipx command as the alternative.

#### Scenario: Missing configuration
- **WHEN** `doctor` runs with no configuration root
- **THEN** the configuration check reports a problem with the fix "Create configuration", and `doctor` exits non-zero

#### Scenario: Invalid robot file
- **WHEN** a robot file declares two slots with the same bus, model and CAN id
- **THEN** the robot check reports a problem naming the file and both slots, in `doctor` and in the Setup view

#### Scenario: Derive without configuration
- **WHEN** corpus `2026-gacmp-q7` is derived with an empty configuration root
- **THEN** derive succeeds and prints a warning naming `flashpoint init`

#### Scenario: New year, last year's logs
- **WHEN** the checks run with the current season 2027, a lake holding only corpus `2026-gacmp-q7`, and no robot for 2027
- **THEN** the robot-season check is a warning saying the lake has no 2027 logs yet and naming Draft robot, and it does not make `doctor` exit non-zero

#### Scenario: Current-season logs and no robot
- **WHEN** the checks run with the current season 2026, a lake holding corpus `2026-gacmp-q7`, and no robot file
- **THEN** the robot-season check is a problem naming Draft robot and the robot folder, and `doctor` exits non-zero

#### Scenario: Only a quarantined log this season
- **WHEN** the checks run with the current season 2026 and the lake's only file is the quarantined zero-byte corpus hoot `2026-gadal-q11-empty`
- **THEN** the robot-season check is a warning, since the lake has no 2026 session

#### Scenario: Unsupported Python
- **WHEN** the checks run for Python 3.10
- **THEN** the Python check is a problem naming the supported range 3.11 to 3.14, and its fix is the documented `uv tool install` command

#### Scenario: Hoot format newer than this release
- **WHEN** the lake holds a synthetic hoot whose format version no owlet in this release's manifest can read (as a 2027 hoot would be for 0.3.0)
- **THEN** the owlet check for that format is a problem whose fix is `uv tool upgrade flashpoint`, with `pipx upgrade flashpoint` as the alternative

#### Scenario: Derive with no robot
- **WHEN** corpus `2026-gacmp-q7` is derived with a configuration made by `init` and no robot file
- **THEN** derive succeeds and its warning names Draft robot in the Setup view
