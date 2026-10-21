## Purpose

Let any FRC team install Flashpoint from a public package, create its own configuration, point it at its own robot, and see and fix setup problems without editing code or knowing the internals.

## ADDED Requirements

### Requirement: Installable public package
Flashpoint SHALL be published as a package that installs with `pipx install flashpoint` on macOS, Windows and Linux, with Python 3.11 to 3.13. The installed package SHALL include the default configuration (season roots that every WPILib robot shares, report and anomaly thresholds, anomaly cause text) and SHALL NOT include any team's robot files, units list or logs. Each release SHALL publish its version, and `flashpoint --version` SHALL print it.

#### Scenario: Clean install
- **WHEN** the built package is installed into an empty environment and `flashpoint --version` and `flashpoint doctor` are run with no configuration
- **THEN** both exit, the version matches the package version, and `doctor` reports that no configuration exists and names `flashpoint init`

#### Scenario: No team data in the package
- **WHEN** the built package's files are listed
- **THEN** it holds the default configuration files and no robot file, units list, `.wpilog` or `.hoot`

### Requirement: Configuration home
The configuration root SHALL be, in order: a path given on the command line, then `FLASHPOINT_CONFIG`, then the per-user configuration folder for the platform (`~/.config/flashpoint` on Linux and macOS, `%APPDATA%\flashpoint` on Windows). Flashpoint SHALL NOT fall back to a folder inside a source checkout. Where a file is missing from the configuration root, the bundled default SHALL be used, and `doctor` and the Setup view SHALL say which files come from defaults.

#### Scenario: Per-user folder
- **WHEN** `FLASHPOINT_CONFIG` is unset and the per-user folder holds `acquire.toml` with team 254
- **THEN** `doctor` shows the per-user folder as the configuration root and team 254

#### Scenario: Default thresholds
- **WHEN** the configuration root has no `report.toml`
- **THEN** the bundled report thresholds are used, and `doctor` lists `report.toml` as a default

### Requirement: Initialising a configuration
`flashpoint init` SHALL create the configuration root and write a starting configuration: the team number, the lake folder, and copies of the editable default files. It SHALL take the team number and lake folder from options, or ask for them when run in a terminal without them. It SHALL NOT overwrite an existing file: a second run SHALL list the files that already exist and change nothing. A team number SHALL be a whole number from 1 to 99999, and anything else SHALL be refused with a message, writing nothing.

#### Scenario: First run
- **WHEN** `flashpoint init --team 254 --lake ~/frc-lake` runs with no configuration root
- **THEN** the configuration root exists with `acquire.toml` naming team 254 and the default files, and `doctor` reports no configuration problem except that no robot is configured

#### Scenario: Second run
- **WHEN** `flashpoint init --team 1678` runs where a configuration already exists
- **THEN** no file changes, and the command lists the existing files and says how to change the team number

#### Scenario: Bad team number
- **WHEN** `flashpoint init --team 0` runs
- **THEN** it exits non-zero with a message about the team number, and no file is written

### Requirement: Robot addresses from the team number
When `acquire.toml` names a team and no hosts, acquire SHALL try, in order, `10.TE.AM.2` (the team number split as the last two digits and the rest), `roborio-<team>-frc.local`, and the USB address `172.22.11.2`. Hosts named in `acquire.toml` SHALL replace these. With no team and no hosts, acquire SHALL refuse to start with a message naming `flashpoint init`, and transfer nothing.

#### Scenario: Four-digit team
- **WHEN** acquire runs for team 6829 with no hosts configured
- **THEN** it tries `10.68.29.2`, `roborio-6829-frc.local` and `172.22.11.2`, in that order

#### Scenario: Short and long team numbers
- **WHEN** the addresses are worked out for teams 254 and 12345
- **THEN** they start with `10.2.54.2` and `10.123.45.2`

#### Scenario: No team
- **WHEN** acquire runs with no team and no hosts configured
- **THEN** it exits non-zero naming `flashpoint init`, and nothing is transferred

### Requirement: Setup checks
Flashpoint SHALL run one set of setup checks, shown by `flashpoint doctor` and by the Setup view. Each check SHALL report ok, warning or problem, a message, and how to fix it: a fix action (see "Fix actions") or the exact command. The checks SHALL cover at least: the configuration root exists; each configuration file loads; a team is set; a robot is configured for the current season; the lake folder exists and can be written; the owlet tool for each Phoenix version in the lake is downloaded and its checksum matches; AdvantageScope is found; the acquire service is installed and running (on platforms with a service); and the Python version is supported. Derive SHALL warn, naming `flashpoint init`, when it maps sessions with no season configuration available (#54).

#### Scenario: Missing configuration
- **WHEN** `doctor` runs with no configuration root
- **THEN** the configuration check reports a problem with the fix "Create configuration", and `doctor` exits non-zero

#### Scenario: Invalid robot file
- **WHEN** a robot file declares two slots with the same bus, model and CAN id
- **THEN** the robot check reports a problem naming the file and both slots, in `doctor` and in the Setup view

#### Scenario: Derive without configuration
- **WHEN** corpus `2026-gacmp-q7` is derived with an empty configuration root
- **THEN** derive succeeds and prints a warning naming `flashpoint init`

### Requirement: Fix actions
The Setup view SHALL offer a fix button for these problems: create the configuration (as `init`, with the team and lake entered in the view); download and verify owlet; set the AdvantageScope path; install the acquire service; start the acquire service. A fix SHALL run only when the server is bound to this machine and the request passes the same guard as triage writes (a loopback client, `Host` and `Origin` naming this machine, a JSON body, and the `X-Flashpoint` header). When the app is shared on the network or opened as a static export, the view SHALL show the command instead of a button. Each fix SHALL be safe to run twice, SHALL show its result, and SHALL re-run the checks afterwards. Problems with no fix action SHALL show the exact command.

#### Scenario: Download owlet from the view
- **WHEN** the owlet check reports a missing binary and the user presses its fix on the local machine
- **THEN** owlet is downloaded and its checksum verified, and the check then reports ok

#### Scenario: Shared on the network
- **WHEN** the app is served with `--host 0.0.0.0` and opened from another machine
- **THEN** the Setup view shows each problem's command and no fix button, and a fix request from that machine is refused

#### Scenario: Request without the guard
- **WHEN** a fix request arrives without the `X-Flashpoint` header
- **THEN** it is refused and nothing runs
