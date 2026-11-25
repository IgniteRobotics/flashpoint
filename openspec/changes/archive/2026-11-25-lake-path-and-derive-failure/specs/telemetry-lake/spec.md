## MODIFIED Requirements

### Requirement: Lake location and portability
The lake root SHALL be configurable (an environment variable or CLI flag, with a documented default). A lake directory copied to another machine SHALL be queryable there without re-ingesting.

Every command SHALL turn the lake root into an absolute folder before it reads or writes the lake. A leading `~` SHALL be expanded from every source. A relative lake from any source (`--lake`, the lake environment variable, or `lake` in the configuration file) SHALL be taken from the folder the command runs in. `flashpoint init` SHALL write the lake into the configuration file as an absolute folder. `doctor` and every message that names the lake SHALL show the absolute folder.

#### Scenario: Copy to another laptop
- **WHEN** the lake directory is copied to a different machine with Flashpoint installed
- **THEN** the same queries return the same results

#### Scenario: Relative --lake on a corpus match
- **WHEN** `flashpoint ingest --lake lake .` runs in a folder holding corpus group `2026-gacmp-q7`
- **THEN** the files are ingested into `./lake`, derive and detection run without error, Replay data is built for the match, and the command exits 0

#### Scenario: Relative lake from the environment
- **WHEN** the lake environment variable is `lake` and `flashpoint doctor` runs in folder `/work`
- **THEN** `doctor` reports the lake as `/work/lake`

#### Scenario: Relative lake in the configuration file
- **WHEN** the configuration file in configuration folder `/cfg` says `lake = "lake"`, and a command runs in folder `/work`
- **THEN** the command uses `/work/lake`

#### Scenario: Home folder in a quoted path
- **WHEN** `flashpoint doctor --lake "~/frc-lake"` runs with the `~` not expanded by the shell
- **THEN** `doctor` reports the lake as the `frc-lake` folder in the user's home folder

#### Scenario: Init stores an absolute lake
- **WHEN** `flashpoint init --team 254 --lake lake` runs in folder `/work` with no configuration
- **THEN** the configuration file names the lake as `/work/lake`

#### Scenario: Server with a relative lake
- **WHEN** `flashpoint serve --lake lake` runs on a derived lake and the anomalies view is opened
- **THEN** the view loads without error
