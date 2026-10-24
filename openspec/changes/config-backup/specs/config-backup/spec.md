## Purpose

Back up the team's configuration (robot files, unit registry, seasons, thresholds) with the lake, and bring it back on restore without ever overwriting configuration that is already there, so a lost laptop loses neither the matches nor the mapping needed to read them.

## ADDED Requirements

### Requirement: Backup includes configuration files
Every backup SHALL copy the configuration folder's configuration files to the remote, beside the raw files and metadata. Configuration files are TOML files at the top of the configuration folder and in its `seasons/` and `robots/` folders, and CSV files in its `units/` folder. Backup SHALL NOT copy any other file: not the service environment file, hidden files or folders (including the editor's local save history), editor temporary files, or files in other folders. Bundled defaults that the team has not copied into its folder SHALL NOT be backed up. A configuration folder that is missing or holds no configuration files SHALL NOT fail the backup; raw files and metadata are still backed up and the result says no configuration was found.

#### Scenario: Robot and units backed up
- **WHEN** backup runs with a configuration folder holding `acquire.toml`, `robots/2026-comp.toml`, `seasons/2026.toml` and `units/units-seed.csv`
- **THEN** the remote's latest configuration holds exactly those four files with identical contents

#### Scenario: Other files left out
- **WHEN** the configuration folder also holds `env`, `.backups/robots/2026-comp.toml.1`, `.2026-comp.toml.abc.tmp`, `notes.txt` and `robots/old/2025-comp.toml`
- **THEN** none of them is on the remote

#### Scenario: No configuration folder
- **WHEN** backup runs and the configuration folder does not exist
- **THEN** raw files and metadata are backed up, the backup succeeds, and its summary says no configuration files were found

### Requirement: Configuration history on the remote
The remote SHALL hold the latest configuration as one folder. When a backup changes or removes a file there, the previous version SHALL be kept in a dated history folder on the remote. Nothing SHALL remove history automatically.

#### Scenario: Edited robot keeps the old version
- **WHEN** a robot file is backed up, then edited, then backed up again
- **THEN** the latest configuration holds the edited file, and a history folder holds the earlier one

#### Scenario: Deleted robot keeps the old version
- **WHEN** a robot file is backed up, then deleted locally, then backed up again
- **THEN** the latest configuration no longer holds it, and a history folder does

### Requirement: Configuration change makes a backup due
In watch mode, a backup SHALL be owed when the configuration files differ from those last backed up successfully, as well as after a cycle that changed raw files or metadata. The usual interval between backups SHALL still apply. A configuration folder that cannot be read SHALL be recorded as a backup error and SHALL NOT stop acquisition or ingest.

#### Scenario: Robot edited on a quiet day
- **WHEN** the watch is running, no new logs arrive, and a robot file is saved from the Setup view
- **THEN** a backup runs within one backup interval, and the remote holds the saved file

#### Scenario: Nothing changed
- **WHEN** a cycle pulls nothing, ingests nothing, and the configuration matches the last backup
- **THEN** no backup runs

### Requirement: Restore never overwrites configuration
Restore SHALL bring back the backed-up configuration files. When the target configuration folder is missing or holds no configuration files, restore SHALL copy them into it. When it already holds any configuration file, restore SHALL leave it unchanged, put the backed-up copy in a `restored-config` folder inside the target lake, and say where it is. A remote with no configuration (made before configuration was backed up) SHALL restore raw files and metadata as before, and say that it held no configuration. Restore's force option SHALL NOT override this rule.

#### Scenario: Restore onto a new laptop
- **WHEN** restore runs into a new lake with an empty configuration folder, from a remote holding a robot file and the unit registry
- **THEN** both files are in the configuration folder with their backed-up contents, and the restored lake rebuilds with them

#### Scenario: Existing configuration kept
- **WHEN** restore runs with a configuration folder that already holds `acquire.toml`
- **THEN** the configuration folder is unchanged, the backed-up files are in the lake's `restored-config` folder, and restore's output names that folder

#### Scenario: Forced restore keeps configuration
- **WHEN** restore runs with force over a lake that has a ledger, and the configuration folder holds a robot file
- **THEN** the configuration folder is unchanged

#### Scenario: Backup from before configuration was included
- **WHEN** restore runs from a remote that has raw files and metadata but no configuration
- **THEN** restore succeeds, the configuration folder is unchanged, and the output says the backup held no configuration
