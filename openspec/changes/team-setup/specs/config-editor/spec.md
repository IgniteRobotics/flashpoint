## Purpose

Show a team its whole Flashpoint configuration in the app and let it change that configuration (team and connection, robots and slots, thresholds, units) without editing files by hand, with the same validation as loading.

## ADDED Requirements

### Requirement: Configuration shown in the Setup view
The Setup view SHALL show the configuration in sections: team and connection (team number, robot addresses, lake folder, acquire settings), robots (each robot with its slots, buses and signals), thresholds (report and anomaly settings), and units. Each section SHALL name the file it comes from and whether that file is a bundled default. A file that fails to load SHALL be shown with the loading error in place of its contents. The view SHALL be present only in served mode.

#### Scenario: Ignite configuration
- **WHEN** the Setup view opens with the 2026 competition robot configured
- **THEN** it lists 23 slots for `2026-comp`, its buses `rio` and `DriveTrain`, and its declared signals, and names `robots/2026-comp.toml`

#### Scenario: Broken file
- **WHEN** a robot file has an unknown key
- **THEN** that robot's section shows the loading error naming the file and the key, and the other sections still show

### Requirement: Editing and saving configuration
On the machine running `serve`, the Setup view SHALL let the user add, change and remove: the team number, robot addresses, lake folder and acquire settings; robots, slots, buses and signals; report and anomaly thresholds; and units. A save SHALL pass the same guard as fix actions, and:
- SHALL be validated with the same rules as loading, and refused, writing nothing, with the message loading would give;
- SHALL be refused when the file changed on disk since the view read it, naming the file;
- SHALL be written atomically, keeping the file's comments and the order of entries the edit didn't touch;
- SHALL keep the previous version of the file in a backups folder in the configuration root, holding the last 20 versions of each file.

When the app is shared on the network or opened as a static export, the configuration SHALL be read-only.

#### Scenario: Valid slot edit
- **WHEN** the user changes `drive-fl`'s CAN id from 11 to 21 and saves
- **THEN** `robots/2026-comp.toml` holds CAN id 21, its comments are unchanged, and the previous version is in the backups folder

#### Scenario: Duplicate slot refused
- **WHEN** the user changes `steer-fl`'s CAN id to 11, which `drive-fl` already uses on the same bus and model, and saves
- **THEN** the save is refused with a message naming both slots, and the file is unchanged

#### Scenario: Changed on disk
- **WHEN** the view has loaded `report.toml`, the file is then edited by hand, and the user saves a threshold change
- **THEN** the save is refused naming `report.toml`, and the hand edit is kept

#### Scenario: Read-only when shared
- **WHEN** the app is served with `--host 0.0.0.0` and the Setup view is opened from another machine
- **THEN** the configuration is shown with no edit controls, and a save request from that machine is refused

### Requirement: What an edit affects
After a save, the Setup view SHALL say what the change affects and what to run: a robot, slot, bus or signal change SHALL name `flashpoint derive`; an anomaly threshold change SHALL name `flashpoint detect`; a report threshold change SHALL name `flashpoint report`; an acquire setting change SHALL say that a running acquire service picks it up after a restart. Saving SHALL NOT change any lake data.

#### Scenario: Slot change
- **WHEN** a slot's role is changed and saved
- **THEN** the view says matches must be re-derived and names `flashpoint derive`, and no lake file changes

### Requirement: Draft a robot from a log
The Setup view SHALL start a new robot file from an imported session. The draft SHALL list every device on the session's hoots with its bus, model and CAN id, the serial number when the session's wpilog has a CAN inventory, and the robot code's bus names from the bus-status entries when logged. It SHALL take the project name and season from the session. The user SHALL complete each slot's id, subsystem and role before saving, and the save SHALL follow "Editing and saving configuration". A draft whose robot id already exists SHALL be refused.

#### Scenario: Draft from Q7
- **WHEN** a robot is drafted from corpus `2026-gacmp-q7`
- **THEN** the draft holds 22 devices across the rio and CANivore buses, with project `Robot-2026` and season 2026

#### Scenario: Log without hoots
- **WHEN** a robot is drafted from corpus `2026-gacmp-p2` (a wpilog with no hoots)
- **THEN** the view says the session has no CAN devices to draft from, and no file is written

#### Scenario: Existing robot id
- **WHEN** a draft is saved as `2026-comp`, which already exists
- **THEN** the save is refused naming `2026-comp`, and the existing file is unchanged

### Requirement: Units list
The Setup view SHALL show the units list with its columns (serial, model, robot, bus, CAN id, role, label, first installed, notes) and let the user add, change and remove units. A save SHALL refuse a duplicate serial number or a reused label, naming the rows.

#### Scenario: Duplicate serial
- **WHEN** the user adds a unit whose serial is already listed and saves
- **THEN** the save is refused naming both rows, and the units file is unchanged
