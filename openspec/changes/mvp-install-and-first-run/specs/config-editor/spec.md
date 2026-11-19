## MODIFIED Requirements

### Requirement: Draft a robot from a log
The Setup view SHALL start a new robot file from an imported session. The draft SHALL list every device on the session's hoots with its bus, model and CAN id, the serial number when the session's wpilog has a CAN inventory, and the robot code's bus names from the bus-status entries when logged. It SHALL take the project name and season from the session. Slot ids SHALL be unique within a draft: when the same model and CAN id appear on two buses, each of those slots' ids SHALL name its bus. The user can change each slot's id and fill in its subsystem and role before saving; subsystem and role may be left empty, and the Setup view SHALL mark slots with no subsystem as unnamed. The save SHALL follow "Editing and saving configuration". A draft whose robot id already exists SHALL be refused. The same draft is what derive saves on its own for a season with no robot file (see team-setup, "First derive drafts the robot").

After a draft is saved as a new robot file, the server SHALL start `flashpoint derive` for its lake in the background, as a separate process, and the view SHALL say that the lake is being rebuilt and to reload Replay when it finishes. When a derive or Replay build is already running for the lake, it SHALL say so and start nothing.

When the lake holds no imported session, the Draft robot tab SHALL say so and SHALL give:
- the exact import command for the lake this server uses, ready to copy, with the lake path quoted for the platform the server runs on;
- one line on what the folder holds: one match's `.wpilog` and `.hoot` files, from the robot's USB stick, from `flashpoint acquire`, or downloaded;
- that the tab is ready once the import finishes and the page is reloaded;
- a pointer to the import section of the guide (see team-setup, "Documentation links").

#### Scenario: Draft from Q7
- **WHEN** a robot is drafted from corpus `2026-gacmp-q7`
- **THEN** the draft holds 22 devices across the rio and CANivore buses, with project `Robot-2026` and season 2026

#### Scenario: Same CAN id on two buses
- **WHEN** a robot is drafted from a synthetic session whose hoots show a TalonFX with CAN id 1 on the rio and another on the CANivore
- **THEN** the draft's two slot ids differ and each names its bus, and the draft loads as a valid robot

#### Scenario: Save starts a rebuild
- **WHEN** a draft from Q7 is saved in served mode on a lake where Q7 shows "no aligned samples"
- **THEN** the view says the lake is being rebuilt, a background derive runs, and after it finishes Q7's Replay data has status `ok`

#### Scenario: Rebuild already running
- **WHEN** a draft is saved while `flashpoint report` holds the lake's report lock
- **THEN** the robot file is saved, the view says a rebuild is already running, and no second derive starts

#### Scenario: Log without hoots
- **WHEN** a robot is drafted from corpus `2026-gacmp-p2` (a wpilog with no hoots)
- **THEN** the view says the session has no CAN devices to draft from, and no file is written

#### Scenario: Existing robot id
- **WHEN** a draft is saved as `2026-comp`, which already exists
- **THEN** the save is refused naming `2026-comp`, and the existing file is unchanged

#### Scenario: Empty lake
- **WHEN** the Draft robot tab is opened on a served empty lake
- **THEN** it shows `flashpoint ingest --lake <this lake> <folder>` with this server's lake path, says what the folder holds and to reload after the import, and points to the import guide

#### Scenario: Lake path with a space
- **WHEN** the server's lake is `C:\Users\Team 254\flashpoint-lake` on Windows, or `/Users/team 254/flashpoint-lake` on macOS, and the lake is empty
- **THEN** the command shown quotes the lake path so that pasting it into that platform's default shell passes it as one argument

#### Scenario: Only quarantined files
- **WHEN** the lake's only file is the quarantined zero-byte corpus hoot `2026-gadal-q11-empty`
- **THEN** the tab shows the empty-lake guidance, and says one file was quarantined and to run `flashpoint doctor`
