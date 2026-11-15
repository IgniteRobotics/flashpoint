## MODIFIED Requirements

### Requirement: Draft a robot from a log
The Setup view SHALL start a new robot file from an imported session. The draft SHALL list every device on the session's hoots with its bus, model and CAN id, the serial number when the session's wpilog has a CAN inventory, and the robot code's bus names from the bus-status entries when logged. It SHALL take the project name and season from the session. The user SHALL complete each slot's id, subsystem and role before saving, and the save SHALL follow "Editing and saving configuration". A draft whose robot id already exists SHALL be refused.

When the lake holds no imported session, the Draft robot tab SHALL say so and SHALL give:
- the exact import command for the lake this server uses, ready to copy, with the lake path quoted for the platform the server runs on;
- one line on what the folder holds: one match's `.wpilog` and `.hoot` files, from the robot's USB stick, from `flashpoint acquire`, or downloaded;
- that the tab is ready once the import finishes and the page is reloaded;
- a pointer to the import section of the guide (see team-setup, "Documentation links").

#### Scenario: Draft from Q7
- **WHEN** a robot is drafted from corpus `2026-gacmp-q7`
- **THEN** the draft holds 22 devices across the rio and CANivore buses, with project `Robot-2026` and season 2026

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
