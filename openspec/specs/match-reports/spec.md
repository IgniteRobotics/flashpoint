# match-reports Specification

## Purpose

Give students and drive coaches a per-match Replay they can open at an event with no internet, answering "which motor ran hot or drew the most, and when" in two clicks, inside one app shell that every Flashpoint view shares, and handing deep dives to AdvantageScope.

## Requirements

### Requirement: Shared app shell
All Flashpoint views SHALL share one shell: the same header, view navigation, colours, typography, spacing, and controls. Adding a view SHALL require registering it with the shell and SHALL NOT require restyling. The navigation SHALL list only views that are available in the current mode. Fonts, scripts, and styles SHALL ship with Flashpoint; no view SHALL request any external host. Interactive controls SHALL be real buttons, links, and inputs, reachable by keyboard, with a visible focus outline. Text SHALL meet a 4.5:1 contrast ratio against its background.

#### Scenario: Registered view appears
- **WHEN** a test view is registered with the shell
- **THEN** it appears in the navigation and renders with the shell's header, fonts, and colours, without its own stylesheet

#### Scenario: No external requests
- **WHEN** the app is used with networking disabled, and its files are scanned
- **THEN** every view renders, and no file references a URL with a remote host

#### Scenario: Keyboard only
- **WHEN** a user tabs through the Replay view
- **THEN** every match, marker, track control, and download is reachable, and each shows a visible focus outline

### Requirement: Replay data from the lake
A report command SHALL compute per-match Replay data from the lake's derived layers (silver, gold, and meta), one data file per match key. It SHALL NOT re-read raw logs to compute values. A build SHALL only rewrite the data of matches whose derived inputs changed since the last build, and SHALL update the match index without re-reading every match's data.

#### Scenario: Build Q7
- **WHEN** Replay data is built for a lake containing corpus `2026-gacmp-q7`
- **THEN** a data file for `2026gacmp_qm7` exists, and the match index lists it

#### Scenario: Incremental rebuild
- **WHEN** the data is built twice and a second match is ingested between the builds
- **THEN** the second build writes data only for the new match, and Q7's data file is unchanged byte-for-byte

#### Scenario: Selecting matches
- **WHEN** the report command is given an event or a list of match keys
- **THEN** only those matches are built or refreshed, and the index still lists every built match

### Requirement: Served and static modes
The app SHALL run in two modes:
- **served:** one command serves every view from a local lake over HTTP, bound to the local machine by default, with an option to share it on the pit network;
- **static:** an export command writes the Replay view and its data to a folder that opens by double-clicking its index page, with no server and no network.

Views that need the lake's query service (such as History) SHALL be absent from the navigation in static mode.

#### Scenario: Served
- **WHEN** the serve command is started on a corpus lake
- **THEN** Replay and History are reachable at the printed local address, and the command stops cleanly on interrupt

#### Scenario: Opened as a file
- **WHEN** a static export is opened directly from disk with networking disabled
- **THEN** the match list loads, a match's tracks render, and History is not in the navigation

#### Scenario: Served files stay inside the app
- **WHEN** the server receives a request for a path outside the app, data, or raw-download locations (such as `/../ledger.sqlite`)
- **THEN** it answers not-found, and no file outside those locations is returned

### Requirement: Payload budget with spike-preserving envelopes
Each match's Replay data SHALL be at most 2 MB uncompressed. Time series SHALL be reduced to envelopes of about 1000 buckets across the match window. Each bucket SHALL carry the minimum, maximum, and mean of the samples it covers. The true minimum and maximum of every series over the match window SHALL survive the reduction. Series that change rarely (such as temperature) MAY be stored as change points instead. Values SHALL be rounded to no fewer than 3 significant figures.

#### Scenario: Q7 under budget
- **WHEN** the Q7 Replay data is built
- **THEN** its size is at most 2 MB uncompressed

#### Scenario: Spike kept
- **WHEN** a synthetic series holds 10 A with a single 150 A sample lasting 4 ms
- **THEN** the reported maximum for that series is 150 A, and the bucket containing the spike shows a maximum of 150 A

#### Scenario: Envelope matches the samples
- **WHEN** any Q7 series is compared with its silver samples
- **THEN** each bucket's minimum and maximum equal the minimum and maximum of the silver samples in that bucket's time range

### Requirement: Replay view
The Replay view SHALL show:
- a **match list** grouped by event: each entry shows the match label, the robot, its marker counts by level, and a mark when alignment confidence is low;
- a **header** with the match key, robot, event, start time, duration, alignment confidence, and source log names;
- **tracks** over match time, with auto and teleop bands. By default these are the battery proxy (lowest device supply voltage), total supply current, the hottest slot's temperature, and the highest-current slot's current. The user MAY swap any track for any slot and metric. Each track shows its value at the cursor, its range, and a reference line where a limit is known;
- a **scrub cursor**, set by dragging or with the keyboard;
- a **readout** listing every slot's current, temperature, and status at the cursor. With no cursor placed, it lists each slot's match maximum, hottest first.

Missing data SHALL be shown as "not logged", never as zero. For a match without temperature (non-Pro hoots), the header SHALL say why.

#### Scenario: Hottest motor in two clicks
- **WHEN** a user opens the app and picks `2026gacmp_qm7` from the match list
- **THEN** the readout lists Q7's slots by maximum temperature, hottest first, with slots over the limit marked

#### Scenario: Scrub
- **WHEN** the user moves the cursor to T+90 s
- **THEN** every track shows its value at T+90 s, and the readout lists each slot's current and temperature at that time

#### Scenario: Temperature not available
- **WHEN** a match's hoots are not Pro-licensed
- **THEN** temperature tracks and readout cells read "not logged", and the header says the hoots carry no temperature

### Requirement: Rule-based event markers
The system SHALL place markers on the Replay timeline from fixed, configurable rules, each with a level (WARN or FAULT), a source slot or `power`, a time, and a one-line message:
- temperature reaching the warning limit (default 65 °C, WARN) or the fault limit (default 75 °C, FAULT);
- the battery proxy dropping below the brownout threshold (default 6.75 V, FAULT) or a sag threshold (default 8.0 V, WARN);
- a stall interval as defined by motor physics (WARN);
- a gap in a slot's samples longer than 1 s during the match (WARN).

Selecting a marker SHALL move the cursor to its time and show its details. Markers SHALL NOT claim a cause or a fix.

#### Scenario: Temperature marker
- **WHEN** a synthetic slot's temperature reaches 66 °C at T+108 s
- **THEN** a WARN marker for that slot sits at T+108 s with the message "Temperature 66 °C"

#### Scenario: Brownout marker
- **WHEN** every device's supply voltage drops to 6.5 V for 0.2 s at T+97 s
- **THEN** a FAULT marker for `power` sits at T+97 s

#### Scenario: Quiet match
- **WHEN** no rule fires for a match
- **THEN** the timeline shows no markers, and the match list shows zero counts

### Requirement: Compare overlay
The user SHALL be able to overlay a second match on every track, aligned on match time and drawn in a distinct style. The readout SHALL show both matches' values for each slot. Slots present in only one match SHALL be marked absent on the other side.

#### Scenario: Overlay E10 on Q7
- **WHEN** the user overlays E10 while viewing Q7
- **THEN** each track shows both matches, and slots missing from E10 read "absent" in its column

### Requirement: Shareable view links
The app SHALL encode the view, selected match, overlay match, cursor time, and track choices in the page address. Opening a copied address SHALL restore the same view, in both served and static modes.

#### Scenario: Restore from link
- **WHEN** a user copies the address while viewing Q7 with E10 overlaid and the cursor at T+90 s, then opens it in a new window
- **THEN** the same match, overlay, and cursor are shown

### Requirement: Open in AdvantageScope
Each match SHALL offer its raw wpilog and hoots as downloads, named so their match is recognisable, for opening in AdvantageScope. In served mode on the local machine, each match SHALL also offer one "Open in AdvantageScope" action that opens the match's logs in place (see `advantagescope-launch`). After the action, the page SHALL show the staged folder and the hoots to add with File › Insert log. When the launch is unavailable, the page SHALL show why and keep the downloads. The app SHALL NOT launch any application except through that guarded launch. Raw files SHALL NOT count toward the payload budget. A static export made without raw files SHALL say the files are not included and name the lake hash of each.

#### Scenario: Download Q7 logs
- **WHEN** the user downloads the raw logs of Q7
- **THEN** the Q7 wpilog and its rio and CANivore hoots download with names containing `2026gacmp_qm7`, and their hashes equal the ledger hashes

#### Scenario: Launch from Replay
- **WHEN** the user chooses "Open in AdvantageScope" on Q15 in a locally served app, with a stand-in program configured as AdvantageScope
- **THEN** the stand-in is started on the staged Q15 wpilog, and the page names the staged folder and lists the two hoots to insert

#### Scenario: Launch unavailable
- **WHEN** Replay is served on the pit network
- **THEN** no launch action is shown, the page says the launch is only available on the machine running Flashpoint, and the downloads work

#### Scenario: Static export
- **WHEN** a static export is opened from disk
- **THEN** no launch action is shown, and the downloads work

#### Scenario: Exported without raw files
- **WHEN** a static export is made with raw files excluded
- **THEN** the download area shows "not included" with each file's hash, and the rest of Replay works

### Requirement: Content is escaped
Every value from logs or configuration (match keys, event names, slot roles, file names, serials) SHALL be rendered as text, never as markup, script, or event-handler code.

#### Scenario: Hostile names
- **WHEN** a robot configuration names a slot role `<img src=x onerror=alert(1)>` and a log file is named `x');alert(1);//.wpilog`
- **THEN** the app shows both literally, and no script runs

### Requirement: Partial and unmatched sessions
Matches that cannot be fully shown SHALL still appear in the match list with a reason; they SHALL never be silently dropped:
- a match with low alignment confidence SHALL carry a visible warning;
- a session with no derived samples (for example a hoot-only session) SHALL be listed with "no aligned samples" and its raw-log downloads only;
- a session with no match key SHALL NOT get a Replay entry, and the build summary SHALL count it.

#### Scenario: Low-confidence match
- **WHEN** a session aligned by enable edges is shown
- **THEN** its Replay header shows a low-alignment warning, and its match-list entry is marked

#### Scenario: Hoot-only session
- **WHEN** the lake holds a match-keyed session with hoots but no wpilog
- **THEN** the match list shows it with "no aligned samples", and selecting it offers only the raw downloads

### Requirement: Build budget
Building one match's Replay data SHALL take under 5 seconds, with peak memory under 500 MB on the reference laptop, reading only that match's partitions.

#### Scenario: Q7 build budget
- **WHEN** the Q7 Replay data is built from an existing lake
- **THEN** wall time is under 5 s, and peak memory is under 500 MB
