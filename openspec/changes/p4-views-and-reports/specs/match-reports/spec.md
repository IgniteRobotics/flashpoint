## Purpose

Give students and drive coaches a per-match report they can open at an event with no internet and no server, answering "which motor ran hot or drew the most" in two clicks, and handing deep dives to AdvantageScope.

## ADDED Requirements

### Requirement: Report build from the lake
A report command SHALL build a static report folder from the lake's derived layers (silver, gold, and meta), with one page of data per match key. It SHALL NOT re-read raw logs to compute values. A build SHALL only rewrite the data of matches whose derived inputs changed since the last build, and SHALL update the match index without re-reading every match's data.

#### Scenario: Build Q7
- **WHEN** the report is built for a lake containing corpus `2026-gacmp-q7`
- **THEN** the report folder holds an index page, the Q7 match data, and the vendored scripts, and the index lists `2026gacmp_qm7`

#### Scenario: Incremental rebuild
- **WHEN** the report is built twice and a second match is ingested between the builds
- **THEN** the second build writes data only for the new match, and Q7's data file is unchanged byte-for-byte

#### Scenario: Selecting matches
- **WHEN** the report command is given an event or a list of match keys
- **THEN** only those matches are built or refreshed, and the index still lists every built match

### Requirement: Works from a folder, offline
The report SHALL open by double-clicking its index page from a local folder, with no network access and no server. All scripts and styles SHALL ship inside the folder; the report SHALL make no request to any external host. A bundled serve command SHALL also serve the folder over local HTTP for sharing on a pit network.

#### Scenario: Opened as a file
- **WHEN** the index page is opened directly from disk with networking disabled
- **THEN** the match list loads and a match's charts render

#### Scenario: No external requests
- **WHEN** the report's files are scanned
- **THEN** none references a URL with a remote host

#### Scenario: Served
- **WHEN** the serve command is started on the report folder
- **THEN** the report is reachable at the printed local address, and the command stops cleanly on interrupt

### Requirement: Payload budget with spike-preserving envelopes
Each match's data SHALL be at most 2 MB uncompressed. Time series SHALL be reduced to envelopes of about 1000 buckets across the match window, each bucket carrying the minimum, maximum, and mean of the samples it covers. The true minimum and maximum of every series over the match window SHALL survive the reduction. Series that change rarely (such as temperature) MAY be stored as change points instead. Values SHALL be rounded to no fewer than 3 significant figures.

#### Scenario: Q7 under budget
- **WHEN** the Q7 report data is built
- **THEN** its size is at most 2 MB uncompressed

#### Scenario: Spike kept
- **WHEN** a synthetic series holds 10 A with a single 150 A sample lasting 4 ms
- **THEN** the reported maximum for that series is 150 A, and the bucket containing the spike shows a maximum of 150 A

#### Scenario: Envelope matches the samples
- **WHEN** any Q7 series is compared with its silver samples
- **THEN** each bucket's minimum and maximum equal the minimum and maximum of the silver samples in that bucket's time range

### Requirement: Match page contents
For each match, the report SHALL show:
- a header with the match key, robot, event, match start time, duration, alignment confidence, and source log names;
- a per-slot table from the feature table: role, subsystem, unit, supply and stator current (mean and P95), maximum temperature, temperature rise rate, supply and motor energy, stall time, and residual P95, for the selected phase (auto, teleop, or match);
- per-slot charts of current, temperature, velocity, and motor voltage over match time, with phase boundaries marked;
- an all-slots heatmap of current and of power over match time;
- total robot supply power and energy over match time.

Slots whose maximum or mean temperature exceeds configurable limits (default 65 °C maximum, 55 °C mean) SHALL be highlighted. Missing data SHALL be shown as "not logged", never as zero.

#### Scenario: Hot motor visible in two clicks
- **WHEN** a user opens the index and picks `2026gacmp_qm7`
- **THEN** the first view is the per-slot table, sorted to show the hottest slot first, with any slot over the limits highlighted

#### Scenario: Phase switch
- **WHEN** the user selects the `auto` phase
- **THEN** the table shows the auto feature rows for each slot

#### Scenario: Temperature not available
- **WHEN** a match's hoots are not Pro-licensed, so no temperature was logged
- **THEN** temperature columns and charts read "not logged" for that match, and the header says why

### Requirement: Compare two matches
The report SHALL let the user compare two matches side by side, with time series aligned on match time and the per-slot tables shown together, matched by slot. Slots present in only one match SHALL be shown with the other side marked absent.

#### Scenario: Compare Q7 with E10
- **WHEN** the user adds Q7 and E10 to the comparison
- **THEN** both matches' tables and charts show side by side, and slots missing from one match are marked absent

### Requirement: Shareable view links
The report SHALL encode the selected matches, phase, and view in the page address, so that opening a copied address restores the same view.

#### Scenario: Restore from link
- **WHEN** a user copies the address while comparing Q7 and E10 on the auto phase and opens it in a new window
- **THEN** the same comparison, phase, and view are shown

### Requirement: Open in AdvantageScope
Each match page SHALL offer the match's raw wpilog and hoots as downloads, named so their match is recognisable, for opening in AdvantageScope. The report SHALL never launch any application. Raw files SHALL NOT count toward the payload budget. When the report is built without raw files, the download area SHALL say the files are not included and name the lake hash of each.

#### Scenario: Download Q7 logs
- **WHEN** the user opens the Q7 page and chooses "Open in AdvantageScope"
- **THEN** the Q7 wpilog and its rio and CANivore hoots download with names containing `2026gacmp_qm7`, and their hashes equal the ledger hashes

#### Scenario: Built without raw files
- **WHEN** the report is built with raw files excluded
- **THEN** the download area shows "not included" with each file's hash, and the rest of the page works

### Requirement: Content is escaped
Every value from logs or configuration (match keys, event names, slot roles, file names, serials) SHALL be rendered as text, never as markup, script, or event handler code.

#### Scenario: Hostile names
- **WHEN** a robot configuration names a slot role `<img src=x onerror=alert(1)>` and a log file is named `x');alert(1);//.wpilog`
- **THEN** the report shows both literally, and no script runs

### Requirement: Partial and unmatched sessions
Matches that cannot be fully reported SHALL still appear in the index with a reason, never be silently dropped:
- a match with low alignment confidence SHALL carry a visible warning on its page;
- a session with no derived samples (for example a hoot-only session) SHALL be listed with "no aligned samples" and its raw-log downloads only;
- a session with no match key SHALL NOT get a match page, and the build summary SHALL count it.

#### Scenario: Low-confidence match
- **WHEN** a session aligned by enable edges is reported
- **THEN** its page shows a low-alignment warning, and its index entry is marked

#### Scenario: Hoot-only session
- **WHEN** the lake holds a match-keyed session with hoots but no wpilog
- **THEN** the index lists it with "no aligned samples", and its page offers only the raw downloads

### Requirement: Build budget
Building one match's report data SHALL take under 5 seconds with peak memory under 500 MB on the reference laptop, reading only that match's partitions.

#### Scenario: Q7 build budget
- **WHEN** the Q7 report data is built from an existing lake
- **THEN** wall time is under 5 s and peak memory is under 500 MB
