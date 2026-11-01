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
- **tracks** over match time, with auto and teleop bands. By default these are the battery proxy (as defined under rule-based event markers), total supply current, the hottest slot's temperature, and the highest-current slot's current. The user MAY swap any track for any slot and metric. Each track shows its value at the cursor, its range, and a reference line where a limit is known;
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
The system SHALL place markers on the Replay timeline from fixed, configurable rules, each with a level (WARN or FAULT), a source slot, `power`, or a bus, a time, and a one-line message:
- temperature reaching the warning limit (default 65 °C, WARN) or the fault limit (default 75 °C, FAULT);
- the battery proxy dropping below the brownout threshold (default 6.75 V, held at least 0.02 s, FAULT) or a sag threshold (default 8.0 V, WARN);
- a stall interval as defined by motor physics (WARN);
- a device reboot event during the match (FAULT, on the device's slot), as defined by CAN health;
- an isolated frame gap of at least the dropout threshold during the match (default 0.25 s, WARN, on the device's slot), as defined by CAN health;
- a bus-wide frame gap of at least the bus-silence threshold during the match (default 1 s, WARN, on the bus).

The battery proxy SHALL be the supply voltage of the robot's declared battery-proxy device. When the robot declares none, or that device has no supply voltage in the match, the proxy SHALL be the median supply voltage across the devices logged at each moment. A power marker's message SHALL name the proxy it used. A sample SHALL count as below a threshold only until the next sample from the same device, and never for longer than a gap limit (default 0.1 s), so a logging gap cannot stretch a dip into a brownout. A single motor's supply dip SHALL NOT produce a power marker unless the proxy itself crosses the threshold.

Markers SHALL be read from the lake's derived layers, never computed from raw logs. Bus-wide gaps shorter than the bus-silence threshold SHALL NOT produce markers. A device with no configured slot SHALL NOT get a reboot or dropout marker. Selecting a marker SHALL move the cursor to its time and show its details. Markers SHALL NOT claim a cause or a fix.

#### Scenario: Temperature marker
- **WHEN** a synthetic slot's temperature reaches 66 °C at T+108 s
- **THEN** a WARN marker for that slot sits at T+108 s with the message "Temperature 66 °C"

#### Scenario: Brownout marker
- **WHEN** every device's supply voltage, including the declared proxy's, drops to 6.5 V for 0.2 s at T+97 s
- **THEN** a FAULT marker for `power` sits at T+97 s, and its message names the gyro as the proxy

#### Scenario: Motor dip is not a brownout
- **WHEN** one motor's supply voltage drops to 6.0 V for 1 s while the declared proxy stays at 11 V
- **THEN** no power marker is placed

#### Scenario: Median fallback
- **WHEN** a robot declares no battery proxy, and 3 of 5 devices drop to 6.5 V for 0.2 s while 2 stay at 11 V
- **THEN** a FAULT marker for `power` sits at that time, and its message names the median of devices as the proxy

#### Scenario: Logging gap does not stretch a dip
- **WHEN** the proxy logs one sample at 6.5 V followed by no sample for 0.8 s, then 11 V
- **THEN** the brownout marker for that dip lasts 0.1 s (the gap limit), not 0.8 s

#### Scenario: Reboot marker
- **WHEN** a synthetic match has a reboot event on the intake roller slot at T+128 s while enabled, with the reset count going from 0 to 1
- **THEN** a FAULT marker for that slot sits at T+128 s with the message "Device rebooted (reset count 0 → 1)"

#### Scenario: Long dropout marker
- **WHEN** a synthetic match has an isolated frame gap of 820 ms on the intake roller slot starting at T+127 s
- **THEN** a WARN marker for that slot sits at T+127 s with the message "No CAN frames for 0.82 s"

#### Scenario: Logger gaps stay quiet
- **WHEN** the Q7 Replay data is built
- **THEN** no marker comes from a bus-wide frame gap, and no reboot or dropout marker is shown

#### Scenario: Whole bus silent
- **WHEN** every device on a synthetic rio hoot is silent for 1.4 s at T+60 s
- **THEN** a WARN marker for the rio bus sits at T+60 s with the message "Whole bus silent for 1.4 s", and no slot on that bus gets a dropout marker for that time

#### Scenario: Quiet match
- **WHEN** no rule fires for a match
- **THEN** the timeline shows no markers, and the match list shows zero counts

### Requirement: Compare overlay
The user SHALL be able to overlay a second match on every track, aligned on match time and drawn in a distinct style. The readout SHALL show both matches' values for each slot. Slots present in only one match SHALL be marked absent on the other side.

#### Scenario: Overlay E10 on Q7
- **WHEN** the user overlays E10 while viewing Q7
- **THEN** each track shows both matches, and slots missing from E10 read "absent" in its column

### Requirement: Shareable view links
The app SHALL encode in the page address:
- the view and the selected match;
- the overlay match;
- the cursor time;
- the track choices;
- the shared filters (season, robot, event, unit), as defined by view filters;
- the visible time window.

Opening a copied address SHALL restore the same view, in both served and static modes. If the address holds a window that is invalid or outside the match, the full match window SHALL be used.

#### Scenario: Restore from link
- **WHEN** a user copies the address while viewing Q7 with E10 overlaid and the cursor at T+90 s, then opens it in a new window
- **THEN** the same match, overlay, and cursor are shown

#### Scenario: Restore filters and zoom
- **WHEN** a user copies the address while viewing Q7 with the robot filter set to `comp` and the window zoomed to T+95 – T+99 s, then opens it in a new window
- **THEN** the robot filter reads `comp`, the list shows only `comp` matches, and the tracks show T+95 – T+99 s

#### Scenario: Bad window in the address
- **WHEN** the address holds the window `z=300,20` for Q7
- **THEN** Q7 opens on the full match window

### Requirement: Match list filters
The Replay match list SHALL be filtered by the shared season, robot, and event filters, as defined by view filters, set in the shared filter bar rather than in the left rail. Each filter SHALL offer "all" plus the values present in the match index, newest first. Choosing a season SHALL limit the robot and event filters to the robots and events in that season, and a robot or event choice outside the new season SHALL return to "all". The shared clear control SHALL be offered, and it SHALL be disabled when every filter is already "all". Filters SHALL combine: only matches that satisfy every chosen filter are listed. Each index entry SHALL carry its season, so the filter never infers the season from the event key.

The match index does not record which units played each match. When the shared unit filter is set, Replay SHALL show it in the filter bar as not applied in Replay, and SHALL NOT narrow the match list by it, in served or static mode.

Default filters, used only when the page address holds no shared filter:
- with no selected match in the page address, the filters SHALL default to the most recent event (by match start time), and to that event's season;
- with a selected match, they SHALL default to that match's event.

When no match passes the filters, the list SHALL say so and offer one control that clears them.

When the selected match is excluded by the filters, it SHALL stay open, and the list SHALL say that the selected match is hidden by the filters. Filtering SHALL work the same way in served and static modes, over the matches the index contains, with no network.

A filter value in the address that the index doesn't contain SHALL be treated as an empty result, and that value SHALL be named. It SHALL NOT be silently dropped.

#### Scenario: Default to the most recent event
- **WHEN** Replay opens with no address state on a lake whose matches span events `2025gaalb`, `2026gadal`, and `2026gacmp`, with `2026gacmp` the latest
- **THEN** the filters read season 2026, robot all, and event `2026gacmp`, and only `2026gacmp` matches are listed

#### Scenario: No default after a clear
- **WHEN** the filters were cleared on another view, and Replay is opened from the navigation
- **THEN** all three filters read "all", and every match is listed

#### Scenario: Filters combine
- **WHEN** the user chooses season 2026 and robot `practice` on a lake where both robots played `2026gadal`
- **THEN** only `practice` matches from 2026 events are listed, and the event filter offers only 2026 events

#### Scenario: Season narrows the robot list
- **WHEN** the lake has a robot that played only 2025 matches, and the user chooses season 2026
- **THEN** that robot is not offered in the robot filter, and if it was chosen, the robot filter returns to "all"

#### Scenario: Clear all filters
- **WHEN** the user presses the clear-filters control
- **THEN** season, robot, and event all read "all", every match is listed, and the control is disabled until a filter is chosen again

#### Scenario: Nothing matches
- **WHEN** the filters select a robot that played no match at the chosen event
- **THEN** the list reads "No matches for these filters", and its clear control lists every match

#### Scenario: Selected match filtered out
- **WHEN** the page address selects `2026gacmp_qm7` and sets the event filter to `2026gadal`
- **THEN** Q7 is shown in the main area, and the list notes that the selected match is hidden by the filters

#### Scenario: Unknown value in the address
- **WHEN** the page address sets the event filter to `2019zzzz`
- **THEN** the list is empty, names `2019zzzz` as having no matches, and offers to clear the filters

#### Scenario: Static export filters offline
- **WHEN** a static export with matches from two events is opened from disk with networking disabled, and the user picks the older event
- **THEN** only that event's matches are listed, and no request leaves the page

#### Scenario: Unit filter not applied
- **WHEN** the shared unit filter is set to `u-12345` and Replay is opened
- **THEN** the filter bar shows the unit marked "not applied in Replay", and the match list is the same as with no unit set

#### Scenario: Static export filter bar
- **WHEN** a static export is opened from disk with no network
- **THEN** the filter bar shows season, robot, and event in the band under the header, and filtering and clearing work as in served mode

### Requirement: Timeline zoom
Replay tracks SHALL share one visible time window. The user SHALL be able to change it with:
- zoom-in and zoom-out buttons;
- Ctrl or ⌘ with the scroll wheel, or a trackpad pinch, centred on the pointer;
- shift-drag across a track to select a range;
- a reset control that returns to the full match window.

Plain drag and the keyboard SHALL keep moving the scrub cursor. When the window changes, every track, the marker strip, the auto and teleop bands, the reference lines, and the scrub cursor SHALL move together. A marker outside the window SHALL be hidden from the strip, and the strip SHALL show a count of the hidden markers on each side. Selecting a marker outside the window SHALL move the window to include it.

The window SHALL stay within the match window and SHALL NOT be narrower than 0.5 s. Each track's range readout SHALL describe the visible window. The header SHALL show the visible window (for example "T+95.0 – T+99.0 s"). The zoom controls SHALL be buttons reachable by keyboard, and `+`, `-`, and `0` SHALL zoom in, zoom out, and reset when focus is in the timeline. The compare overlay SHALL follow the same window, aligned on match time.

In static mode, or whenever finer data is unavailable, zooming past the stored bucket width SHALL keep showing the stored buckets, and the header SHALL say "bucket resolution (N ms)".

#### Scenario: Zoom to a brownout
- **WHEN** the user shift-drags from T+95 s to T+99 s on any track of a match with a brownout marker at T+97 s
- **THEN** every track and the marker strip show T+95 to T+99 s, the brownout marker sits under the same x position on each track, and the header reads "T+95.0 – T+99.0 s"

#### Scenario: Plain drag still scrubs
- **WHEN** the user drags across a track without Shift
- **THEN** the cursor moves, and the visible window is unchanged

#### Scenario: Buttons and reset
- **WHEN** the user presses zoom-in twice and then reset
- **THEN** the window narrows around the cursor (or the window centre if no cursor is set) each time, and reset restores the full match window

#### Scenario: Hidden markers
- **WHEN** the window is T+95 to T+99 s and the match has markers at T+20 s and T+120 s
- **THEN** neither marker is drawn, and the strip shows one hidden marker on the left and one on the right

#### Scenario: Static export at bucket resolution
- **WHEN** a static export is zoomed to a 2 s window
- **THEN** the tracks show the stored buckets in that window, and the header says "bucket resolution" with the stored bucket width

#### Scenario: Window cannot leave the match
- **WHEN** the user zooms out three times from the full match window, or tries to zoom in past 0.5 s
- **THEN** the window stays the full match window, or 0.5 s wide

### Requirement: Windowed detail in served mode
In served mode, when the visible window is narrower than about half the match window, Replay SHALL fetch finer envelopes for the visible window from the lake's silver layer. Each finer envelope SHALL follow the same rules as the stored ones:
- about 1000 buckets across the visible window, each bucket no narrower than 10 ms;
- the minimum, maximum, and mean of each bucket;
- the true minimum and maximum of every series in the window kept;
- temperature as change points;
- at least 3 significant figures.

The fetch SHALL read only that match's partitions, and SHALL NOT read raw logs. While a fetch is pending, the tracks SHALL keep showing the stored buckets. If the fetch fails, or the match has no silver data in the lake, the tracks SHALL keep the stored buckets and the header SHALL say why. The endpoint SHALL accept only a known match key and a window inside that match's window. It SHALL refuse unknown, repeated, or out-of-range parameters with a client error and SHALL never return data outside the match.

#### Scenario: Finer detail on zoom
- **WHEN** Q7 is served and zoomed to a 2 s window
- **THEN** the tracks redraw with buckets of 10 ms or narrower than the stored width, and each bucket's minimum and maximum equal those of the silver samples in its time range

#### Scenario: Spike kept in the window
- **WHEN** a synthetic series holds 10 A with a single 150 A sample lasting 4 ms, and the window around it is fetched
- **THEN** the bucket containing the spike shows a maximum of 150 A

#### Scenario: Window fetch budget
- **WHEN** a 2 s window of Q7 is fetched from an existing lake
- **THEN** the response arrives in under 1 s with peak memory under 500 MB, and only Q7's partitions are read

#### Scenario: Bad window refused
- **WHEN** the endpoint is asked for an unbuilt match key, a window that ends before it starts, a window outside the match, or an unknown parameter
- **THEN** it answers with a client error and a one-line reason, and returns no samples

#### Scenario: Silver missing
- **WHEN** Q7's Replay data is built and its silver partition is then removed from the lake, and the user zooms in
- **THEN** the tracks keep the stored buckets, and the header says finer data is unavailable

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

### Requirement: Anomaly badges in Replay
Replay SHALL show anomalies that affect the matches it lists:
- each match in the match list with an open anomaly flagged at that match SHALL show the highest severity glyph among them;
- each track whose slot has an open anomaly flagged at the shown match SHALL show a badge with the anomaly's method and title;
- a `power` anomaly SHALL badge the battery track.

The badges SHALL use the same sensitivity setting as the Anomalies view. In served mode, the badges SHALL come from the current anomalies and triage, without rebuilding Replay data, and selecting a badge SHALL open that anomaly in the Anomalies view. In static mode, the export SHALL carry a snapshot of the open anomalies for its matches, labelled with the time it was taken. Static badges SHALL be read-only and SHALL NOT link to the Anomalies view. Building Replay data SHALL NOT depend on anomalies, so a detection run never rebuilds match data.

#### Scenario: Served badge
- **WHEN** the app is served on a corpus lake built from `2026-gacmp-q7` with a test configuration that sets the brownout threshold to 8.5 V and the sag threshold to 9.0 V (above Q7's lowest battery proxy voltage), and Replay opens Q7
- **THEN** Q7 in the match list shows the HIGH glyph, the battery track shows the brownout badge, and selecting it opens that anomaly in the Anomalies view

#### Scenario: Resolved anomaly drops its badge
- **WHEN** the Q7 brownout anomaly is resolved, and Replay is refreshed
- **THEN** Q7 shows no anomaly glyph, and the Replay data files are unchanged

#### Scenario: Static snapshot
- **WHEN** a static export of Q7 is made and opened from disk with networking disabled
- **THEN** the brownout badge shows with the snapshot time, and it does not link anywhere

#### Scenario: Detection does not rebuild Replay
- **WHEN** detection runs again after a configuration change, and the report command runs
- **THEN** no match data file is rewritten

### Requirement: Side rails scroll independently
When both side rails sit beside the main area, the header and the filter bar SHALL stay in view, and the left rail, the main area, and the right rail SHALL each scroll on their own within the window. Scrolling one SHALL NOT move the others. When the window is too narrow and the rails wrap above or below the main area, the page SHALL scroll as a whole. This SHALL hold on every view that has rails.

#### Scenario: Scroll the evidence, keep the queue
- **WHEN** Anomalies is open at 1440 × 900 with a queue longer than the window, and the user scrolls the detail panel to its end
- **THEN** the queue's first item and the filter bar are still in view

#### Scenario: Scroll the match list, keep the tracks
- **WHEN** Replay is open at 1440 × 900 on a lake with 60 matches, and the user scrolls the match list to its last match
- **THEN** the tracks and the readout don't move

#### Scenario: Narrow window
- **WHEN** Replay is open at 700 px wide
- **THEN** the rails wrap, and the page scrolls as one

### Requirement: Minimizable side rails
Each side rail SHALL have a control that minimizes it to a narrow strip at its side of the window, and the strip SHALL have a control that restores it. A minimized strip SHALL name the rail. The main area SHALL take the freed width, and charts SHALL redraw to the new width. The controls SHALL be buttons reachable by keyboard that announce whether the rail is expanded. Each view and side SHALL remember its choice on this machine. If the browser can't store it, rails SHALL start expanded and the controls SHALL still work.

#### Scenario: Minimize the Replay match list
- **WHEN** the user minimizes Replay's left rail
- **THEN** the rail becomes a strip labelled MATCHES, and the tracks widen to fill the space

#### Scenario: Remembered per view
- **WHEN** the user minimizes the right rail in History and reloads the page, then opens Replay
- **THEN** History's right rail is still minimized, and Replay's right rail is expanded

#### Scenario: Keyboard
- **WHEN** a keyboard user tabs to a rail's minimize control and presses Enter, then presses Enter on the strip's restore control
- **THEN** the rail minimizes and is restored, and each control reports the rail's expanded state

#### Scenario: No storage
- **WHEN** local storage is unavailable and the user minimizes a rail
- **THEN** the rail minimizes, and after a reload it is expanded

### Requirement: Replay data kept current by the acquire watch
After a cycle's derive succeeds, the acquire command SHALL build Replay data incrementally (the same build as the report command with no options), in watch and one-shot modes alike, and SHALL NOT build it in a dry run. The build SHALL run as a separate process, after derive and detection and before backup, so the watch's memory stays within the existing build budget. A stop SHALL end the build like any other step.

A build that fails, is stopped, or finds another build running SHALL stay owed: the next cycle SHALL run it, even when nothing new arrived, and it SHALL stay owed across a restart of the watch. A failed build SHALL NOT fail the cycle, block later cycles' pulls, ingest or derive, or be retried more than once per cycle. The watch status SHALL show "Replay build pending" while a build is owed, and the tail of the last failed build's output until a build succeeds.

A running served app SHALL list a match built this way after the page is reloaded, without restarting the server.

#### Scenario: New match reaches Replay
- **WHEN** the watch is running beside `flashpoint serve`, and a cycle ingests and derives a new match
- **THEN** the same cycle builds that match's Replay data, and after a page reload Replay lists the match and opens it

#### Scenario: Only changed matches are rebuilt
- **WHEN** a cycle derives one new match in a lake whose other matches are already built
- **THEN** the build writes data for the new match only, and every other match's data file is unchanged byte-for-byte

#### Scenario: Nothing new
- **WHEN** a cycle has nothing to ingest and no build owed
- **THEN** no Replay build runs

#### Scenario: Derive failed
- **WHEN** a cycle's derive exits non-zero
- **THEN** no Replay build runs in that cycle

#### Scenario: Build fails and is retried
- **WHEN** the Replay build exits non-zero after a successful derive
- **THEN** the status shows the build's error and "Replay build pending", the next cycle runs the build again with nothing new to ingest, and after it succeeds the status shows neither

#### Scenario: Owed across a restart
- **WHEN** the watch stops while a Replay build is owed, and is started again
- **THEN** its first cycle runs the build

#### Scenario: Dry run
- **WHEN** the acquire command runs with `--dry-run`
- **THEN** no Replay build runs

### Requirement: One Replay build at a time
The report command SHALL hold a per-lake lock while it builds. When another build holds the lock, the report command SHALL exit with code 3 and a message naming the lock, and SHALL write nothing. The served app's start-up build SHALL treat exit 3 as a build already in progress and start normally. The acquire watch SHALL treat exit 3 as not done: the build stays owed for the next cycle, and it is not reported as an error. A lock left behind by a process that no longer runs SHALL NOT block later builds.

#### Scenario: Second build while one runs
- **WHEN** a Replay build holds the lake's lock and `flashpoint report` is run on the same lake
- **THEN** it exits 3, says a build is already running, and writes no data file, index or build state

#### Scenario: Watch meets a running build
- **WHEN** the watch's Replay build exits 3
- **THEN** the status shows "Replay build pending" and no error, and the next cycle runs the build

#### Scenario: Server starts during a watch build
- **WHEN** `flashpoint serve` starts while the watch's Replay build holds the lock
- **THEN** the server starts and serves the existing Replay data

#### Scenario: Stale lock
- **WHEN** a build was killed and left its lock file behind
- **THEN** the next `flashpoint report` builds normally

### Requirement: Replay data rebuilt after a manual ingest
After `flashpoint ingest` or `flashpoint rebuild` runs derive and the derive succeeds, the command SHALL build Replay data incrementally. That is the same build as the report command with no options, run as a separate process after derive and detection. `--no-report` SHALL skip the build. `--no-derive` SHALL skip it as well, so the acquire watch, which ingests with `--no-derive`, keeps running its own build once per cycle.

When another build holds the lake's Replay lock, the command SHALL print that a build is already running and that `flashpoint report` must be run after it finishes. When the build fails, the command SHALL print the build's error. Neither case SHALL change the command's exit code, which stays the ingest result.

#### Scenario: Wpilog arrives after its hoots
- **WHEN** a match's hoots are ingested on their own, Replay data is built (the match shows "No aligned samples"), and then the match's wpilog is ingested with `flashpoint ingest`
- **THEN** the second ingest rebuilds that match's Replay data, and Replay shows its aligned device data without a separate `flashpoint report`

#### Scenario: Only changed matches are rebuilt
- **WHEN** `flashpoint ingest` adds one new match to a lake whose other matches are already built
- **THEN** the build writes data for the new match only, and every other match's data file is unchanged byte-for-byte

#### Scenario: Opt out
- **WHEN** `flashpoint ingest --no-report` ingests a new match
- **THEN** derive runs and no Replay build runs

#### Scenario: Nothing ingested
- **WHEN** `flashpoint ingest` finds only files already in the lake
- **THEN** neither derive nor a Replay build runs

#### Scenario: Build already running
- **WHEN** `flashpoint ingest` ingests a new match while another Replay build holds the lock
- **THEN** the ingest output says a build is already running and tells the user to run `flashpoint report` after it, and the exit code is the ingest's own
