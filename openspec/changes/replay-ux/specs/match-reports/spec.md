## ADDED Requirements

### Requirement: Match list filters
The Replay match list SHALL have three filters: season, robot, and event. Each filter SHALL offer "all" plus the values present in the match index, newest first. Choosing a season SHALL limit the robot and event filters to the robots and events in that season, and a robot or event choice outside the new season SHALL return to "all". A control that clears every filter SHALL always be offered, and it SHALL be disabled when every filter is already "all". Filters SHALL combine: only matches that satisfy every chosen filter are listed. Each index entry SHALL carry its season, so the filter never infers the season from the event key.

Default filters:
- with no filters and no selected match in the page address, the filters SHALL default to the most recent event (by match start time), and to that event's season;
- with a selected match and no filters in the address, they SHALL default to that match's event.

When no match passes the filters, the list SHALL say so and offer one control that clears them.

When the selected match is excluded by the filters, it SHALL stay open, and the list SHALL say that the selected match is hidden by the filters. Filtering SHALL work the same way in served and static modes, over the matches the index contains, with no network.

A filter value in the address that the index doesn't contain SHALL be treated as an empty result, and that value SHALL be named. It SHALL NOT be silently dropped.

#### Scenario: Default to the most recent event
- **WHEN** Replay opens with no address state on a lake whose matches span events `2025gaalb`, `2026gadal`, and `2026gacmp`, with `2026gacmp` the latest
- **THEN** the filters read season 2026, robot all, and event `2026gacmp`, and only `2026gacmp` matches are listed

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

## MODIFIED Requirements

### Requirement: Shareable view links
The app SHALL encode in the page address:
- the view and the selected match;
- the overlay match;
- the cursor time;
- the track choices;
- the match list filters (season, robot, event);
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
