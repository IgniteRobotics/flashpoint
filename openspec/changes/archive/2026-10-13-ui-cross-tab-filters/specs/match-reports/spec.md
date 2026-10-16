## ADDED Requirements

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

## MODIFIED Requirements

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
