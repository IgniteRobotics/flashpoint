## Purpose

One set of filters (season, robot, event, unit) shared by every Flashpoint view and shown in one filter bar, so a filter chosen on one view applies on all of them, and clearing it clears it everywhere.

## ADDED Requirements

### Requirement: Shared filter state
The app SHALL keep one filter state with four shared filters: season, robot, event, and unit. Each filter is either "all" or one value. A filter set on any view SHALL apply on every view that is opened afterwards, including views opened from the navigation and from links inside the app. The shared filters SHALL be kept in the page address, so a copied address restores them. A link inside the app that sets a shared filter SHALL keep the other shared filters as they are.

Sensitivity, the anomaly queue tab, and view-only filters SHALL NOT be part of the shared state.

#### Scenario: Event carries from Replay to History
- **WHEN** the user chooses event `2026gacmp` in Replay and then opens History from the navigation
- **THEN** History's event filter reads `2026gacmp`, and its chart and table show only `2026gacmp` matches

#### Scenario: Robot carries from History to Anomalies
- **WHEN** the user chooses robot `practice` in History and then opens Anomalies from the navigation
- **THEN** the Anomalies robot filter reads `practice`, and only `practice` anomalies are listed and counted

#### Scenario: Drill-through keeps the other filters
- **WHEN** History is filtered to season 2026 and the user follows a unit's anomaly badge to Anomalies
- **THEN** Anomalies reads season 2026 and that unit

#### Scenario: Copied address restores the filters
- **WHEN** the user copies the address of Anomalies with season 2026, robot `comp`, and unit `u-12345` set, and opens it in a new window
- **THEN** the same three filters are set, and the event filter reads "all"

### Requirement: One filter bar
Every view SHALL show the shared filters in one filter bar, in the band under the header, in the same order and with the same labels: SEASON, ROBOT, EVENT, and UNIT. Season, robot, and event SHALL be selects that offer "all" ("All seasons", "All robots", "All events") and the values present in that view's data, newest first. The unit filter SHALL be shown only when set, as a removable chip naming the unit. A view's own extra filters SHALL follow the shared ones after a visible divider, and SHALL be labelled as applying to that view only. The bar SHALL show a CLEAR FILTERS control on every view.

Choosing a season SHALL limit the robot and event choices to the robots and events in that season. A robot or event choice outside the new season SHALL return to "all".

#### Scenario: Same bar on every view
- **WHEN** the user opens Replay, History, and Anomalies in served mode
- **THEN** each shows SEASON, ROBOT, and EVENT in the same order, with the same "all" labels, in the band under the header, followed by CLEAR FILTERS

#### Scenario: Season narrows robots on every view
- **WHEN** the lake has a robot that played only 2025 matches, and the user chooses season 2026 on Anomalies
- **THEN** that robot is not offered on Anomalies, History, or Replay, and if it was chosen, the robot filter returns to "all"

#### Scenario: View-only filters are marked
- **WHEN** the user opens History
- **THEN** the match type, phase, subsystem, and slot filters sit after the divider, labelled as History-only

### Requirement: Clearing clears everywhere
The CLEAR FILTERS control SHALL set every shared filter to "all" and SHALL clear the current view's own extra filters. Afterwards, every view SHALL show its unfiltered data, and the clear control SHALL be disabled until a filter is set again. A view's default filters SHALL NOT be re-applied after a clear.

#### Scenario: Clear on History clears Replay
- **WHEN** Replay is filtered to robot `comp` and event `2026gacmp`, and the user opens History and presses CLEAR FILTERS
- **THEN** History shows all time, and returning to Replay lists every match with all three filters reading "all"

#### Scenario: Clear removes the unit
- **WHEN** Anomalies is filtered to unit `u-12345` and the user presses CLEAR FILTERS
- **THEN** the unit chip is gone, every anomaly in the tab is listed, and History no longer emphasises that unit

#### Scenario: Nothing to clear
- **WHEN** every shared filter reads "all" and the view has no extra filter set
- **THEN** CLEAR FILTERS is disabled

### Requirement: Default filters
With no shared filter in the page address, History and Anomalies SHALL start with every filter at "all". Replay SHALL keep its defaults (the most recent event, or the selected match's event), and the defaults it applies SHALL become the shared state that the other views then use. An address that holds any shared filter, including "all", SHALL be used as it is, and no default SHALL be applied.

#### Scenario: Replay default becomes shared
- **WHEN** Replay opens with an empty address on a lake whose latest event is `2026gacmp`, and the user then opens History
- **THEN** History's season reads 2026 and its event reads `2026gacmp`

#### Scenario: History opened first
- **WHEN** History opens with an empty address
- **THEN** every filter reads "all"

### Requirement: Values a view doesn't contain
A shared filter value that a view's data doesn't contain SHALL be treated as an empty result on that view, and the view SHALL name that value. The value SHALL NOT be silently dropped or reset, so it still applies on the views that do contain it.

#### Scenario: Event with no built Replay data
- **WHEN** History is filtered to event `2026gacol`, which is in the lake but has no match built for Replay, and the user opens Replay
- **THEN** Replay's list says no matches pass, names `event 2026gacol`, and offers to clear the filters; History still reads `2026gacol` when the user returns

#### Scenario: Hostile value in the address
- **WHEN** the address holds the event value `x' OR 1=1 --`
- **THEN** every view shows an empty result naming that value, and no query errors

### Requirement: Selected item hidden by the filters
When a view's selected item (Replay's match, the selected anomaly) is excluded by the shared filters, it SHALL stay open, and the view SHALL say that it is hidden by the filters.

#### Scenario: Selected anomaly outside the event
- **WHEN** the address selects an anomaly flagged only at `2026gadal`, and the event filter reads `2026gacmp`
- **THEN** its detail stays open, and the queue says the selected anomaly is hidden by the filters

### Requirement: Navigation badges are not filtered
The navigation badges SHALL keep counting across the whole lake, whatever the shared filters are, so a filter never hides that something needs attention.

#### Scenario: Badge ignores the robot filter
- **WHEN** the robot filter reads `practice` and the only open anomaly is on `comp`
- **THEN** the Anomalies badge reads 1, and the Anomalies queue lists none
