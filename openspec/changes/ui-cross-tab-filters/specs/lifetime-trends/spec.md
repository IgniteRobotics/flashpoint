## ADDED Requirements

### Requirement: History uses the shared filters
History SHALL take its season, robot, event, and unit from the shared filters, as defined by view filters, shown in the shared filter bar. Season, robot, and event SHALL narrow the chart, the device table, and the tiles, as queries. The unit filter SHALL select that unit in History: its line is emphasised, its row is marked, and its details fill the right rail. The other units in range SHALL stay in the chart and table. Selecting a unit in the table SHALL set the shared unit filter, and selecting it again SHALL clear it.

History's match type, phase, subsystem, and slot filters SHALL stay History-only. They SHALL sit in the same filter bar after the divider, SHALL be kept in the page address, and SHALL be cleared by the shared clear control on History.

#### Scenario: Unit chosen in History reaches Anomalies
- **WHEN** the user selects unit `u-12345` in History's table and opens Anomalies from the navigation
- **THEN** Anomalies lists only that unit's anomalies, and its unit chip reads `u-12345`

#### Scenario: Unit set elsewhere
- **WHEN** the shared unit filter is `u-12345` and History is opened
- **THEN** that unit's line is emphasised and its details are in the right rail, and the other units are still listed

#### Scenario: History-only filter stays on History
- **WHEN** the user sets History's subsystem filter to `shooter` and opens Anomalies
- **THEN** Anomalies is not filtered by subsystem, and History still reads `shooter` when the user returns from the navigation

#### Scenario: Clear on History
- **WHEN** History has event `2026gacmp` and subsystem `shooter` set, and the user presses CLEAR FILTERS
- **THEN** the event, the subsystem, and every other filter read "all", and the chart shows all time
