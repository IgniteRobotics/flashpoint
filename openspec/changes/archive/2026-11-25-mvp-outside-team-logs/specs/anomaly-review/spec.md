## ADDED Requirements

### Requirement: Anomalies marked beta
In served mode, the Anomalies view header SHALL show a BETA tag and a one-line note. The note SHALL say that detection is tuned on one team's robots, that findings are suggestions to check against the evidence, and link the documentation's Anomalies page. That page SHALL also be marked beta and explain how to make findings more trustworthy: declaring a battery-proxy slot, declaring sibling groups, and letting units build their baseline. It SHALL link the robot-configuration documentation's sections on `battery_proxy` and `[[siblings]]`. The tag and note SHALL show whatever the counts, including when nothing is flagged. They SHALL NOT be dismissible in a way that hides them for other users of the lake.

#### Scenario: Beta tag on open
- **WHEN** the app is served on a lake built from corpus `2026-gacmp-q7` and Anomalies is opened
- **THEN** the header shows the BETA tag and the note, and the note's link opens the documentation's Anomalies page

#### Scenario: Beta tag with nothing flagged
- **WHEN** Anomalies is opened on a lake with no open anomalies
- **THEN** the BETA tag and note still show above the "nothing is flagged" message

#### Scenario: Docs page marked beta
- **WHEN** the documentation's Anomalies page is read
- **THEN** it is marked beta, and its links to the `battery_proxy` and `[[siblings]]` sections resolve to headings that a docs test finds by anchor
