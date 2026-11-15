## ADDED Requirements

### Requirement: Phoenix Pro coverage check
The setup checks SHALL include a Phoenix Pro coverage check, shown by `flashpoint doctor` and the Setup view. It SHALL report how many hoots in the lake are Pro-licensed, out of how many, and for each bus with non-Pro hoots, how many there are. It SHALL be:
- ok when every hoot is Pro-licensed;
- a warning when any hoot is not: the message says temperature trends and temperature anomalies need a Phoenix Pro-licensed device on that bus, and links the documentation page on Phoenix Pro and temperature;
- ok, with the message "no hoots yet", when the lake holds no hoots.

It SHALL never be a problem, so it never makes `doctor` exit non-zero. It SHALL have no fix action. It SHALL read only the ledger, never the raw hoots. Hoots whose Pro flag is unknown (for example quarantined before conversion) SHALL be counted as unknown, not as non-Pro.

#### Scenario: All Pro
- **WHEN** `doctor` runs on a lake built from corpus-v1
- **THEN** the Pro coverage check is ok and reports every converted corpus hoot as Pro-licensed, with the empty quarantined hoot counted as unknown

#### Scenario: Mixed lake
- **WHEN** `doctor` runs on a synthetic lake with 4 Pro CANivore hoots and 4 non-Pro rio hoots
- **THEN** the check is a warning reading 4 of 8 hoots Pro-licensed with 4 non-Pro on `rio`, it links the Phoenix Pro and temperature page, and `doctor` exits 0 if nothing else is a problem

#### Scenario: Empty lake
- **WHEN** `doctor` runs on a lake with no hoots
- **THEN** the check is ok with "no hoots yet"

#### Scenario: Shown in the Setup view
- **WHEN** the Setup view's Checks tab is opened on the mixed synthetic lake
- **THEN** it shows the same warning and message as `doctor`, and no fix button
