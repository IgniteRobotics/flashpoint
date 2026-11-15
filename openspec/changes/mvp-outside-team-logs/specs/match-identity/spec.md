## MODIFIED Requirements

### Requirement: Source precedence
Identity SHALL come from the first of these sources that gives an event, a match type and a match number:
1. **In-log FMS data** in the session's wpilog: the FMSInfo fields, or, when they give no match number, the driver-station match fields that AdvantageKit logs under `/DriverStation/`.
2. **The wpilog filename**: the WPILib pattern (`FRC_yyyyMMdd_HHmmss_EVENT_{P|Q|E}n`) or the AdvantageKit pattern (`akit_yy-MM-dd_HH-mm-ss_EVENT_{p|q|e}n`, level letter in either case).
3. **The hoot filenames**: the event and match label in the names of the session's hoots (`EVENT_{P|Q|E}n_…`). They are used only when the wpilog gives no identity, or when the session has no wpilog.

The source used SHALL be recorded as `fms`, `filename` or `hoot-filename`. If the in-log value and the wpilog filename disagree, the in-log value wins and a `match-identity-conflict` warning is recorded. If a session's hoot groups carry different labels and the label is the source, the earliest group's label wins, and a `match-identity-conflict` warning is recorded. A field that is present but unusable SHALL be treated as absent and never stop identification. Unusable means an empty event name, a zero match number, or an unknown match type.

#### Scenario: Filename disagrees with FMS
- **WHEN** a log's filename says `Q8` but its FMS data says match 7
- **THEN** the match key uses 7, and the log carries warning `match-identity-conflict`

#### Scenario: Non-match log
- **WHEN** corpus `2025-nofms` is identified
- **THEN** no match key is assigned, and the log is classified as `non-match`

#### Scenario: AdvantageKit driver-station fields
- **WHEN** a synthetic wpilog has no FMSInfo entries and logs `/DriverStation/EventName` = `GACMP`, `/DriverStation/MatchType` = qualification and `/DriverStation/MatchNumber` = 7, and is named `akit_26-04-09_21-51-13.wpilog`
- **THEN** its session's match key is `2026gacmp_qm7` with source `fms`

#### Scenario: AdvantageKit filename
- **WHEN** a synthetic wpilog with no FMS fields is named `akit_26-04-09_21-51-13_GACMP_q7.wpilog`
- **THEN** its session's match key is `2026gacmp_qm7` with source `filename`

#### Scenario: Hoot label rescues an unnamed wpilog
- **WHEN** a synthetic session pairs a wpilog that is anchored by `systemTime` but has no FMS fields and no event in its filename with the hoots `GACMP_Q7_rio_2026-04-09_21-51-06.hoot` and `GACMP_Q7_<canivore-id>_2026-04-09_21-51-06.hoot`
- **THEN** the session's match key is `2026gacmp_qm7` with source `hoot-filename`, and it is classified as a match

#### Scenario: Wpilog identity wins over the hoot label
- **WHEN** corpus `2026-gacmp-q7` is identified
- **THEN** its source is still `fms`, and the hoot label is not consulted

#### Scenario: Partial driver-station fields
- **WHEN** a synthetic AdvantageKit wpilog logs `/DriverStation/MatchNumber` = 7 but no event name and match type 0, and its hoots are named `GACMP_Q7_…`
- **THEN** the session's match key is `2026gacmp_qm7` with source `hoot-filename`, and identification raises no error

#### Scenario: Unknown match type
- **WHEN** a synthetic wpilog logs `/DriverStation/MatchType` = `Scrimmage`, along with an event and a match number, and has no other source
- **THEN** no match key is assigned, the session is classified as `non-match`, and derive succeeds

#### Scenario: Hoot groups disagree
- **WHEN** a synthetic session's wpilog has no identity, and its two hoot groups are labelled `Q7` and `Q8`
- **THEN** the match key uses the earlier group's `Q7`, and the session carries warning `match-identity-conflict`
