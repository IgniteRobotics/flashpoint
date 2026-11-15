## MODIFIED Requirements

### Requirement: Clock alignment with confidence
Each hoot in a session SHALL get an offset onto the wpilog clock, with a method and a confidence:
1. **`payload-match` (high):** identical payloads of a signal logged to both files (for example the drivetrain pose). Spread must be under 5 ms.
2. **`enable-edges` (low):** matching robot-enable transitions. Accepted only with at least 2 paired edges, a spread under 1 s, and agreement with the wall-clock estimate within 10 s. The wpilog's enable transitions SHALL come from the driver-station enable entry that WPILib's `DriverStation.startDataLog` writes, or, when the wpilog has no such entry, from AdvantageKit's driver-station enabled entry. When both are present, the WPILib entry SHALL be used.
3. **`wall-clock` (very low):** filename and anchor times.

The method, offset, spread, and number of matched points SHALL be recorded, and for `enable-edges` the recorded method details SHALL name which enable entry was used.

#### Scenario: High-confidence alignment
- **WHEN** corpus `2026-gacmp-q7` is aligned
- **THEN** both hoots align by `payload-match`, the offset is about 19.56 s, and the spread is under 5 ms

#### Scenario: No shared payload
- **WHEN** a session's hoot shares no payload signal with its wpilog
- **THEN** alignment falls back to `enable-edges` and is recorded as low confidence

#### Scenario: Buses share a clock
- **WHEN** a session's rio and CANivore hoots are aligned
- **THEN** their offsets differ by less than 1 ms

#### Scenario: AdvantageKit enable entry
- **WHEN** a synthetic AdvantageKit-style wpilog with no WPILib enable entry, whose AdvantageKit enabled entry has 3 enable transitions, is aligned with a synthetic hoot whose robot-enable transitions match within 200 ms, and they share no payload signal
- **THEN** the hoot aligns by `enable-edges` with low confidence, and the method details name the AdvantageKit entry

#### Scenario: Both enable entries present
- **WHEN** a synthetic wpilog has both the WPILib enable entry and the AdvantageKit enabled entry, with different transitions
- **THEN** alignment uses the WPILib entry

#### Scenario: AdvantageKit enable entry with too few edges
- **WHEN** a synthetic AdvantageKit-style wpilog's enabled entry has a single enable transition, or holds values of the wrong type
- **THEN** alignment falls back to `wall-clock` with very low confidence, and derive completes without error
