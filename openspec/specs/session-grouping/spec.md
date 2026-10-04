# session-grouping Specification

## Purpose
Group the logs one robot produced during one power-on (a wpilog plus one hoot per CAN bus) and place every sample on a single clock. Without this, signals from different files cannot be compared.

## Requirements

### Requirement: Session grouping
A session SHALL be one wpilog plus every hoot group that overlaps it in wall-clock time from the same robot. A hoot group is the set of hoots, one per bus, started together (same filename timestamp). A file SHALL belong to at most one session. Hoots that overlap no wpilog SHALL form a hoot-only session.

#### Scenario: Qualification match session
- **WHEN** corpus `2026-gacmp-q7` is grouped
- **THEN** one session contains its wpilog, CANivore hoot, and rio hoot

#### Scenario: Hoot logger restart after the wpilog ended
- **WHEN** corpus `2026-gacmp-e10` is grouped (its wpilog ends at 19:28:10 UTC, and its second hoot group starts at 19:29:15)
- **THEN** the wpilog and the first hoot group form one session, and the second hoot group forms a hoot-only session with the same match key from its filename

#### Scenario: Restart within the wpilog
- **WHEN** a second hoot group starts while the wpilog is still recording
- **THEN** both hoot groups join the wpilog's session, each with its own clock offset

### Requirement: Clock alignment with confidence
Each hoot in a session SHALL get an offset onto the wpilog clock, with a method and a confidence:
1. **`payload-match` (high):** identical payloads of a signal logged to both files (for example the drivetrain pose). Spread must be under 5 ms.
2. **`enable-edges` (low):** matching robot-enable transitions. Accepted only with at least 2 paired edges, a spread under 1 s, and agreement with the wall-clock estimate within 10 s.
3. **`wall-clock` (very low):** filename and anchor times.

The method, offset, spread, and number of matched points SHALL be recorded.

#### Scenario: High-confidence alignment
- **WHEN** corpus `2026-gacmp-q7` is aligned
- **THEN** both hoots align by `payload-match`, the offset is about 19.56 s, and the spread is under 5 ms

#### Scenario: No shared payload
- **WHEN** a session's hoot shares no payload signal with its wpilog
- **THEN** alignment falls back to `enable-edges` and is recorded as low confidence

#### Scenario: Buses share a clock
- **WHEN** a session's rio and CANivore hoots are aligned
- **THEN** their offsets differ by less than 1 ms
