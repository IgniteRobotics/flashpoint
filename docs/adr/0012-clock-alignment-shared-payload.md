# 0012. Hoot-to-wpilog clock alignment by shared payload

- Status: Proposed
- Date: 2026-10-04
- Refs: P2 (`openspec/changes/p2-semantics-and-physics`), spec `session-grouping`

## Context
- Hoot (CTRE) and wpilog (WPILib) timestamps run on **different clocks**. On corpus Q7 the offset is 19.56 s.
- Without alignment, motor current from a hoot can't be lined up with robot state from the wpilog.
- Rio and CANivore hoots started together share a clock: their `RobotEnable` edges are identical to the microsecond.

## Decision
Align each hoot group to its wpilog in this order:
1. **`payload-match` (high confidence).**
   - CTRE's generated swerve telemetry writes `DriveState/Pose` (`struct:Pose2d`) both to the rio hoot and to NetworkTables (wpilog).
   - Identical payload bytes pair samples exactly. Only values unique in each log are used.
   - The offset is the median and the spread is the IQR. Accept with at least 50 matches and a spread under 5 ms.
   - Q7 result: **15,039 matches, offset 19.5635 s, IQR 76 µs**.
2. **`enable-edges` (low confidence).**
   - `DS:enabled` vs hoot `RobotEnable`, paired one-to-one.
   - Requires at least 2 pairs, a spread under 1 s, and agreement with the wall clock within 10 s.
   - Typical error is 30–500 ms.
3. **`wall-clock` (very low confidence).** The hoot filename stamp against the wpilog's `systemTime` anchor, accurate to seconds.

Every hoot records its method, offset, spread, match count, and agreement with sibling buses. Gold features carry the session's weakest alignment confidence.

## Consequences
- Signals from different files can be joined to sub-millisecond accuracy on robots that publish the pose to both logs (Robot-2026 and Phoenix-2026 do, through CTRE `Telemetry.java`).
- Robots that don't publish it get low-confidence alignment, and downstream consumers can tell.
- **Real data showed why the guards matter:** on corpus E10, a single wpilog enable edge was reused for two hoot edges and produced a confident-looking 127.8 s offset where the true value is about 196 s. Pairing is now one-to-one and checked against the wall clock.

## Alternatives considered
- **Enable edges only:** too coarse (30–500 ms) and fragile with few edges.
- **Filename timestamps only:** 1 s resolution, plus an unknown logger start delay.
- **Cross-correlating motor signals:** works without shared telemetry, but it's costlier and ambiguous when the robot is idle. Keep it in reserve.
