## Purpose

Divide each session into match phases (pre-match, autonomous, teleop, post-match), so features describe the match and not the pit time around it.

## ADDED Requirements

### Requirement: Phases from robot state
The system SHALL derive phases from the robot's mode over time.
- **Preferred source:** the robot mode logged in an aligned hoot, which is CAN-timestamped and covers the whole match. On corpus Q7 the wpilog stops at 213 s while the hoot runs to the end of teleop at 291.6 s.
- **Fallback:** the wpilog's driver-station enabled and autonomous state.

The source used SHALL be recorded. Enabled-autonomous time is `auto`, enabled-teleop time is `teleop`, disabled time between the first and last enabled periods is `gap`, and time before or after them is `pre` or `post`. Each sample in silver SHALL carry its phase and its time since match start.

#### Scenario: Qualification match phases
- **WHEN** corpus `2026-gacmp-q7` is framed
- **THEN** it has exactly one `auto` period followed by one `teleop` period, the disabled interval between them is labelled `gap` rather than counted as match time, and teleop ends about 291.6 s on the wpilog clock (past the wpilog's own end) because the hoot's robot mode is used

#### Scenario: Wpilog-only session
- **WHEN** a session has no aligned hoot
- **THEN** phases come from the wpilog's driver-station state, and the source is recorded as `ds`

#### Scenario: No enable
- **WHEN** a session never becomes enabled
- **THEN** every sample is labelled `pre`, and no match features are produced

### Requirement: Match start reference
Match time SHALL be measured from the first enable transition of the session, on the wpilog clock.

#### Scenario: Match time origin
- **WHEN** a sample occurs 1.5 s after the first enable
- **THEN** its match time is 1.5 s
