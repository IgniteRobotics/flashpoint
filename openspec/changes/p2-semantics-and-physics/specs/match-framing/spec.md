## Purpose

Divide each session into match phases (pre-match, autonomous, teleop, post-match), so features describe the match and not the pit time around it.

## ADDED Requirements

### Requirement: Phases from driver station state
The system SHALL derive phases from the driver station's enabled and autonomous state. Contiguous enabled-autonomous time is `auto`, contiguous enabled-teleop time is `teleop`, disabled time between enabled periods is `gap`, and time outside them is `pre` or `post`. Each sample in silver SHALL carry its phase and its time since match start.

#### Scenario: Qualification match phases
- **WHEN** corpus `2026-gacmp-q7` is framed
- **THEN** it has exactly one `auto` period followed by one `teleop` period, and the disabled interval between them is labelled `gap` rather than counted as match time

#### Scenario: No enable
- **WHEN** a session never becomes enabled
- **THEN** every sample is labelled `pre`, and no match features are produced

### Requirement: Match start reference
Match time SHALL be measured from the first enable transition of the session, on the wpilog clock.

#### Scenario: Match time origin
- **WHEN** a sample occurs 1.5 s after the first enable
- **THEN** its match time is 1.5 s
