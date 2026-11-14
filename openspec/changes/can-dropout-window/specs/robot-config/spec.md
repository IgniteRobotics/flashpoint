## ADDED Requirements

### Requirement: Bus dropout window

A robot configuration's bus entry (its robot-code name and hoot bus) MAY set `dropout_window_ms`: the shortest silence, in milliseconds, that counts as a dropout or frame gap on that bus. It SHALL be a number greater than 0. A bus without it SHALL have no window. A value that is zero, negative or not a number SHALL be rejected on load with a message naming the file and the bus.

#### Scenario: Window declared
- **WHEN** the 2026 competition robot configuration is loaded
- **THEN** its `DriveTrain` bus has a dropout window of 35 ms, and its `rio` bus has none

#### Scenario: Bad window
- **WHEN** a robot configuration sets `dropout_window_ms = 0` on a bus
- **THEN** loading fails with an error naming the file and that bus
