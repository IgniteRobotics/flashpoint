## ADDED Requirements

### Requirement: Supply path resistance
For each motor and period, the system SHALL estimate the resistance of the motor's supply path, in milliohms. This is the slope of the drop (the battery proxy voltage minus the motor's supply voltage) against the motor's supply current, over the moments when both are logged. The motor's output voltage SHALL NOT enter the estimate, so voltage compensation and duty cycle do not change it.

It SHALL be absent, with a reason, when:
- the robot has no battery proxy device logged in the period (`no-proxy`);
- the motor's supply current is above a minimum (default 10 A) for less than a minimum time (default 1 s), or its range at those moments is too narrow to fit (`low-current`);
- the estimate is negative (`no-fit`).

#### Scenario: Known resistance
- **WHEN** a synthetic motor draws 10 to 60 A while its supply voltage sits 20 mΩ × its current below the proxy voltage
- **THEN** its supply path resistance for that period is 20 mΩ within 1 mΩ

#### Scenario: Load does not change it
- **WHEN** the same motor draws twice the current in a second period with the same 20 mΩ path
- **THEN** both periods give 20 mΩ within 1 mΩ, although the second period's voltage drop is twice as large

#### Scenario: Too little current
- **WHEN** a steer motor never draws more than 8 A in a period
- **THEN** its supply path resistance is absent and flagged `low-current`

#### Scenario: Corpus drive motors
- **WHEN** corpus `2026-gacmp-q7` is derived with the 2026 robot configuration
- **THEN** each drive motor's match supply path resistance is between 5 and 40 mΩ
