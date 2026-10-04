# motor-physics Specification

## Purpose
Turn raw motor signals into physically meaningful, comparable quantities (power, energy, thermal load, stall time, deviation from the motor model) that do not depend on sample rate or logging gaps.

## Requirements

### Requirement: Time-weighted statistics
Means and percentiles over a period SHALL be weighted by time held (each sample held until the next sample of the same signal). They SHALL NOT be weighted by sample count. Time before a signal's first sample SHALL be excluded, not treated as zero.

#### Scenario: Irregular sampling
- **WHEN** a signal holds 10 A for 9 s and 100 A for 1 s, sampled at different rates
- **THEN** its time-weighted mean is 19 A

#### Scenario: Late first sample
- **WHEN** a temperature signal's first sample arrives 5 s into a period
- **THEN** the first 5 s are excluded from its mean

### Requirement: Power and energy
For each motor, the system SHALL compute:
- motor power (motor voltage × stator current) and supply power (supply voltage × supply current);
- energy in watt-hours for each, integrated over time held.

Velocity SHALL keep its sign.

#### Scenario: Constant draw
- **WHEN** a motor holds 12 V supply and 10 A supply current for 60 s
- **THEN** its supply energy is 2.0 Wh

### Requirement: Current residual from device-reported constants
Using the torque and velocity constants and stall current that the device reports, the system SHALL compute the expected stator current for the measured voltage and velocity, and the residual (measured minus expected). If the device reports no constants, the residual SHALL be absent rather than estimated.

#### Scenario: Free-spinning motor
- **WHEN** a motor runs at steady free speed for its applied voltage
- **THEN** its residual is near zero relative to its stall current

#### Scenario: No constants
- **WHEN** a hoot lacks the device's motor constants
- **THEN** residual features for that motor are absent and flagged `no-motor-constants`

### Requirement: Thermal and stall features
For each motor and period, the system SHALL compute the maximum temperature, the temperature rise rate, and the stall time: time with stator current above a configurable fraction of stall current while the absolute velocity is below a configurable threshold.

#### Scenario: Stall
- **WHEN** a motor draws 60 % of its stall current at zero velocity for 2 s
- **THEN** its stall time for that period is 2 s
