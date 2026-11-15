## ADDED Requirements

### Requirement: Nominal frame rate per device
Each per-device CAN-health row SHALL record the device's nominal frame rate in frames per second. This is the inverse of the nominal period used to find its frame gaps in that session, so its auto, teleop and match rows carry the same value. A device with no interval between frames while enabled (so no nominal period) SHALL have an empty frame rate, never zero. Recording the rate SHALL NOT change how gaps are found, counted or classified.

#### Scenario: CANivore and rio rates on Q7
- **WHEN** corpus `2026-gacmp-q7` is derived with the 2026 robot configuration
- **THEN** every CANivore drive motor's `match` row records a frame rate between 200 and 300 frames per second, every rio device's row records a positive frame rate, and the gap counts are the same as before the rate was recorded

#### Scenario: No frames while enabled
- **WHEN** a synthetic mapped device sends frames only while the robot is disabled
- **THEN** its CAN-health row has an empty frame rate, and derive succeeds

#### Scenario: Truncated hoot
- **WHEN** corpus `2026-gacmp-e10-truncated` is derived
- **THEN** each device on that hoot records the frame rate measured over the frames it has, and derive succeeds
