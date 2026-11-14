## MODIFIED Requirements

### Requirement: Frame gaps measured per device
For every device in a session's hoots, derive SHALL measure the device's frame stream against its own nominal period: the median interval between its frames while the robot is enabled. An interval of at least 2.5 times the nominal period, and at least its bus's dropout window when the robot configuration sets one (see `robot-config`), SHALL be a frame gap, and the frames it misses SHALL be its length divided by the nominal period, rounded, minus one. Only gaps that start while the robot is enabled SHALL count. Frames in the first 5 seconds of each hoot SHALL be ignored. A device whose frames stop before an enabled window ends SHALL have a gap running to the earlier of the window end and the last frame of any device in that hoot, under the same length rule; the end of a hoot SHALL never count as a gap. Devices with no configured slot SHALL be measured, so they count as siblings, but SHALL get no CAN-health row.

#### Scenario: Gap against the device's own period
- **WHEN** a synthetic device with an 8 ms frame period goes silent for 40 ms while enabled
- **THEN** one frame gap of 40 ms is recorded for it, missing 4 frames

#### Scenario: Normal jitter is not a gap
- **WHEN** a synthetic device with an 8 ms period has intervals of up to 19 ms
- **THEN** no frame gap is recorded for it

#### Scenario: Disabled time ignored
- **WHEN** a device is silent for 2 s while the robot is disabled
- **THEN** no frame gap is recorded for that silence

#### Scenario: Device stops before the window ends
- **WHEN** a synthetic device's frames stop 3 s before teleop ends while the other devices on its bus keep sending until the end
- **THEN** a frame gap of about 3 s is recorded for it, ending at the teleop end

#### Scenario: Truncated hoot
- **WHEN** corpus `2026-gacmp-e10-truncated` (a hoot read as `incomplete-read`) is derived
- **THEN** no device on that hoot gets a gap running past the hoot's last frame, and derive succeeds

#### Scenario: Jitter under the bus window
- **WHEN** a synthetic device with a 4 ms period on a bus with a 35 ms dropout window has an interval of 30 ms while enabled
- **THEN** no frame gap is recorded for it

#### Scenario: Gap over the bus window
- **WHEN** the same device has an interval of 40 ms while enabled
- **THEN** one frame gap of 40 ms is recorded for it, missing 9 frames

#### Scenario: Slow device on a windowed bus
- **WHEN** a synthetic device with a 20 ms period on a bus with a 35 ms dropout window has an interval of 45 ms while enabled
- **THEN** no frame gap is recorded, because 2.5 periods (50 ms) is longer than the window
