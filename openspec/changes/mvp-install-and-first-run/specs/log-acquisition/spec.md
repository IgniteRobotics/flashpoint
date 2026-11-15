## ADDED Requirements

### Requirement: Low-priority hashing on the robot
The checksum the robot computes for a pull (see "Verified transfer") SHALL run at the lowest CPU priority the robot offers, so robot code keeps the processor. When the robot has no way to lower a command's priority, the checksum SHALL run at normal priority and the pull SHALL NOT fail or lose its verification for that reason. Lowering the priority SHALL NOT change what counts as `verified` or `size-verified`.

#### Scenario: Hash at low priority
- **WHEN** a stable wpilog is pulled from the test robot
- **THEN** the test robot records that the checksum ran at the lowest priority, and the pull is recorded as `verified`

#### Scenario: No way to lower priority
- **WHEN** the test robot has a checksum command but no priority-lowering command
- **THEN** the checksum runs at normal priority, the pull is recorded as `verified`, and one note per host per cycle is logged

#### Scenario: No checksum command
- **WHEN** the test robot has neither command
- **THEN** the file is accepted on size match and recorded as `size-verified`, as under "Verified transfer"

#### Scenario: Priority command without a checksum command
- **WHEN** the test robot has the priority-lowering command but no checksum command
- **THEN** the file is accepted on size match and recorded as `size-verified`, not counted as a failed attempt

### Requirement: Robot pull labelled beta until checked on a robot
Until the live-robot checks (a pull from a real roboRIO with its transfer rate, checksum time and robot loop overruns recorded, and an unattended watch run as a service) are recorded as passed, `flashpoint acquire --help` SHALL label pulling from the robot and running acquire as a service "beta, tested on one robot". The automatic import guide shows the same label (see project-documentation, "Automatic import guide"). Importing from a USB stick or other removable volume SHALL NOT carry the label.

#### Scenario: Help text
- **WHEN** `flashpoint acquire --help` is run
- **THEN** its description labels robot pull and service mode "beta, tested on one robot"

#### Scenario: Removable volumes unlabelled
- **WHEN** `flashpoint acquire --help` is run
- **THEN** its options for removable volumes (such as `--no-usb`) carry no maturity label
